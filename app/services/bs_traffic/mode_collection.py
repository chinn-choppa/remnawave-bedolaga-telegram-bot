"""Bounded mode-transition collection with durable exact identity membership."""

import asyncio
from datetime import UTC, timedelta
from uuid import uuid4

from sqlalchemy import func, select

from app.database.models import BsTrafficCollectionJob as Job, BsTrafficLedger, BsTrafficPoll as Poll, Subscription
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner
from app.services.bs_traffic.observations import MAX_NODES
from app.services.bs_traffic.panel_admission import REQUEST_SECONDS
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.queue import collection_request_hash, enqueue_job, run_once


def baseline_nodes(policy, new_modes, target_topology=None):
    topology = target_topology or policy['topology']
    return {
        node: group
        for group in ('ordinary', 'bs')
        if new_modes[group] == 'finite'
        for node in topology[f'{group}_nodes']
        if policy[f'{group}_mode'] == 'unlimited' or node not in policy['topology'][f'{group}_nodes']
    }


async def collect_mode_baselines(reader, *, user_id, subscription_id, new_modes, new_tariff_id=None):
    """Collect outside business locks; final debit revalidates every stored proof.

    No payment or policy mutation. The caller already checked the live account
    and quote, and must check them again before purchasing. Collection alone
    authorizes neither adoption nor a change of entitlement.
    """
    if not isinstance(reader, BoundPanelReader) or any(
        type(value) is not int or value <= 0 for value in (user_id, subscription_id)
    ):
        raise LedgerConflict('Owned bound mode collection required')
    if (
        not isinstance(new_modes, dict)
        or set(new_modes) != {'ordinary', 'bs'}
        or any(value not in ('finite', 'unlimited') for value in new_modes.values())
    ):
        raise LedgerConflict('Explicit target traffic modes required')
    async with asyncio.timeout(REQUEST_SECONDS):
        await reader.assert_current()
        target_topology = None
        if new_tariff_id is not None:
            from app.database.models import Tariff
            from app.services.bs_traffic.evidence import collect_topology_evidence
            from app.services.bs_traffic.panel_admission import panel_slot
            from app.services.bs_traffic.policy import _topology
            from app.services.bs_traffic.tariff_settings import BsTrafficSettings

            async with reader._factory() as db:
                tariff = await db.get(Tariff, new_tariff_id)
                if tariff is None:
                    raise LedgerConflict('Target tariff missing')
                ordinary = list(tariff.allowed_squads or [])
                bs = list(BsTrafficSettings.model_validate(tariff.bs_traffic_settings).squad_uuids)
            async with panel_slot(reader._factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader, panel_identity=reader.panel_scope, ordinary_squads=ordinary, bs_squads=bs
                )
            target_topology = _topology(evidence.topology)
        async with reader._factory() as db, db.begin():
            generation = await db.scalar(
                select(BsTrafficLedger.account_generation)
                .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
                .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
            )
            if generation is None:
                raise LedgerConflict('Owned split subscription required')
            state, _ = await _lock(db, user_id, subscription_id, generation)
            await require_activated_owner(db, subscription_id)
            if state.panel_scope != reader.panel_scope:
                raise LedgerConflict('Wrong mode collection panel scope')
            policy = _read_policy(state.access_policy)
            nodes = sorted(baseline_nodes(policy, new_modes, target_topology))
            if not nodes:
                return ()
            if len(nodes) > 200:
                raise LedgerConflict('Mode collection node budget exceeded')
            now = await db.scalar(select(func.clock_timestamp()))
            day = now.astimezone(UTC).date()
            panel_id = state.panel_user_id
            jobs = []
            for start in range(0, len(nodes), MAX_NODES):
                jobs.append(
                    await enqueue_job(
                        db,
                        operation_key=f'mode:{uuid4()}',
                        panel_scope=reader.panel_scope,
                        day=day,
                        node_uuids=tuple(nodes[start : start + MAX_NODES]),
                        identities={panel_id: generation},
                    )
                )
        for identifier in jobs:
            if not await run_once(reader._factory, reader, reader.panel_scope, job_id=identifier):
                raise LedgerConflict('Mode collection is busy or paused')
        await reader.assert_current()
        async with reader._factory() as db:
            now = await db.scalar(select(func.clock_timestamp()))
            await completed_mode_collection(
                db,
                job_ids=jobs,
                scope=reader.panel_scope,
                panel_id=panel_id,
                generation=generation,
                nodes=nodes,
                day=day,
                now=now,
            )
        return tuple(jobs)


async def completed_mode_collection(db, *, job_ids, scope, panel_id, generation, nodes, day, now):
    """A full parsed response may omit an account; it does not measure zero.

    Exact request membership survives identity-map cleanup in the immutable
    request hash. A job selecting additional accounts is not this proof.
    """
    if (
        not isinstance(job_ids, (tuple, list))
        or not 1 <= len(job_ids) <= 2
        or len(set(job_ids)) != len(job_ids)
        or any(type(value) is not int or value <= 0 for value in job_ids)
        or not 1 <= len(nodes) <= 200
    ):
        raise LedgerConflict('Bounded exact mode collection required')
    rows = (
        await db.execute(
            select(Job, Poll)
            .join(Poll, Poll.id == Job.poll_id)
            .where(Job.id.in_(job_ids), Job.panel_scope == scope)
            .execution_options(populate_existing=True)
        )
    ).all()
    if len(rows) != len(job_ids):
        raise LedgerConflict('Mode collection is incomplete')
    result = {}
    for job, poll in rows:
        expected_hash = collection_request_hash(
            panel_scope=scope, day=day, node_uuids=tuple(job.node_uuids), identities={panel_id: generation}
        )
        if (
            job.state != 'complete'
            or job.day != day
            or job.selected_accounts != 1
            or job.request_hash != expected_hash
            or poll.panel_scope != scope
            or poll.day != day
            or poll.selected_accounts != 1
            or poll.node_uuids != job.node_uuids
            or poll.response_nodes is None
            or not poll.response_hash
            or not poll.created_at <= now < poll.created_at + timedelta(seconds=30)
            or set(result).intersection(job.node_uuids)
        ):
            raise LedgerConflict('Mode collection membership is stale or changed')
        result.update(dict.fromkeys(job.node_uuids, poll))
    if set(result) != set(nodes):
        raise LedgerConflict('Mode collection does not cover the exact target nodes')
    return result
