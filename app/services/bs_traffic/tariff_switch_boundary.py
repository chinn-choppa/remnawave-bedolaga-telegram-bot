"""Explicit current-day baselines when an unlimited group becomes finite.

The source response is not a flush watermark. A previously unlimited sample
cannot debit a newly purchased finite quota. Preserve the prior cursor in the
financial receipt instead of silently resetting its accounting provenance.
"""

from datetime import UTC, timedelta

from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficObservation, BsTrafficPoll, BsTrafficUsageCursor
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner
from app.services.bs_traffic.mode_collection import baseline_nodes, completed_mode_collection
from app.services.bs_traffic.policy import _read_policy


async def adopt_mode_baselines(
    db, *, user_id, subscription_id, account_generation, new_modes, collection_job_ids=(), target_topology=None
):
    """Caller saves the returned manifest with debit/grants in one transaction.

    No policy, money, grants or HTTP change here. Existing pending operations
    retain their frozen eligibility. Every source is validated before adoption.
    Missing rows remain missing, including an omitted account in a full response.
    """
    if (
        not isinstance(new_modes, dict)
        or set(new_modes) != {'ordinary', 'bs'}
        or any(value not in ('finite', 'unlimited') for value in new_modes.values())
    ):
        raise LedgerConflict('Explicit target traffic modes required')
    async with db.begin_nested():
        state, _ = await _lock(db, user_id, subscription_id, account_generation)
        await require_activated_owner(db, subscription_id)
        policy = _read_policy(state.access_policy)
        expected = baseline_nodes(policy, new_modes, target_topology)
        entering = {node for node, group in expected.items() if node not in policy['topology'][f'{group}_nodes']}
        if not expected:
            return {}
        if len(expected) > 200:
            raise LedgerConflict('Mode baseline node budget exceeded')
        now = await db.scalar(select(func.clock_timestamp()))
        day = now.astimezone(UTC).date()
        rows = (
            await db.execute(
                select(BsTrafficObservation, BsTrafficPoll)
                .join(BsTrafficPoll, BsTrafficPoll.id == BsTrafficObservation.latest_poll_id)
                .where(
                    BsTrafficObservation.panel_scope == state.panel_scope,
                    BsTrafficObservation.panel_user_id == state.panel_user_id,
                    BsTrafficObservation.account_generation == account_generation,
                    BsTrafficObservation.day == day,
                    BsTrafficObservation.node_uuid.in_(expected),
                )
                .order_by(BsTrafficObservation.node_uuid)
                .with_for_update(of=(BsTrafficObservation, BsTrafficPoll))
                .execution_options(populate_existing=True)
            )
        ).all()
        missing = set(expected) - {raw.node_uuid for raw, _ in rows}
        omitted = {}
        if missing:
            if settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
                raise LedgerConflict('Explicit current-day mode baseline missing')
            proof = await completed_mode_collection(
                db,
                job_ids=collection_job_ids,
                scope=state.panel_scope,
                panel_id=state.panel_user_id,
                generation=account_generation,
                nodes=tuple(expected),
                day=day,
                now=now,
            )
            omitted = {node: proof[node] for node in missing}
        cursors = {
            cursor.node_uuid: cursor
            for cursor in await db.scalars(
                select(BsTrafficUsageCursor)
                .where(
                    BsTrafficUsageCursor.subscription_id == subscription_id,
                    BsTrafficUsageCursor.node_uuid.in_(expected),
                    BsTrafficUsageCursor.day == day,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        }
        for raw, poll in rows:
            old = cursors.get(raw.node_uuid)
            if (
                raw.needs_review
                or raw.high_water_bytes != raw.latest_bytes
                or poll.panel_scope != state.panel_scope
                or poll.day != day
                or raw.node_uuid not in poll.node_uuids
                or poll.response_nodes is None
                or not poll.response_hash
                or not poll.created_at <= now < poll.created_at + timedelta(seconds=30)
                or (state.ownership_started_at is not None and poll.created_at < state.ownership_started_at)
                or (
                    old is not None
                    and (
                        old.account_generation != account_generation
                        or old.ownership_cycle != state.ownership_cycle
                        or (old.traffic_group != expected[raw.node_uuid] and raw.node_uuid not in entering)
                        or old.needs_review
                        or old.processed_bytes > raw.high_water_bytes
                        or (old.last_poll_started_at is not None and old.last_poll_started_at > poll.created_at)
                    )
                )
            ):
                raise LedgerConflict('Mode baseline source is stale, reset or belongs to another owner')
        for node, poll in omitted.items():
            old = cursors.get(node)
            if state.ownership_started_at is not None and poll.created_at < state.ownership_started_at:
                raise LedgerConflict('Mode collection predates current ownership')
            if old is not None and (
                old.account_generation != account_generation
                or old.ownership_cycle != state.ownership_cycle
                or (old.traffic_group != expected[node] and node not in entering)
                or old.needs_review
                or old.processed_bytes != 0
            ):
                raise LedgerConflict('Omitted mode baseline has incompatible prior cursor')
        manifest = {}
        sources = [(raw.node_uuid, raw, poll) for raw, poll in rows] + [
            (node, None, poll) for node, poll in omitted.items()
        ]
        for node, raw, poll in sources:
            group = expected[node]
            old = cursors.get(node)
            previous = (
                None
                if old is None
                else {
                    'baseline_bytes': old.baseline_bytes,
                    'processed_bytes': old.processed_bytes,
                    'last_poll_started_at': old.last_poll_started_at.isoformat() if old.last_poll_started_at else None,
                    'bootstrap_evidence': old.bootstrap_evidence,
                }
            )
            cursor = old or BsTrafficUsageCursor(subscription_id=subscription_id, node_uuid=node, day=day)
            cursor.account_generation = account_generation
            cursor.ownership_cycle = state.ownership_cycle
            cursor.traffic_group = group
            amount = raw.high_water_bytes if raw is not None else 0
            cursor.baseline_bytes = cursor.processed_bytes = amount
            cursor.last_poll_started_at = poll.created_at if raw is not None else None
            cursor.needs_review = False
            cursor.bootstrap_evidence = (
                dict(version=1, kind='topology', first_observed_poll_id=None, first_observed_day=None)
                if node in entering
                else None
            )
            if raw is None:
                cursor.bootstrap_evidence = dict(
                    version=1,
                    kind='omitted',
                    scope=state.panel_scope,
                    panel_id=state.panel_user_id,
                    generation=account_generation,
                    cycle=state.ownership_cycle,
                    node=node,
                    day=day.isoformat(),
                    poll_id=poll.id,
                    response_hash=poll.response_hash,
                    collection_job_ids=list(collection_job_ids),
                    first_observed_day=None,
                    first_observed_poll_id=None,
                )
            if old is None:
                db.add(cursor)
            manifest.setdefault(group, {'finite_since': now.isoformat(), 'nodes': []})['nodes'].append(
                {
                    'node': node,
                    'kind': 'observed' if raw is not None else 'omitted',
                    'day': day.isoformat(),
                    'poll_id': poll.id,
                    'response_hash': poll.response_hash,
                    'sample_started_at': poll.created_at.isoformat(),
                    'baseline_bytes': amount,
                    'previous_cursor': previous,
                }
            )
        state.revision += 1
        await db.flush()
        return manifest
