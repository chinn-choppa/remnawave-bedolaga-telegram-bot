"""Collect pre-grace split context through an explicitly bound read-only client.

This does not start grace, debit usage or establish a traffic flush boundary.
The caller supplies panel_before. Its controlled fields and counter continuity
are independently checked against a strict full read. The grace coordinator
must still validate the expected overlay and full target under its write lock;
this read does not fence later manual edits or authorize a panel write.
"""

import asyncio
from copy import deepcopy

from sqlalchemy import exists, func, select

from app.database.models import GraceAccessSessionModel, Subscription
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.delivery_store import _local_guard
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.policy import _read_policy, _topology
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.grace_access_service import GracePanelSnapshot, GraceSplitAccess


async def _open_grace(db, subscription_id):
    return bool(
        await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == subscription_id))
        or await db.scalar(
            select(
                exists().where(
                    GraceAccessSessionModel.subscription_id == subscription_id,
                    GraceAccessSessionModel.state != 'completed',
                )
            )
        )
    )


async def capture_grace_context(reader, *, user_id, subscription_id, account_generation, panel_before):
    if not isinstance(reader, BoundPanelReader) or not isinstance(panel_before, GracePanelSnapshot):
        raise LedgerConflict('Bound panel reader and full pre-grace snapshot required')
    factory = reader._factory
    async with asyncio.timeout(30):
        await reader.assert_current()
        async with factory() as db, db.begin():
            state, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
            if state.panel_scope != reader.panel_scope or panel_before.remnawave_id != panel_id:
                raise LedgerConflict('Grace source belongs to another panel account')
            if await _open_grace(db, subscription_id):
                raise LedgerConflict('Grace source is already overlaid')
            from app.services.bs_traffic.grace_topology import unsent_grace_transition

            transition = await unsent_grace_transition(db, state)
            transition_source = deepcopy(transition[1]['source']) if transition is not None else None
            policy, revision = deepcopy(_read_policy(state.access_policy)), state.revision
            now = await db.scalar(select(func.clock_timestamp()))
            guard = await _local_guard(db, user_id, subscription_id, now)
        try:
            async with panel_slot(factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
                source_evidence = None
                if transition_source is not None:
                    from app.services.bs_traffic.tariff_switch_topology import manifest

                    source_evidence = await collect_topology_evidence(
                        reader,
                        panel_identity=reader.panel_scope,
                        ordinary_squads=transition_source['topology']['ordinary_squads'],
                        bs_squads=transition_source['topology']['bs_squads'],
                    )
                    if manifest(source_evidence) != transition_source:
                        raise LedgerConflict('Pre-grace transition source topology changed')
            async with panel_slot(factory, reader.panel_scope):
                account = await reader.get_split_grace_snapshot(panel_id)
        except RemnaWaveAPIError as exc:
            pause = backoff_for(exc)
            if pause is not None:
                await persist_panel_pause(factory, reader.panel_scope, pause.retry_after_seconds)
            raise
        if account is None or generation_of(panel_scope=reader.panel_scope, account=account) != account_generation:
            raise LedgerConflict('Pre-grace panel account changed')
        if (
            not panel_before.traffic_is_known
            or account.status != panel_before.status.upper()
            or account.expire_at != panel_before.expire_at
            or account.traffic_limit_bytes != panel_before.traffic_limit_bytes
            or account.external_squad_uuid != panel_before.external_squad_uuid
            or account.last_traffic_reset_at != panel_before.last_traffic_reset_at
            or account.used_traffic_bytes < panel_before.used_traffic_bytes
        ):
            raise LedgerConflict('Full pre-grace panel source changed')
        squads = _observed_squads(account)
        if squads != tuple(sorted(panel_before.squad_uuids)):
            raise LedgerConflict('Pre-grace panel selection changed')
        if evidence.fingerprint != policy['fingerprint'] or _topology(evidence.topology) != policy['topology']:
            raise LedgerConflict('Pre-grace topology changed')
        await reader.assert_current()
        async with factory() as db, db.begin():
            state, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
            now = await db.scalar(select(func.clock_timestamp()))
            tariff_id = await db.scalar(select(Subscription.tariff_id).where(Subscription.id == subscription_id))
            if (
                state.panel_scope != reader.panel_scope
                or state.revision != revision
                or state.access_policy != policy
                or tariff_id != policy['tariff_id']
                or guard != await _local_guard(db, user_id, subscription_id, now)
                or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
                or (
                    source_evidence is not None and not source_evidence.is_fresh(now, panel_identity=reader.panel_scope)
                )
                or await _open_grace(db, subscription_id)
            ):
                raise LedgerConflict('Subscription changed during pre-grace capture')
            from app.services.bs_traffic.grace_topology import validate_grace_selection

            await validate_grace_selection(db, state, squads)
            return GraceSplitAccess(
                subscription_id,
                panel_id,
                reader.panel_scope,
                account_generation,
                tariff_id,
                revision,
                evidence.fingerprint,
                squads,
                max(evidence.observed_at, source_evidence.observed_at) if source_evidence else evidence.observed_at,
                min(evidence.valid_until, source_evidence.valid_until) if source_evidence else evidence.valid_until,
            )
