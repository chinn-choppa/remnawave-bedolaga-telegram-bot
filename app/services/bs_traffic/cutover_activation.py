"""Confirm current panel/topology/intake, then atomically activate split ownership.

Internal only: no route, runtime enrollment or worker registration. A historical
activation receipt is not permission to skip guards on later financial writes.
"""

import asyncio

from sqlalchemy import func, select

from app.database.models import BsTrafficLedger, Subscription
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.cutover_baseline import inspect_cutover_baseline
from app.services.bs_traffic.cutover_recovery import _load
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.grace_intent import _decode
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_cutover import _build_target, _pending_snapshot
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.recovery_backoff import backoff_for


async def activate_cutover(reader, *, user_id, subscription_id, account_generation, operation_id):
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound cutover reader required')
    local = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    keys = dict(local, operation_id=operation_id, revalidate_verified=True)
    factory = reader._factory
    try:
        async with asyncio.timeout(30):
            await reader.assert_current()
            async with factory() as db, db.begin():
                status, row, target = await _load(db, reader, **keys)
                if status == 'activated':
                    return 'activated'  # Historical receipt; never mutates current state.
                if status != 'verified' or target is None:
                    return status
                snapshot = await _pending_snapshot(db, reader, **local)
                baseline = await inspect_cutover_baseline(db, reader, **local)
                expected = (row.attempt_token, row.target_hash, row.guard_hash)
                source = _decode(row.expected_source, row.source_hash, paid=False)
            policy = snapshot['policy']
            async with panel_slot(factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
                if target.external_squad_uuid is not None:
                    if await reader.split_external_squad_exists(target.external_squad_uuid) is not True:
                        return 'external_missing'
                observed = await reader.get_split_grace_snapshot(target.id)
            if not full_restore_confirmed(observed, target):
                return 'unconfirmed'
            await reader.assert_current()
            async with factory() as db, db.begin():
                status, row, current_target = await _load(db, reader, **keys)
                if status != 'verified' or current_target is None:
                    return status
                if expected != (row.attempt_token, row.target_hash, row.guard_hash) or current_target != target:
                    return 'obsolete'
                fresh = await inspect_cutover_baseline(db, reader, **local)
                snapshot = await _pending_snapshot(db, reader, **local)
                desired = _build_target(
                    snapshot, source, evidence, panel_scope=reader.panel_scope, account_generation=account_generation
                )
                now = await db.scalar(select(func.clock_timestamp()))
                if (
                    fresh.fingerprint != baseline.fingerprint
                    or now >= min(baseline.valid_until, fresh.valid_until, snapshot['valid_until'])
                    or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
                    or not full_restore_confirmed(observed, desired)
                ):
                    return 'obsolete'
                # No allowance, raw counter, pending usage or paid term is rewritten.
                (await db.get(Subscription, subscription_id)).traffic_accounting_mode = 'split'
                (await db.get(BsTrafficLedger, subscription_id)).revision += 1
                row.state, row.activated_at = 'activated', now
                await db.flush()
            return 'activated'
    except PanelBusy:
        return 'deferred'
    except LedgerConflict:
        return 'obsolete'
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
