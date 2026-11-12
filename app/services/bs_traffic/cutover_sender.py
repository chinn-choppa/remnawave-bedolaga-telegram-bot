"""Single-attempt cutover sender; internal only, no scheduler or activation.

Attempt is committed before preflight. Only explicit exits in the live claim
holder before calling the writer may record not_sent. Exceptions/cancellation
retain attempted, even if raised before PATCH: another worker cannot prove the
original coroutine will never send. Unknown outcomes only get readback.
Local locks cannot fence a remote admin edit; no remote CAS exists here.
"""

import asyncio

from sqlalchemy import func, select

from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.cutover_baseline import inspect_cutover_baseline
from app.services.bs_traffic.cutover_intent import _row, claim_cutover
from app.services.bs_traffic.cutover_recovery import _load, recover_cutover
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_cutover import _build_target, _pending_snapshot
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause, remaining_pause
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.panel_sync.writer import patch_split_cutover_target


async def send_cutover(reader, *, user_id, subscription_id, account_generation, operation_id):
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound cutover reader required')
    factory = reader._factory
    local = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    keys = dict(local, operation_id=operation_id)
    attempt = None
    try:
        async with asyncio.timeout(30):
            await reader.assert_current()
            async with factory() as db, db.begin():
                await _recovery_lock(db, user_id, subscription_id, account_generation)
                existing = await _row(db, subscription_id=subscription_id, operation_id=operation_id)
                recovery_only = existing.state != 'pending'
            if recovery_only:
                return await recover_cutover(reader, **keys)
            async with panel_slot(factory, reader.panel_scope):
                async with factory() as db, db.begin():
                    snapshot = await _pending_snapshot(db, reader, **local)
                    try:
                        baseline = await inspect_cutover_baseline(db, reader, **local)
                    except LedgerConflict:
                        return 'accounting_pending'
                policy = snapshot['policy']
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
                async with factory() as db, db.begin():
                    try:
                        ready = await inspect_cutover_baseline(db, reader, **local)
                    except LedgerConflict:
                        return 'accounting_pending'
                    now = await db.scalar(select(func.clock_timestamp()))
                    if ready.fingerprint != baseline.fingerprint or now >= min(ready.valid_until, baseline.valid_until):
                        return 'accounting_pending'
                    attempt = await claim_cutover(db, reader, **keys)
                if attempt is not None:
                    async with factory() as db, db.begin():
                        status, row, target = await _load(db, reader, **keys)

                        async def abort_unsent(reason):
                            # Only used by explicit preflight exits below; never
                            # from an exception handler or recovery process.
                            if row.state == 'attempted' and row.attempt_token == attempt.token:
                                row.state, row.not_sent_reason = 'not_sent', reason
                                await db.flush()
                            return reason

                        if status != 'attempted' or row.attempt_token != attempt.token:
                            return await abort_unsent('obsolete')
                        now = await db.scalar(select(func.clock_timestamp()))
                        if attempt.send_deadline <= now or not evidence.is_fresh(
                            now, panel_identity=reader.panel_scope
                        ):
                            return await abort_unsent('obsolete')
                        if target.external_squad_uuid is not None:
                            if await reader.split_external_squad_exists(target.external_squad_uuid) is not True:
                                return await abort_unsent('external_missing')
                        current = await reader.get_split_grace_snapshot(target.id)
                        if not full_restore_confirmed(current, target):
                            if not full_restore_confirmed(current, attempt.expected_source):
                                return await abort_unsent('source_changed')
                            snapshot = await _pending_snapshot(db, reader, **local)
                            try:
                                desired = _build_target(
                                    snapshot,
                                    current,
                                    evidence,
                                    panel_scope=reader.panel_scope,
                                    account_generation=account_generation,
                                )
                            except LedgerConflict:
                                return await abort_unsent('policy_changed')
                            if not full_restore_confirmed(desired, target):
                                return await abort_unsent('policy_changed')
                            try:
                                ready = await inspect_cutover_baseline(db, reader, **local)
                            except LedgerConflict:
                                return await abort_unsent('accounting_pending')
                            if ready.fingerprint != baseline.fingerprint:
                                return await abort_unsent('accounting_pending')
                            status, checked, _ = await _load(db, reader, **keys)
                            now = await db.scalar(select(func.clock_timestamp()))
                            if (
                                status != 'attempted'
                                or checked.attempt_token != attempt.token
                                or now
                                >= min(
                                    attempt.send_deadline,
                                    row.valid_until,
                                    snapshot['valid_until'],
                                    baseline.valid_until,
                                    ready.valid_until,
                                )
                                or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
                            ):
                                return await abort_unsent('obsolete')
                            if await remaining_pause(db, panel_scope=reader.panel_scope) > 0:
                                return await abort_unsent('deferred')
                            await reader.assert_current()
                            await patch_split_cutover_target(reader._api, target=target)
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
        if attempt is None:
            return 'unconfirmed'
    # Also handles retries: claim returns None, so they may only read back.
    try:
        return await recover_cutover(reader, **keys)
    except LedgerConflict:
        return 'obsolete'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
