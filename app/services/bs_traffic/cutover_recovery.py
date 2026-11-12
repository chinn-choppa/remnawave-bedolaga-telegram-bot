"""Read-only confirmation of a persisted cutover attempt; never PATCH or activate.

Historical 'verified' is not fresh permission to complete the accounting switch.
The future activation transaction must revalidate remote state and local rights.
"""

import asyncio

from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.cutover_intent import _row
from app.services.bs_traffic.grace_intent import _decode
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_cutover import _pending_snapshot
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for


async def _load(db, reader, *, user_id, subscription_id, account_generation, operation_id, revalidate_verified=False):
    ledger, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
    row = await _row(db, subscription_id=subscription_id, operation_id=operation_id)
    if (
        row.panel_scope != reader.panel_scope
        or ledger.panel_scope != reader.panel_scope
        or row.account_generation != account_generation
    ):
        raise LedgerConflict('Cutover recovery belongs to another panel account')
    if row.state != 'attempted' and not (revalidate_verified and row.state == 'verified'):
        return row.state, row, None
    snapshot = await _pending_snapshot(
        db, reader, user_id=user_id, subscription_id=subscription_id, account_generation=account_generation
    )
    if row.guard_hash != snapshot['guard_hash'] or row.ledger_revision != snapshot['revision']:
        return 'obsolete', row, None
    target = _decode(row.target, row.target_hash)
    if target.id != panel_id or generation_of(panel_scope=reader.panel_scope, account=target) != account_generation:
        raise LedgerConflict('Cutover target identity changed')
    return row.state, row, target


async def recover_cutover(reader, *, user_id, subscription_id, account_generation, operation_id):
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound cutover reader required')
    keys = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        operation_id=operation_id,
    )
    factory = reader._factory
    async with asyncio.timeout(30):
        await reader.assert_current()
        async with factory() as db, db.begin():
            status, row, target = await _load(db, reader, **keys)
            if status != 'attempted':
                return status
            expected = (row.attempt_token, row.target_hash, row.guard_hash)
        try:
            async with panel_slot(factory, reader.panel_scope):
                observed = await reader.get_split_grace_snapshot(target.id)
        except RemnaWaveAPIError as exc:
            pause = backoff_for(exc)
            if pause is not None:
                await persist_panel_pause(factory, reader.panel_scope, pause.retry_after_seconds)
            return 'unconfirmed'
        if not full_restore_confirmed(observed, target):
            return 'unconfirmed'
        await reader.assert_current()
        async with factory() as db, db.begin():
            status, row, current_target = await _load(db, reader, **keys)
            if status != 'attempted':
                return status
            if expected != (row.attempt_token, row.target_hash, row.guard_hash) or current_target != target:
                return 'obsolete'
            row.state = 'verified'
            await db.flush()
        return 'verified'
