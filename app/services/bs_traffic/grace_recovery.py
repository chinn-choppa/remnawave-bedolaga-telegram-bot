"""Read-only confirmation of a committed full grace restore attempt.

No PATCH, reset, drop or grace completion. 'verified' on a repeated call refers
only to the recorded historical result, never permission to skip fresh guards
in a subsequent grace completion transaction. HTTP is outside business locks.
"""

import asyncio

from sqlalchemy import func, select

from app.database.models import BsTrafficGraceIntent, BsTrafficPanelBinding, GraceAccessSessionModel
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.grace_intent import _decode, _restore_guard
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for


async def _load(
    db,
    reader,
    user_id,
    subscription_id,
    account_generation,
    operation_id,
    *,
    revalidate_verified=False,
    allow_revision_change=False,
):
    # Only readback/completion may tolerate a changed usage revision. The
    # sender still requires its exact claim revision, and completion rebuilds
    # the current paid target before it may close the overlay.
    ledger, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
    row = await db.scalar(
        select(BsTrafficGraceIntent)
        .where(
            BsTrafficGraceIntent.id == operation_id,
            BsTrafficGraceIntent.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        row is None
        or row.panel_scope != reader.panel_scope
        or ledger.panel_scope != reader.panel_scope
        or row.account_generation != account_generation
    ):
        raise LedgerConflict('Restore recovery belongs to another account or panel')
    if row.state != 'attempted' and not (revalidate_verified and row.state == 'verified'):
        return row.state, row, None
    target = _decode(row.target, row.target_hash, paid=row.completion_reason == 'paid')
    if target.id != panel_id or generation_of(panel_scope=reader.panel_scope, account=target) != account_generation:
        raise LedgerConflict('Restore target identity is obsolete')
    grace = await db.scalar(
        select(GraceAccessSessionModel)
        .where(
            GraceAccessSessionModel.id == row.grace_id,
            GraceAccessSessionModel.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == reader.panel_scope,
            BsTrafficPanelBinding.endpoint_hash == reader.endpoint_hash,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    try:
        current_guard = await _restore_guard(db, user_id, subscription_id, now)
    except LedgerConflict:
        return 'obsolete', row, None
    if (
        binding is None
        or grace is None
        or grace.state != 'restoring'
        or grace.version != row.grace_version
        or (not allow_revision_change and ledger.revision != row.ledger_revision)
        or (target.status == 'ACTIVE' and target.expire_at <= now)
        or row.guard_hash != current_guard
    ):
        return 'obsolete', row, None
    return row.state, row, target


async def recover_restore(reader, *, user_id, subscription_id, account_generation, operation_id):
    if not isinstance(reader, BoundPanelReader) or type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Bound reader and exact restore operation required')
    factory = reader._factory
    args = (reader, user_id, subscription_id, account_generation, operation_id)
    async with asyncio.timeout(30):
        await reader.assert_current()
        async with factory() as db, db.begin():
            status, row, target = await _load(db, *args, allow_revision_change=True)
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
            status, row, current_target = await _load(db, *args, allow_revision_change=True)
            if status != 'attempted':
                return status
            if expected != (row.attempt_token, row.target_hash, row.guard_hash) or current_target != target:
                return 'obsolete'
            verified_at = await db.scalar(select(func.clock_timestamp()))
            row.state = 'verified'
            row.verified_at = verified_at
            await db.flush()
        return 'verified'
