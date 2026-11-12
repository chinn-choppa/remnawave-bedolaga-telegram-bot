"""Read-only recovery of durable split writes, not the delivery scheduler.

No PATCH or drop is issued here. The caller supplies current independently
validated external topology/ownership evidence; local entitlement state is
checked again after HTTP. No SQL transaction or row lock spans network I/O.
Verification records historical delivery, not permission for a later drop:
that operation must recheck current entitlements, topology and grace separately.
"""

import asyncio
from datetime import timedelta

from sqlalchemy import func, select

from app.database.models import Subscription, User
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.delivery_plan import _selection
from app.services.bs_traffic.delivery_store import SEND_WINDOW_SECONDS, _operation, reconcile
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, _lock
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_pause import persist_panel_pause, read_panel_pause
from app.services.bs_traffic.policy import _fingerprint
from app.services.bs_traffic.recovery_backoff import RecoveryBackoff, backoff_for
from app.services.grace_access_runtime import _acquire_database_lock


READ_TIMEOUT = 15


def _observed_squads(account):
    rows = getattr(account, 'active_internal_squads', None)
    if not isinstance(rows, list) or len(rows) > 128:
        raise ValueError('Explicit panel squad list required')
    if any(not isinstance(row, dict) or not isinstance(row.get('uuid'), str) for row in rows):
        raise ValueError('Invalid panel squad list')
    values = tuple(sorted(row['uuid'] for row in rows))
    _selection(values)
    return values


async def _recovery_lock(db, user_id, subscription_id, generation):
    if any(type(value) is not int or value <= 0 for value in (user_id, subscription_id)):
        raise LedgerConflict('Invalid recovery target')
    if db.get_bind().dialect.name != 'postgresql':
        raise LedgerConflict('Recovery requires PostgreSQL')
    await db.execute(select(User.id).where(User.id == user_id).with_for_update())
    owner = await db.scalar(select(Subscription.user_id).where(Subscription.id == subscription_id))
    if owner != user_id:
        raise LedgerConflict('Recovery target belongs to another owner')
    # Reuse global/shared and per-subscription grace leases, including when
    # runtime mode is currently disabled. No lease spans the HTTP call.
    await _acquire_database_lock(db, subscription_id)
    return await _lock(db, user_id, subscription_id, generation)


async def recover_delivery(
    session_factory,
    api,
    *,
    user_id,
    subscription_id,
    account_generation,
    operation_id,
    current_guard_hash,
    completed_attempt_token=None,
    topology_evidence: TopologyEvidence | None = None,
    propagate_backoff: bool = False,
):
    """Recover attempted/attention only. Errors leave the original attempt intact.

    Caller must not treat `already_verified` as proof of current access. A
    verified row is immutable history, not a renewable authorization token.
    """
    if not _fingerprint(current_guard_hash):
        raise LedgerConflict('Fresh external delivery guard required')
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with session_factory() as db, db.begin():
        state, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
        row = await _operation(db, subscription_id, operation_id)
        if row.state == 'verified':
            return 'already_verified'
        if row.state == 'not_sent':
            return 'not_sent'
        if row.state not in ('attempted', 'attention'):
            return 'not_attempted'
        now = await db.scalar(select(func.clock_timestamp()))
        if (
            row.state == 'attempted'
            and completed_attempt_token != row.token
            and row.attempted_at + timedelta(seconds=SEND_WINDOW_SECONDS) > now
        ):
            return 'in_progress'
        if topology_evidence is not None and not topology_evidence.is_fresh(now, panel_identity=state.panel_scope):
            return 'unconfirmed'
        scope, token = state.panel_scope, row.token
    remaining = await read_panel_pause(session_factory, scope)
    if remaining > 0:
        if propagate_backoff:
            raise RecoveryBackoff(remaining)
        return 'unconfirmed'
    try:
        async with asyncio.timeout(READ_TIMEOUT):
            async with panel_slot(session_factory, scope):
                account = await api.get_split_access_snapshot(panel_id)
        if account is None or generation_of(panel_scope=scope, account=account) != account_generation:
            return 'unconfirmed'
        observed = _observed_squads(account)
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(session_factory, scope, pause.retry_after_seconds)
            if propagate_backoff:
                raise pause from None
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
    async with session_factory() as db, db.begin():
        try:
            state, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
            row = await _operation(db, subscription_id, operation_id)
            if row.state == 'verified':
                return 'already_verified'
            if row.state == 'not_sent':
                return 'not_sent'
            now = await db.scalar(select(func.clock_timestamp()))
            if topology_evidence is not None and not topology_evidence.is_fresh(now, panel_identity=state.panel_scope):
                return 'unconfirmed'
            return await reconcile(
                db,
                **target,
                operation_id=operation_id,
                token=token,
                current_guard_hash=current_guard_hash,
                observed_squads=observed,
                topology_evidence=topology_evidence,
            )
        except LedgerConflict:
            # Account transfer/recreation during HTTP is not readback success.
            return 'obsolete'
