"""Durable cutover staging/claim, without HTTP or accounting activation.

Caller must commit the claim before any HTTP. Once attempted, a request may
only be reconciled by reading the panel; expiry does not permit another claim.
A claim is not permission to skip full source/topology checks before sending.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select

from app.database.models import BsTrafficCutoverIntent as Intent
from app.external.remnawave_api import SplitGraceSnapshot
from app.services.bs_traffic.grace_intent import _decode, _encode
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, _key
from app.services.bs_traffic.managed_cutover import ManagedCutoverPlan, _build_target, _pending_snapshot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.readback import _recovery_lock


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def _row(db, *, subscription_id, operation_id):
    if type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Exact cutover operation required')
    row = await db.scalar(
        select(Intent)
        .where(
            Intent.id == operation_id,
            Intent.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise LedgerConflict('Cutover operation belongs to another subscription')
    return row


async def stage_cutover(db, reader, *, user_id, subscription_id, account_generation, operation_key, plan):
    _key(operation_key)
    if not isinstance(reader, BoundPanelReader) or not isinstance(plan, ManagedCutoverPlan):
        raise LedgerConflict('Bound reader and prepared cutover required')
    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    if ledger.panel_scope != reader.panel_scope:
        raise LedgerConflict('Cutover panel changed')
    target, source = _encode(plan.target), _encode(plan.expected_source, paid=False)
    expected = (
        plan.revision,
        plan.guard_hash,
        _hash(target),
        _hash(source),
        plan.valid_until,
        reader.panel_scope,
        account_generation,
    )
    old = await db.scalar(
        select(Intent).where(Intent.subscription_id == subscription_id, Intent.operation_key == operation_key)
    )
    if old is not None:
        if (
            old.ledger_revision,
            old.guard_hash,
            old.target_hash,
            old.source_hash,
            old.valid_until,
            old.panel_scope,
            old.account_generation,
        ) != expected:
            raise LedgerConflict('Cutover replay cannot change its original terms')
        return old.id  # Historical receipt, never a fresh send authorization.
    snapshot = await _pending_snapshot(db, reader, **keys)
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        plan.revision != snapshot['revision']
        or plan.guard_hash != snapshot['guard_hash']
        or not now < plan.valid_until <= snapshot['valid_until']
        or plan.valid_until > plan.evidence.valid_until
        or not plan.evidence.is_fresh(now, panel_identity=reader.panel_scope)
    ):
        raise LedgerConflict('Cutover preparation is stale')
    computed = _build_target(
        snapshot,
        plan.expected_source,
        plan.evidence,
        panel_scope=reader.panel_scope,
        account_generation=account_generation,
    )
    if _encode(computed) != target:
        raise LedgerConflict('Cutover target does not match current paid rights')
    if (
        await db.scalar(
            select(Intent.id)
            .where(
                Intent.subscription_id == subscription_id,
                Intent.ownership_cycle == ledger.ownership_cycle,
                Intent.state.not_in(('cancelled', 'not_sent')),
            )
            .limit(1)
        )
        is not None
    ):
        raise LedgerConflict('Previous cutover requires reconciliation')
    row = Intent(
        subscription_id=subscription_id,
        ownership_cycle=ledger.ownership_cycle,
        operation_key=operation_key,
        ledger_revision=plan.revision,
        panel_scope=reader.panel_scope,
        account_generation=account_generation,
        target=target,
        expected_source=source,
        target_hash=_hash(target),
        source_hash=_hash(source),
        guard_hash=plan.guard_hash,
        valid_until=plan.valid_until,
        state='pending',
    )
    db.add(row)
    await db.flush()
    return row.id


@dataclass(frozen=True)
class CutoverClaim:
    operation_id: int
    token: str
    target: SplitGraceSnapshot
    expected_source: SplitGraceSnapshot
    send_deadline: datetime


async def claim_cutover(db, reader, *, user_id, subscription_id, account_generation, operation_id):
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound cutover reader required')
    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    row = await _row(db, subscription_id=subscription_id, operation_id=operation_id)
    if (
        row.panel_scope != reader.panel_scope
        or ledger.panel_scope != reader.panel_scope
        or row.account_generation != account_generation
    ):
        raise LedgerConflict('Cutover claim belongs to another panel account')
    if row.state != 'pending':
        return None
    now = await db.scalar(select(func.clock_timestamp()))
    if row.valid_until <= now:
        row.state = 'cancelled'
        await db.flush()
        return None
    snapshot = await _pending_snapshot(db, reader, **keys)
    if snapshot['revision'] != row.ledger_revision or snapshot['guard_hash'] != row.guard_hash:
        raise LedgerConflict('Cutover rights changed before claim')
    target, source = _decode(row.target, row.target_hash), _decode(row.expected_source, row.source_hash, paid=False)
    if any(
        generation_of(panel_scope=reader.panel_scope, account=item) != account_generation for item in (target, source)
    ):
        raise LedgerConflict('Stored cutover identity changed')
    now = await db.scalar(select(func.clock_timestamp()))
    deadline = min(row.valid_until, snapshot['valid_until'], now + timedelta(seconds=15))
    if deadline <= now:
        row.state = 'cancelled'
        await db.flush()
        return None
    row.state, row.attempt_token, row.attempted_at, row.send_deadline = 'attempted', str(uuid4()), now, deadline
    await db.flush()
    return CutoverClaim(row.id, row.attempt_token, target, source, deadline)


async def cancel_pending_cutover(db, *, user_id, subscription_id, account_generation, operation_id):
    """Cancel only unclaimed work. A timeout or stale attempted write is not unsent."""
    await _recovery_lock(db, user_id, subscription_id, account_generation)
    row = await _row(db, subscription_id=subscription_id, operation_id=operation_id)
    if row.state != 'pending':
        return False
    row.state = 'cancelled'
    await db.flush()
    return True
