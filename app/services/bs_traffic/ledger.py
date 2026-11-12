"""Transactional split grants and pending usage, without network or money I/O.

Caller owns commit/rollback and must issue a grant in the SAME transaction as
payment fulfillment. A usage interval is a classified consumption interval,
NOT a poll timestamp or the day when the panel flushed history. The settlement
coordinator must establish that classification and unique non-overlapping
source keys; raw observations must not be passed straight into this module.

Reported usage explicitly uses the observation request time for eligibility;
it is approximate accounting, not a classified consumption interval.
Pending usage freezes its eligible grants. Resolution never reaches newer
purchases, never moves excess to another group and never creates future debt.
"""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB

from app.config import settings
from app.database.models import (
    BsTrafficAllowance as Grant,
    BsTrafficCutoverIntent,
    BsTrafficLedger as Ledger,
    BsTrafficManagedTransfer,
    BsTrafficPanelBinding,
    BsTrafficUsage as Usage,
    BsTrafficUsageCursor as Cursor,
    Subscription,
    TrafficAccountingState,
    User,
)
from app.services.bs_traffic.base_extension import covers_interval
from app.services.bs_traffic.usage_math import fifo_allocations, usage_request_hash


UNENCUMBERED_SCAN_LIMIT = 64


class LedgerConflict(ValueError):
    pass


def _key(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 160 or value.strip() != value:
        raise LedgerConflict('Invalid operation identity')


def _group(value):
    if value not in ('ordinary', 'bs'):
        raise LedgerConflict('Invalid traffic group')


def _interval(start, end):
    for value in (start, end):
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise LedgerConflict('Explicit aware interval is required')
    if start >= end:
        raise LedgerConflict('Invalid interval')


def _amount(value, *, zero=False):
    if type(value) is not int or not (0 if zero else 1) <= value < 2**53:
        raise LedgerConflict('Exact nonnegative byte amount is required')


async def _lock(db, user_id, subscription_id, account_generation, *, optional=False):
    if db.get_bind().dialect.name != 'postgresql':
        raise LedgerConflict('Split ledger requires PostgreSQL')
    if any(type(v) is not int or v <= 0 for v in (user_id, subscription_id)):
        raise LedgerConflict('Invalid owner or subscription')
    if (
        not isinstance(account_generation, str)
        or len(account_generation) != 64
        or any(c not in '0123456789abcdef' for c in account_generation)
    ):
        raise LedgerConflict('Invalid account generation')
    # Same order as payment flows: owner -> subscription -> split records.
    owner = await db.scalar(select(User.id).where(User.id == user_id).with_for_update())
    target = (
        await db.execute(
            select(Subscription.id, Subscription.remnawave_id)
            .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
            .with_for_update()
        )
    ).first()
    if owner is None or target is None:
        raise LedgerConflict('Subscription does not belong to this owner')
    state = await db.scalar(
        select(Ledger)
        .where(Ledger.subscription_id == subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if state is None:
        if optional:
            return None, target.remnawave_id
        raise LedgerConflict('Split ledger is not initialized')
    if state.account_generation != account_generation or state.panel_user_id != target.remnawave_id:
        raise LedgerConflict('Panel account binding changed; reconciliation required')
    return state, target.remnawave_id


def activated_owner_query():
    """Shared persisted activation predicate for checks and bounded fleet scans."""
    return (
        select(BsTrafficCutoverIntent.id)
        .join(Ledger, Ledger.subscription_id == BsTrafficCutoverIntent.subscription_id)
        .join(BsTrafficManagedTransfer, BsTrafficManagedTransfer.subscription_id == Ledger.subscription_id)
        .join(TrafficAccountingState, TrafficAccountingState.subscription_id == Ledger.subscription_id)
        .join(Subscription, Subscription.id == Ledger.subscription_id)
        .join(BsTrafficPanelBinding, BsTrafficPanelBinding.panel_scope == Ledger.panel_scope)
        .where(
            Subscription.traffic_accounting_mode == 'split',
            BsTrafficPanelBinding.retired_at.is_(None),
            Ledger.panel_user_id == Subscription.remnawave_id,
            BsTrafficCutoverIntent.state == 'activated',
            BsTrafficCutoverIntent.ownership_cycle == Ledger.ownership_cycle,
            BsTrafficManagedTransfer.ownership_cycle == Ledger.ownership_cycle,
            BsTrafficCutoverIntent.activated_at.is_not(None),
            BsTrafficCutoverIntent.account_generation == Ledger.account_generation,
            BsTrafficCutoverIntent.panel_scope == Ledger.panel_scope,
            BsTrafficManagedTransfer.account_generation == Ledger.account_generation,
            BsTrafficManagedTransfer.panel_scope == Ledger.panel_scope,
            TrafficAccountingState.phase == 'transferred',
            TrafficAccountingState.panel_identity == BsTrafficManagedTransfer.source_identity,
            TrafficAccountingState.revision == BsTrafficManagedTransfer.source_revision + 1,
            TrafficAccountingState.used_bytes == BsTrafficManagedTransfer.source_used_bytes,
            TrafficAccountingState.lifetime_bytes == BsTrafficManagedTransfer.source_lifetime_bytes,
        )
    )


async def require_activated_owner(db, subscription_id, *, allow_restriction=False):
    """Require a persisted activation, not a manually changed mode string."""
    query = activated_owner_query().where(Ledger.subscription_id == subscription_id)
    if not allow_restriction:
        query = query.where(func.coalesce(Ledger.restriction_attempt['state'].as_string(), '') != 'attempted')
    identifier = await db.scalar(query)
    if identifier is None:
        raise LedgerConflict('Split accounting has no confirmed activation')
    return identifier


async def _require_no_pending_transfer(db, subscription_id):
    # Called only after _lock acquired the subscription row lock. Read the
    # persisted scalar rather than a possibly stale ORM Subscription instance.
    # Raw evidence and historical idempotent results remain available during
    # cutover; new money entitlements and classified debits must wait.
    mode = await db.scalar(select(Subscription.traffic_accounting_mode).where(Subscription.id == subscription_id))
    if mode == 'split_pending':
        raise LedgerConflict('Accounting transfer is pending panel confirmation')
    if mode == 'split':
        await require_activated_owner(db, subscription_id)
    if mode == 'managed':
        from app.database.models import BsTrafficOwnershipCycle

        current = await db.scalar(select(Ledger.ownership_cycle).where(Ledger.subscription_id == subscription_id))
        cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, current))
        if cycle is not None and cycle.state == 'managed':
            raise LedgerConflict('Split ownership was returned to ordinary accounting')


async def initialize(db, *, user_id, subscription_id, account_generation, panel_scope, panel_user_id):
    try:
        scope = str(UUID(panel_scope)) if isinstance(panel_scope, str) else None
    except ValueError:
        scope = None
    if scope is None or type(panel_user_id) is not int or not 0 < panel_user_id < 2**53:
        raise LedgerConflict('Explicit panel scope and account are required')
    state, bound_id = await _lock(db, user_id, subscription_id, account_generation, optional=True)
    if bound_id != panel_user_id:
        raise LedgerConflict('Panel account does not match subscription')
    if state is not None:
        if state.panel_scope != scope:
            raise LedgerConflict('Panel epoch changed')
        return
    db.add(
        Ledger(
            subscription_id=subscription_id,
            panel_scope=scope,
            panel_user_id=panel_user_id,
            account_generation=account_generation,
            revision=0,
        )
    )
    await db.flush()


async def grant(
    db, *, user_id, subscription_id, account_generation, group, operation_key, amount_bytes, starts_at, expires_at
):
    _key(operation_key)
    _group(group)
    _amount(amount_bytes)
    _interval(starts_at, expires_at)
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    old = await db.scalar(
        select(Grant)
        .where(
            Grant.subscription_id == subscription_id, Grant.traffic_group == group, Grant.operation_key == operation_key
        )
        .execution_options(populate_existing=True)
    )
    if old is not None:
        if (old.granted_bytes, old.starts_at, old.expires_at) != (amount_bytes, starts_at, expires_at):
            raise LedgerConflict('Grant operation was already used with different terms')
        return old.id
    await _require_no_pending_transfer(db, subscription_id)
    row = Grant(
        subscription_id=subscription_id,
        traffic_group=group,
        ownership_cycle=state.ownership_cycle,
        operation_key=operation_key,
        granted_bytes=amount_bytes,
        remaining_bytes=amount_bytes,
        starts_at=starts_at,
        expires_at=expires_at,
    )
    db.add(row)
    state.revision += 1
    await db.flush()
    return row.id


def eligible_grant_conditions(subscription_id, group, interval_start, interval_end):
    """Exact shared SQL eligibility, including paid base extensions/retirement."""
    return (
        Grant.subscription_id == subscription_id,
        Grant.ownership_cycle
        == select(Ledger.ownership_cycle).where(Ledger.subscription_id == subscription_id).scalar_subquery(),
        Grant.traffic_group == group,
        covers_interval(Grant, interval_start, interval_end),
        or_(Grant.retired_at.is_(None), Grant.retired_at >= interval_end),
        Grant.remaining_bytes > 0,
    )


async def open_usage(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    group,
    source_key,
    amount_bytes,
    interval_start,
    interval_end,
    evidence_kind='classified',
):
    """Freeze eligibility; reported intervals identify sampling, raw ones storage."""
    if evidence_kind not in ('classified', 'reported', 'raw_pending'):
        raise LedgerConflict('Unknown usage evidence kind')
    _key(source_key)
    _group(group)
    _amount(amount_bytes, zero=True)
    _interval(interval_start, interval_end)
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    digest = usage_request_hash(group, evidence_kind, account_generation, amount_bytes, interval_start, interval_end)
    old = await db.scalar(
        select(Usage)
        .where(Usage.subscription_id == subscription_id, Usage.source_key == source_key)
        .execution_options(populate_existing=True)
    )
    if old is not None:
        if old.request_hash != digest:
            raise LedgerConflict('Usage source was already used with different terms')
        return old.id
    if evidence_kind in ('classified', 'reported'):
        await _require_no_pending_transfer(db, subscription_id)
    ids = []
    if evidence_kind in ('classified', 'reported') and (
        state.ownership_started_at is None or interval_start >= state.ownership_started_at
    ):
        ids = (
            await db.scalars(
                select(Grant.id)
                .where(
                    *eligible_grant_conditions(subscription_id, group, interval_start, interval_end),
                )
                .order_by(Grant.expires_at, Grant.id)
                .limit(1001)
            )
        ).all()
    if len(ids) > 1000:
        raise LedgerConflict('Too many eligible grants; settlement requires attention')
    row = Usage(
        subscription_id=subscription_id,
        traffic_group=group,
        source_key=source_key,
        ownership_cycle=state.ownership_cycle,
        request_hash=digest,
        account_generation=account_generation,
        evidence_kind=evidence_kind,
        interval_start=interval_start,
        interval_end=interval_end,
        amount_bytes=amount_bytes,
        eligible_grants=list(ids),
        allocations=[],
        state='pending',
        charged_bytes=0,
        unallocated_bytes=0,
    )
    db.add(row)
    state.revision += 1
    await db.flush()
    return row.id


async def resolve_usage(db, *, user_id, subscription_id, account_generation, usage_id, decision):
    if decision not in ('charge', 'write_off'):
        raise LedgerConflict('Explicit settlement decision is required')
    if type(usage_id) is not int or usage_id <= 0:
        raise LedgerConflict('Invalid usage operation')
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    row = await db.scalar(
        select(Usage)
        .where(Usage.id == usage_id, Usage.subscription_id == subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None or row.account_generation != account_generation:
        raise LedgerConflict('Usage operation does not match this account')
    if decision == 'charge' and row.evidence_kind not in ('classified', 'reported'):
        raise LedgerConflict('Raw history is not a classified consumption interval')
    final_state = 'charged' if decision == 'charge' else 'written_off'
    if row.state != 'pending':
        if row.state != final_state:
            raise LedgerConflict('Completed settlement cannot be reinterpreted')
        return
    if decision == 'charge':
        await _require_no_pending_transfer(db, subscription_id)
    pending = row.amount_bytes
    allocations = []
    if decision == 'charge' and row.eligible_grants:
        grants = (
            await db.scalars(
                select(Grant)
                .where(
                    Grant.id.in_(row.eligible_grants),
                    Grant.subscription_id == subscription_id,
                    Grant.traffic_group == row.traffic_group,
                )
                .order_by(Grant.expires_at, Grant.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
        if len(grants) != len(row.eligible_grants):
            raise LedgerConflict('Settlement grant history is incomplete')
        allocations, pending = fifo_allocations([(item.id, item.remaining_bytes) for item in grants], pending)
        charged = {item['grant_id']: item['bytes'] for item in allocations}
        for item in grants:
            item.remaining_bytes -= charged.get(item.id, 0)
    resolved_at = await db.scalar(select(func.clock_timestamp()))
    # The operator can opt out while this task is waiting for database locks.
    # Check immediately before changing a pending operation, not on entry.
    if decision == 'write_off' and settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
        raise LedgerConflict('Ambiguous usage write-off requires operator opt-in')
    row.state = final_state
    row.resolved_at = resolved_at
    row.charged_bytes = row.amount_bytes - pending
    row.unallocated_bytes = pending
    row.allocations = allocations
    state.revision += 1
    await db.flush()


async def summary(db, *, user_id, subscription_id, account_generation, now):
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise LedgerConflict('Aware summary time is required')
    await _lock(db, user_id, subscription_id, account_generation)
    balance_rows = (
        await db.execute(
            select(Grant.traffic_group, func.sum(Grant.remaining_bytes), func.count())
            .where(
                Grant.subscription_id == subscription_id,
                covers_interval(Grant, now, now, instant=True),
                or_(Grant.retired_at.is_(None), Grant.retired_at > now),
            )
            .group_by(Grant.traffic_group)
        )
    ).all()
    balances = {group: total for group, total, _ in balance_rows}
    grant_counts = {group: count for group, _, count in balance_rows}
    pending = {
        group: (int(total), count, raw)
        for group, total, count, raw in (
            await db.execute(
                select(
                    Usage.traffic_group,
                    func.sum(Usage.amount_bytes),
                    func.count(),
                    func.bool_or(Usage.evidence_kind == 'raw_pending'),
                )
                .where(Usage.subscription_id == subscription_id, Usage.state == 'pending')
                .group_by(Usage.traffic_group)
            )
        ).all()
    }
    review_groups = set(
        (
            await db.scalars(
                select(Cursor.traffic_group)
                .where(Cursor.subscription_id == subscription_id, Cursor.needs_review.is_(True))
                .distinct()
            )
        ).all()
    )
    # Only current grants outside every frozen classified debit are proven
    # available despite pending work. Raw buckets/review have no safe boundary.
    # This is not a forecast of unobserved traffic or a settlement decision.
    candidates = [
        group
        for group, (_, count, raw) in pending.items()
        if not raw
        and group not in review_groups
        and count <= UNENCUMBERED_SCAN_LIMIT
        and grant_counts.get(group, 0) <= UNENCUMBERED_SCAN_LIMIT
    ]
    free_balances = {}
    if candidates:
        # Expand the bounded frozen sets once, not once per current grant.
        # Grant ids are globally unique; ownership/group are filtered here.
        encumbered_ids = select(
            cast(func.jsonb_array_elements_text(cast(Usage.eligible_grants, JSONB)), BigInteger)
        ).where(
            Usage.subscription_id == subscription_id,
            Usage.traffic_group.in_(candidates),
            Usage.state == 'pending',
        )
        free_balances = dict(
            (
                await db.execute(
                    select(Grant.traffic_group, func.sum(Grant.remaining_bytes))
                    .where(
                        Grant.subscription_id == subscription_id,
                        Grant.traffic_group.in_(candidates),
                        covers_interval(Grant, now, now, instant=True),
                        or_(Grant.retired_at.is_(None), Grant.retired_at > now),
                        Grant.remaining_bytes > 0,
                        ~Grant.id.in_(encumbered_ids),
                    )
                    .group_by(Grant.traffic_group)
                )
            ).all()
        )
    return {
        group: {
            'remaining_bytes': int(balances.get(group, 0)),
            'unencumbered_bytes': (
                0
                if group in review_groups or pending.get(group, (0, 0, False))[2]
                else int(free_balances.get(group, 0))
                if group in pending
                else int(balances.get(group, 0))
            ),
            'needs_review': group in review_groups,
            'pending_bytes': pending.get(group, (0, 0))[0],
            'pending_operations': pending.get(group, (0, 0))[1],
        }
        for group in ('ordinary', 'bs')
    }
