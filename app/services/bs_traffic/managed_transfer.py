"""Read-only locked transfer manifest; not enrollment or permission to grant.

The old accountant keeps ownership. A later atomic transfer must revalidate this
manifest, establish the usage boundary and retire the old writer before importing
anything. Never debit ambiguous old usage from the proposed destination grants.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from sqlalchemy import func, or_, select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficPanelBinding,
    BsTrafficUsage,
    PaidTrafficResetIntent,
    Subscription,
    TrafficAccountingState,
    TrafficAllowance,
    TrafficBaseGrantIntent,
)
from app.services.bs_traffic.ledger import LedgerConflict, _amount, _group, _key, _lock
from app.services.traffic_accounting import AccountingConflict
from app.services.traffic_accounting_service import _assert_no_split_delivery, lock_accounting_subscription


TRANSFER_GRANT_LIMIT = 64


@dataclass(frozen=True)
class TransferGrant:
    source_key: str
    destination_key: str
    original_bytes: int
    remaining_bytes: int
    expires_at: datetime


@dataclass(frozen=True)
class ManagedTransferManifest:
    user_id: int
    subscription_id: int
    tariff_id: int
    subscription_status: str
    paid_until: datetime | None
    panel_scope: str
    panel_user_id: int
    account_generation: str
    ledger_revision: int
    source_identity: str
    source_revision: int
    source_phase: str
    lifetime_bytes: int
    used_bytes: int
    business_strategy: str
    calendar_timezone: str
    calendar_anchor: datetime
    next_refresh_at: datetime | None
    destination_group: str
    grants: tuple[TransferGrant, ...]
    total_bytes: int
    fingerprint: str


def _aware(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise LedgerConflict('Transfer dates must be explicitly timezone-aware')
    return value.astimezone(UTC)


async def inspect_managed_transfer(db, *, user_id, subscription_id, account_generation, destination_group):
    """Caller owns transaction; returned manifest becomes stale after lock release.

    No HTTP, write, debit, calendar refresh or default destination. Expired and
    exhausted source history stays in the old ledger; only live remainders are
    candidates. Overflow refuses the whole manifest rather than truncating it.
    """
    _group(destination_group)
    if any(type(value) is not int or value <= 0 for value in (user_id, subscription_id)):
        raise LedgerConflict('Exact transfer owner required')
    owner = await db.scalar(select(Subscription.user_id).where(Subscription.id == subscription_id))
    if owner != user_id:
        raise LedgerConflict('Transfer subscription belongs to another owner')
    try:
        subscription, grace_open = await lock_accounting_subscription(db, subscription_id)
        ledger, _ = await _lock(db, user_id, subscription_id, account_generation)
        await _assert_no_split_delivery(db, subscription_id)
    except AccountingConflict as exc:
        raise LedgerConflict(str(exc)) from exc
    state = await db.scalar(
        select(TrafficAccountingState)
        .where(TrafficAccountingState.subscription_id == subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope).where(
            BsTrafficPanelBinding.panel_scope == ledger.panel_scope, BsTrafficPanelBinding.retired_at.is_(None)
        )
    )
    if (
        subscription.tariff_id is None
        or subscription.traffic_accounting_mode != 'managed'
        or grace_open
        or binding is None
        or state is None
        or state.phase not in ('ready', 'unlimited')
        or state.grace_excluded
        or state.activation_requested
        or state.pending_sync_fields
        or state.projected_revision != state.revision
    ):
        raise LedgerConflict('Managed accounting is not ready for transfer inspection')
    for model in (BsTrafficAllowance, BsTrafficUsage):
        if (
            await db.scalar(
                select(model.id)
                .where(model.subscription_id == subscription_id, model.ownership_cycle == ledger.ownership_cycle)
                .limit(1)
            )
            is not None
        ):
            raise LedgerConflict('Destination ledger must be empty before initial transfer')
    if (
        await db.scalar(
            select(TrafficBaseGrantIntent.id)
            .where(TrafficBaseGrantIntent.subscription_id == subscription_id, TrafficBaseGrantIntent.state == 'pending')
            .limit(1)
        )
        is not None
    ):
        raise LedgerConflict('A paid base grant is still pending')
    if (
        await db.scalar(
            select(PaidTrafficResetIntent.id)
            .where(
                PaidTrafficResetIntent.subscription_id == subscription_id,
                or_(
                    PaidTrafficResetIntent.state.in_(('pending', 'attempted', 'blocked')),
                    PaidTrafficResetIntent.activation_pending.is_(True),
                ),
            )
            .limit(1)
        )
        is not None
    ):
        raise LedgerConflict('A paid traffic reset is still pending')
    now = await db.scalar(select(func.clock_timestamp()))
    rows = (
        await db.scalars(
            select(TrafficAllowance)
            .where(
                TrafficAllowance.subscription_id == subscription_id,
                TrafficAllowance.remaining_bytes > 0,
                TrafficAllowance.retired_at.is_(None),
                TrafficAllowance.expires_at > now,
            )
            .order_by(TrafficAllowance.operation_key)
            .limit(TRANSFER_GRANT_LIMIT + 1)
            .execution_options(populate_existing=True)
        )
    ).all()
    if len(rows) > TRANSFER_GRANT_LIMIT:
        raise LedgerConflict('Transfer exceeds the grant inspection budget')
    _amount(state.lifetime_bytes, zero=True)
    _amount(state.used_bytes, zero=True)
    if state.used_bytes > state.lifetime_bytes or not state.panel_identity:
        raise LedgerConflict('Invalid source accounting identity or counters')
    grants = []
    for row in rows:
        _key(row.operation_key)
        _amount(row.granted_bytes)
        _amount(row.remaining_bytes)
        if row.remaining_bytes > row.granted_bytes:
            raise LedgerConflict('Source remainder exceeds original entitlement')
        key = hashlib.sha256(json.dumps([state.panel_identity, row.operation_key]).encode()).hexdigest()
        grants.append(
            TransferGrant(
                row.operation_key, f'managed:{key}', row.granted_bytes, row.remaining_bytes, _aware(row.expires_at)
            )
        )
    total = sum(item.remaining_bytes for item in grants)
    _amount(total, zero=True)
    values = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        tariff_id=subscription.tariff_id,
        subscription_status=subscription.status,
        paid_until=_aware(subscription.end_date) if subscription.end_date else None,
        panel_scope=ledger.panel_scope,
        panel_user_id=ledger.panel_user_id,
        account_generation=account_generation,
        ledger_revision=ledger.revision,
        source_identity=state.panel_identity,
        source_revision=state.revision,
        source_phase=state.phase,
        lifetime_bytes=state.lifetime_bytes,
        used_bytes=state.used_bytes,
        business_strategy=state.business_strategy,
        calendar_timezone=state.calendar_timezone,
        calendar_anchor=_aware(state.calendar_anchor),
        next_refresh_at=_aware(state.next_refresh_at) if state.next_refresh_at else None,
        destination_group=destination_group,
        grants=tuple(grants),
        total_bytes=total,
    )
    raw = dict(values, grants=[asdict(item) for item in grants])
    fingerprint = hashlib.sha256(
        json.dumps(raw, sort_keys=True, default=lambda value: value.isoformat(), separators=(',', ':')).encode()
    ).hexdigest()
    return ManagedTransferManifest(**values, fingerprint=fingerprint)
