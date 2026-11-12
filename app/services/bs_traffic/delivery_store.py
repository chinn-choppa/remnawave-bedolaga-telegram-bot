"""Inactive durable split write attempts; caller owns transactions and commit.

A claim MUST be committed before any HTTP. Never retry a claimed request after
crash/timeout, even after its evidence expires. Only verified readback may close
it. guard_hash is a caller-produced digest of current ownership, tariff, expiry,
admin/grace and topology under their shared lease; this store cannot establish
that evidence or close the panel's missing remote-CAS race. No runtime writer
calls this store yet. Readback identity and freshness are caller prerequisites.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficDelivery as Delivery,
    BsTrafficLedger,
    GraceAccessSessionModel as Grace,
    Subscription,
    Tariff,
    TrafficAccountingState,
    User,
)
from app.services.bs_traffic.delivery_plan import _selection, plan_squads
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.ledger import LedgerConflict, _key, _lock, require_activated_owner
from app.services.bs_traffic.policy import _fingerprint, _read_policy, project_access


SEND_WINDOW_SECONDS = 15


@dataclass(frozen=True)
class DeliveryAttempt:
    operation_id: int
    token: str
    panel_user_id: int
    squads: tuple[str, ...]
    send_deadline: datetime


async def _operation(db, subscription_id, operation_id):
    if type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Invalid delivery identity')
    row = await db.scalar(
        select(Delivery)
        .where(Delivery.id == operation_id, Delivery.subscription_id == subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise LedgerConflict('Delivery does not belong to this subscription')
    return row


async def _local_guard(db, user_id, subscription_id, now):
    # Lock order is User -> Subscription -> Ledger. Grace lease coordination is
    # still required around delivery; this read also catches unversioned edits.
    row = (
        await db.execute(
            select(
                BsTrafficLedger.access_policy,
                BsTrafficLedger.account_generation,
                BsTrafficLedger.panel_scope,
                BsTrafficLedger.panel_user_id,
                User.id,
                User.status,
                Subscription.status,
                Subscription.tariff_id,
                Subscription.end_date,
                Subscription.is_trial,
                Subscription.connected_squads,
                Subscription.traffic_accounting_mode,
                Tariff.traffic_limit_gb,
                Tariff.allowed_squads,
                Tariff.bs_traffic_settings,
            )
            .select_from(User)
            .join(Subscription, Subscription.user_id == User.id)
            .join(BsTrafficLedger, BsTrafficLedger.subscription_id == Subscription.id)
            .outerjoin(Tariff, Tariff.id == Subscription.tariff_id)
            .where(User.id == user_id, Subscription.id == subscription_id)
        )
    ).one()
    # A ledger or mode string is not enrollment. A transferred managed source
    # requires the durable activation receipt; legacy dry-run behavior remains.
    managed_state = await db.scalar(
        select(TrafficAccountingState.subscription_id).where(TrafficAccountingState.subscription_id == subscription_id)
    )
    if row.traffic_accounting_mode == 'split':
        await require_activated_owner(db, subscription_id)
        # Existing grants belong to the frozen subscription policy. Editing
        # future tariff offers cannot revoke an already paid delivery.
        if row.tariff_id != _read_policy(row.access_policy)['tariff_id']:
            raise LedgerConflict('Subscription tariff changed; reconcile paid entitlements')
    elif row.traffic_accounting_mode != 'legacy' or managed_state is not None:
        raise LedgerConflict('Split writes require a completed accounting ownership transfer')
    grace = (
        await db.execute(
            select(Grace.id, Grace.version, Grace.state, Grace.grace_until)
            .where(Grace.subscription_id == subscription_id, Grace.state != 'completed')
            .order_by(Grace.id)
        )
    ).all()
    local = list(row)
    # Prices may change after payment without revoking the purchased access.
    # Access-affecting tariff edits must invalidate an old delivery plan.
    bs = row.bs_traffic_settings
    local[-1] = {key: bs.get(key) for key in ('limit_gb', 'squad_uuids')} if isinstance(bs, dict) else bs
    value = [local, row.end_date is None or row.end_date <= now, [list(item) for item in grace]]
    return hashlib.sha256(json.dumps(value, default=lambda item: item.isoformat(), sort_keys=True).encode()).hexdigest()


async def enqueue(
    db, *, user_id, subscription_id, account_generation, operation_key, revision, guard_hash, squads, valid_until
):
    _key(operation_key)
    _selection(squads)
    if type(revision) is not int or revision < 0 or not _fingerprint(guard_hash):
        raise LedgerConflict('Explicit delivery revision and guard required')
    if not isinstance(valid_until, datetime) or valid_until.tzinfo is None or valid_until.utcoffset() is None:
        raise LedgerConflict('Aware evidence deadline required')
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    old = await db.scalar(
        select(Delivery).where(Delivery.subscription_id == subscription_id, Delivery.operation_key == operation_key)
    )
    if old is not None:
        if (old.revision, old.guard_hash, old.squads) != (revision, guard_hash, list(squads)):
            raise LedgerConflict('Delivery replay changed its request')
        # A retry must never extend the original evidence deadline.
        return old.id
    now = await db.scalar(select(func.clock_timestamp()))
    if state.revision != revision or valid_until <= now:
        raise LedgerConflict('Delivery evidence is obsolete')
    unresolved = await db.scalar(
        select(Delivery.id)
        .where(
            Delivery.subscription_id == subscription_id,
            Delivery.state.in_(('pending', 'attempted', 'attention')),
        )
        .limit(1)
    )
    if unresolved is not None:
        raise LedgerConflict('Previous delivery needs reconciliation')
    row = Delivery(
        subscription_id=subscription_id,
        operation_key=operation_key,
        revision=revision,
        guard_hash=guard_hash,
        local_guard_hash=await _local_guard(db, user_id, subscription_id, now),
        squads=list(squads),
        valid_until=valid_until,
    )
    db.add(row)
    await db.flush()
    return row.id


async def claim(db, *, user_id, subscription_id, account_generation, operation_id, current_guard_hash):
    if not _fingerprint(current_guard_hash):
        raise LedgerConflict('Fresh delivery guard required')
    state, panel_id = await _lock(db, user_id, subscription_id, account_generation)
    row = await _operation(db, subscription_id, operation_id)
    if row.state != 'pending':
        return None
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        state.revision != row.revision
        or current_guard_hash != row.guard_hash
        or row.valid_until <= now
        or row.local_guard_hash != await _local_guard(db, user_id, subscription_id, now)
    ):
        row.state = 'cancelled'
        await db.flush()
        return None
    _selection(tuple(row.squads))
    row.state, row.token, row.attempted_at = 'attempted', str(uuid4()), now
    await db.flush()
    return DeliveryAttempt(row.id, row.token, panel_id, tuple(row.squads), now + timedelta(seconds=SEND_WINDOW_SECONDS))


async def reconcile(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    operation_id,
    token,
    current_guard_hash,
    observed_squads,
    topology_evidence=None,
):
    """Compare a fresh independently verified readback, never a PATCH response.

    A stale entitlement guard leaves attention even if squads match: matching
    old access is not proof that the customer's latest purchase was delivered.
    """
    _selection(observed_squads)
    if not _fingerprint(current_guard_hash):
        raise LedgerConflict('Fresh delivery guard required')
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    row = await _operation(db, subscription_id, operation_id)
    if not isinstance(token, str) or row.token != token or row.state not in ('attempted', 'attention', 'verified'):
        raise LedgerConflict('Delivery attempt does not match')
    if row.state == 'verified':
        return row.state
    now = await db.scalar(select(func.clock_timestamp()))
    matches = (
        row.local_guard_hash == await _local_guard(db, user_id, subscription_id, now)
        and row.revision == state.revision
        and row.guard_hash == current_guard_hash
        and row.squads == list(observed_squads)
    )
    if topology_evidence is None:
        # Legacy/direct reconciliation has no renewed proof of current rights.
        matches = matches and row.valid_until > now
    elif matches:
        matches = isinstance(topology_evidence, TopologyEvidence) and topology_evidence.is_fresh(
            now,
            panel_identity=state.panel_scope,
        )
        if matches:
            grace = await db.scalar(
                select(Grace.id)
                .where(
                    Grace.subscription_id == subscription_id,
                    Grace.state != 'completed',
                )
                .limit(1)
            )
            grace = bool(
                grace
                or await db.scalar(
                    select(Subscription.grace_session_open).where(
                        Subscription.id == subscription_id,
                    )
                )
            )
            projection = await project_access(
                db,
                user_id=user_id,
                subscription_id=subscription_id,
                account_generation=account_generation,
                evidence=topology_evidence,
                grace_active=grace,
            )
            matches = all(
                group.state in ('active', 'exhausted', 'disabled', 'pending')
                for group in (projection.ordinary, projection.bs)
            )
            if matches:
                plan = plan_squads(projection, topology=topology_evidence.topology, current_squads=observed_squads)
                matches = plan.action == 'unchanged' and plan.squads == tuple(row.squads)
        # Never extend the historical send deadline/token. Fresh GET + current
        # entitlements can confirm access after a crash without another PATCH.
    row.state = 'verified' if matches else 'attention'
    await db.flush()
    return row.state
