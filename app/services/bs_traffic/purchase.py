"""Atomic balance checkout for activated split accounts; no network or commit.

The caller obtains fresh topology/current squad evidence before entering this
transaction, commits the result, then delivers the persisted operation through
panel_sync. An unavailable/ambiguous panel is never reason to charge twice.
Public entrypoints must not expose checkout before the delivery worker is wired.
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, or_, select

from app.config import settings
from app.database.crud.transaction import create_transaction
from app.database.crud.user import lock_user_for_pricing, subtract_user_balance
from app.database.models import (
    BsTrafficAllowance as Allowance,
    BsTrafficDelivery as Delivery,
    GraceAccessSessionModel,
    PaymentMethod,
    Subscription,
    Tariff,
    Transaction,
    TransactionType,
)
from app.services.bs_traffic.base_extension import covers_interval
from app.services.bs_traffic.delivery_plan import plan_squads
from app.services.bs_traffic.delivery_store import enqueue
from app.services.bs_traffic.intake import grant_with_reported_settlement
from app.services.bs_traffic.ledger import LedgerConflict, _group, _key, require_activated_owner
from app.services.bs_traffic.policy import _read_policy, project_access
from app.services.bs_traffic.preparation import delivery_guard
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.tariff_settings import require_matching_traffic_policy, traffic_package_price
from app.services.pricing_engine import PricingEngine


@dataclass(frozen=True)
class PurchaseResult:
    transaction_id: int
    grant_id: int
    delivery_id: int
    price_kopeks: int
    replayed: bool = False


@dataclass(frozen=True)
class PackageQuote:
    subscription_id: int
    group: str
    gb: int
    period_days: int
    base_price_kopeks: int
    price_kopeks: int
    discount_percent: int
    balance_kopeks: int
    missing_kopeks: int
    valid_until: datetime


async def _quote_locked(db, *, state, target, group, gb, evidence, current_squads):
    user_id, subscription_id = target['user_id'], target['subscription_id']
    if not settings.is_tariffs_mode():
        raise LedgerConflict('Split purchases require tariff mode')
    user = await lock_user_for_pricing(db, user_id)
    sub = await db.scalar(
        select(Subscription).where(Subscription.id == subscription_id).execution_options(populate_existing=True)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        user.status != 'active'
        or user.restriction_subscription
        or sub.is_trial
        or sub.status not in ('active', 'limited')
        or sub.end_date is None
        or sub.end_date <= now
    ):
        raise LedgerConflict('Subscription is not eligible for a paid traffic package')
    grace = sub.grace_session_open or await db.scalar(
        select(GraceAccessSessionModel.id)
        .where(
            GraceAccessSessionModel.subscription_id == subscription_id,
            GraceAccessSessionModel.state != 'completed',
        )
        .limit(1)
    )
    if grace:
        raise LedgerConflict('Temporary access must be completed before purchase')
    policy = _read_policy(state.access_policy)
    if group == 'bs' and policy.get('bs_purchase_enabled', True) is False:
        raise LedgerConflict('BS purchases are stopped while paid packages remain available')
    if policy['tariff_id'] != sub.tariff_id or policy[f'{group}_mode'] != 'finite':
        raise LedgerConflict('Traffic policy no longer matches the purchase')
    tariff = await db.scalar(
        select(Tariff)
        .where(Tariff.id == sub.tariff_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    try:
        require_matching_traffic_policy(tariff, policy)
        ceiling = await db.scalar(
            select(func.coalesce(func.sum(Allowance.granted_bytes), 0)).where(
                Allowance.subscription_id == subscription_id,
                Allowance.traffic_group == group,
                covers_interval(Allowance, now, now, instant=True),
                or_(Allowance.retired_at.is_(None), Allowance.retired_at > now),
            )
        )
        base_price = traffic_package_price(tariff, group=group, gb=gb, current_limit_bytes=int(ceiling))
    except (ValueError, AttributeError) as error:
        raise LedgerConflict(str(error)) from error
    price, _, percent = PricingEngine.calculate_traffic_discount(base_price, user, 30)
    if percent < 100 and price > 0:
        price = max(100, price)
    projection = await project_access(db, **target, evidence=evidence, grace_active=False)
    if getattr(projection, group).state == 'pending' or any(
        item.state not in ('active', 'exhausted', 'disabled', 'pending')
        for item in (projection.ordinary, projection.bs)
    ):
        raise LedgerConflict('Traffic accounting or topology requires reconciliation')
    plan = plan_squads(projection, topology=evidence.topology, current_squads=current_squads)
    if plan.action == 'wait':
        raise LedgerConflict('Current squad selection requires reconciliation')
    quote = PackageQuote(
        subscription_id=subscription_id,
        group=group,
        gb=gb,
        period_days=30,
        base_price_kopeks=base_price,
        price_kopeks=price,
        discount_percent=percent,
        balance_kopeks=user.balance_kopeks,
        missing_kopeks=max(0, price - user.balance_kopeks),
        valid_until=min(evidence.valid_until, sub.end_date),
    )
    return quote, user, now


async def quote_package(db, *, user_id, subscription_id, account_generation, group, gb, evidence, current_squads):
    """Prepare confirmation terms without debit, grant, enqueue, HTTP or commit.

    Uses exactly the same locked rules as purchase. The quote is informational,
    not a reservation or a trusted payment token: checkout always recalculates.
    Callers must end this transaction before returning a screen to a user.
    """
    _group(group)
    if type(gb) is not int or not 1 <= gb <= 100_000:
        raise LedgerConflict('Invalid traffic package')
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with db.begin_nested():
        state, _ = await _recovery_lock(db, target['user_id'], target['subscription_id'], target['account_generation'])
        await require_activated_owner(db, subscription_id)
        quote, _, _ = await _quote_locked(
            db,
            state=state,
            target=target,
            group=group,
            gb=gb,
            evidence=evidence,
            current_squads=current_squads,
        )
        return quote


async def _receipt_locked(db, *, user_id, subscription_id, group, gb, expected_price_kopeks, key):
    transaction = await db.scalar(
        select(Transaction).where(
            Transaction.external_id == key,
            Transaction.payment_method == PaymentMethod.BALANCE.value,
        )
    )
    old_grant = await db.scalar(
        select(Allowance).where(
            Allowance.subscription_id == subscription_id,
            Allowance.operation_key == key,
        )
    )
    old_delivery = await db.scalar(
        select(Delivery).where(
            Delivery.subscription_id == subscription_id,
            Delivery.operation_key == key,
        )
    )
    if transaction is not None or old_grant is not None or old_delivery is not None:
        if (
            transaction is None
            or old_grant is None
            or old_delivery is None
            or transaction.user_id != user_id
            or not transaction.is_completed
            or transaction.type != TransactionType.SUBSCRIPTION_PAYMENT.value
            or transaction.amount_kopeks != -expected_price_kopeks
            or old_grant.traffic_group != group
            or old_grant.granted_bytes != gb * 1024**3
        ):
            raise LedgerConflict('Purchase replay does not match its original terms')
        return PurchaseResult(transaction.id, old_grant.id, old_delivery.id, expected_price_kopeks, True)

    return None


async def find_purchase_receipt(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    group,
    gb,
    expected_price_kopeks,
    operation_key,
):
    """Validate an existing paid receipt without requiring a reachable panel."""
    _group(group)
    _key(operation_key)
    if type(gb) is not int or not 1 <= gb <= 100_000:
        raise LedgerConflict('Invalid traffic package')
    if type(expected_price_kopeks) is not int or not 0 <= expected_price_kopeks <= 2_147_483_647:
        raise LedgerConflict('Invalid expected price')
    await _recovery_lock(db, user_id, subscription_id, account_generation)
    await require_activated_owner(db, subscription_id)
    key = 'split-purchase:' + hashlib.sha256(f'{user_id}:{subscription_id}:{operation_key}'.encode()).hexdigest()
    return await _receipt_locked(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        group=group,
        gb=gb,
        expected_price_kopeks=expected_price_kopeks,
        key=key,
    )


async def purchase_package(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    group,
    gb,
    expected_price_kopeks,
    operation_key,
    evidence,
    current_squads,
):
    """Debit, settle known usage, grant 30 days and queue access atomically.

    Same-key replay validates the original group, size and paid price. It does
    not reprice, regrant or extend expiry. A new purchase always recalculates
    price from current server settings under the owner's/subscription's locks.
    Caller owns outer commit and post-commit transaction notifications.
    """
    _group(group)
    _key(operation_key)
    if type(gb) is not int or not 1 <= gb <= 100_000:
        raise LedgerConflict('Invalid traffic package')
    if type(expected_price_kopeks) is not int or not 0 <= expected_price_kopeks <= 2_147_483_647:
        raise LedgerConflict('Invalid expected price')
    key = 'split-purchase:' + hashlib.sha256(f'{user_id}:{subscription_id}:{operation_key}'.encode()).hexdigest()
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with db.begin_nested():
        state, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
        await require_activated_owner(db, subscription_id)
        previous = await _receipt_locked(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            group=group,
            gb=gb,
            expected_price_kopeks=expected_price_kopeks,
            key=key,
        )
        if previous is not None:
            return previous

        quote, user, now = await _quote_locked(
            db,
            state=state,
            target=target,
            group=group,
            gb=gb,
            evidence=evidence,
            current_squads=current_squads,
        )
        price = quote.price_kopeks
        if price != expected_price_kopeks:
            raise LedgerConflict('Traffic package price changed; confirm a new quote')
        description = f'Докупка {gb} ГБ ' + ('БС-трафика' if group == 'bs' else 'обычного трафика')
        if not await subtract_user_balance(db, user, price, description, commit=False):
            raise LedgerConflict('Insufficient balance')
        grant_id = await grant_with_reported_settlement(
            db,
            **target,
            group=group,
            operation_key=key,
            amount_bytes=gb * 1024**3,
            starts_at=now,
            expires_at=now + timedelta(days=30),
        )
        projection = await project_access(db, **target, evidence=evidence, grace_active=False)
        if getattr(projection, group).state != 'active':
            raise LedgerConflict('Purchased traffic cannot be delivered')
        plan = plan_squads(projection, topology=evidence.topology, current_squads=current_squads)
        if plan.action == 'wait':
            raise LedgerConflict('Current squad selection requires reconciliation')
        delivery_id = await enqueue(
            db,
            **target,
            operation_key=key,
            revision=projection.revision,
            guard_hash=delivery_guard(**target, evidence=evidence),
            squads=plan.squads,
            valid_until=evidence.valid_until,
        )
        transaction = await create_transaction(
            db,
            user_id,
            TransactionType.SUBSCRIPTION_PAYMENT,
            price,
            description,
            payment_method=PaymentMethod.BALANCE,
            external_id=key,
            commit=False,
        )
        return PurchaseResult(transaction.id, grant_id, delivery_id, price)
