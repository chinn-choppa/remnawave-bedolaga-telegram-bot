"""Fulfil an already authenticated recurring charge inside its callback transaction.

Provider adapters own signature, agreement, amount/currency and charge dedupe
checks. This module performs no provider request and never debits the wallet.
"""

import hashlib
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm.attributes import set_committed_value

from app.config import settings
from app.database.models import (
    BsTrafficLedger,
    BsTrafficRenewalIntent,
    PaymentMethod,
    Subscription,
    Tariff,
    Transaction,
    TransactionType,
    User,
)
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner
from app.services.bs_traffic.policy import _read_policy, capture_bs_drain_before_extension
from app.services.bs_traffic.renewal import RenewalResult, _commit_paid_renewal
from app.services.bs_traffic.renewal_grace import require_no_unresolved_grace_restore
from app.services.bs_traffic.tariff_settings import BsTrafficSettings, require_matching_traffic_policy
from app.services.grace_access_echo import undo_grace_overlay_echo


async def fulfill_recurrent_renewal(
    db,
    *,
    user_id,
    subscription_id,
    period_days,
    amount_kopeks,
    payment_method,
    charge_id,
):
    """No commit/network; callback must commit its provider record atomically."""
    if (
        payment_method not in (PaymentMethod.LAVA, PaymentMethod.PLATEGA, PaymentMethod.CASHERA, PaymentMethod.CISPAY)
        or not isinstance(payment_method, PaymentMethod)
        or type(period_days) is not int
        or period_days <= 0
        or type(amount_kopeks) is not int
        or not 0 <= amount_kopeks <= 2_147_483_647
        or not isinstance(charge_id, str)
        or not 1 <= len(charge_id) <= 255
        or charge_id.strip() != charge_id
    ):
        raise LedgerConflict('Invalid confirmed recurring charge')
    key = 'split-provider:' + hashlib.sha256(f'{payment_method.value}:{charge_id}'.encode()).hexdigest()
    async with db.begin_nested():
        generation = await db.scalar(
            select(BsTrafficLedger.account_generation).where(
                BsTrafficLedger.subscription_id == subscription_id,
            )
        )
        state, _ = await _lock(db, user_id, subscription_id, generation)
        await require_activated_owner(db, subscription_id)
        receipt = await db.scalar(
            select(BsTrafficRenewalIntent).where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.operation_key == key,
            )
        )
        transaction = await db.scalar(
            select(Transaction).where(
                Transaction.payment_method == payment_method.value,
                Transaction.external_id == charge_id,
            )
        )
        if receipt is not None:
            if (
                transaction is None
                or transaction.id != receipt.transaction_id
                or transaction.user_id != user_id
                or transaction.amount_kopeks != -amount_kopeks
                or transaction.type != TransactionType.SUBSCRIPTION_PAYMENT.value
                or not transaction.is_completed
                or receipt.account_generation != generation
                or receipt.charged_kopeks != 0
                or receipt.parameters.get('period_days') != period_days
                or receipt.parameters.get('total_amount_kopeks') != amount_kopeks
            ):
                raise LedgerConflict('Recurring receipt does not match charge')
            return RenewalResult(
                receipt.id,
                transaction.id,
                receipt.parameters['grants'],
                datetime.fromisoformat(receipt.parameters['old_end_date']),
                receipt.intended_expires_at,
                0,
                True,
                receipt.state not in ('verified', 'superseded'),
            )
        if transaction is not None:
            raise LedgerConflict('Recurring charge already belongs to another operation')
        await require_no_unresolved_grace_restore(db, subscription_id)
        if await db.scalar(
            select(BsTrafficRenewalIntent.id)
            .where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.state == 'attempted',
            )
            .limit(1)
        ):
            raise LedgerConflict('Previous paid delivery must be reconciled')
        user = await db.get(User, user_id, populate_existing=True)
        sub = await db.get(Subscription, subscription_id, populate_existing=True)
        if (
            user.status != 'active'
            or user.restriction_subscription
            or sub.status not in ('active', 'trial', 'limited', 'expired')
        ):
            raise LedgerConflict('Subscription cannot accept recurring renewal')
        await undo_grace_overlay_echo(db, sub)
        await db.flush()
        await capture_bs_drain_before_extension(db, sub)
        tariff = await db.scalar(
            select(Tariff)
            .where(Tariff.id == sub.tariff_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if tariff is None or not tariff.is_active:
            raise LedgerConflict('Recurring tariff is unavailable')
        if tariff.is_daily and sub.is_daily_paused:
            raise LedgerConflict('Recurring daily target changed')
        policy = _read_policy(state.access_policy)
        try:
            require_matching_traffic_policy(tariff, policy)
            bs = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
        except ValueError as error:
            raise LedgerConflict('Recurring tariff traffic policy changed') from error
        set_committed_value(sub, 'tariff', tariff)
        now = await db.scalar(select(func.clock_timestamp()))
        old_end = sub.end_date
        if old_end is None:
            raise LedgerConflict('Paid expiry is missing')
        base = max(old_end, now) if not sub.is_trial or settings.TRIAL_ADD_REMAINING_DAYS_TO_PAID else now
        try:
            new_end = base + timedelta(days=period_days)
        except OverflowError:
            raise LedgerConflict('Unsupported recurring period') from None
        from app.services.traffic_reset_policy import should_reset_traffic_on_daily_charge

        reset = should_reset_traffic_on_daily_charge(tariff) if tariff.is_daily else settings.RESET_TRAFFIC_ON_PAYMENT
        return await _commit_paid_renewal(
            db,
            state=state,
            user=user,
            sub=sub,
            tariff=tariff,
            policy=policy,
            bs=bs,
            account_generation=generation,
            key=key,
            total=amount_kopeks,
            charge=0,
            payment_method=payment_method,
            transaction_external_id=charge_id,
            now=now,
            old_end=old_end,
            new_end=new_end,
            period_days=period_days,
            reset=bool(reset or sub.is_trial),
            consume_promo_offer=False,
            description=f'Автопродление {payment_method.value}',
            extra_parameters={'kind': 'provider_daily' if tariff.is_daily else 'provider'},
            daily_charge_at=now if tariff.is_daily else None,
        )


async def lock_recurrent_target(db, record):
    """Acquire financial locks before locking the provider agreement itself.

    Callers read the agreement only to locate these immutable foreign keys, then
    reload it FOR UPDATE. This order also covers provider history replay.
    """
    await db.scalar(
        select(User).where(User.id == record.user_id).with_for_update().execution_options(populate_existing=True)
    )
    await db.scalar(
        select(Subscription)
        .where(Subscription.id == record.subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


async def fulfill_or_credit_recurrent(db, *, record, subscription, payment_method, charge_id):
    """Handle split ownership or preserve an already collected payment in wallet.

    The adapter holds User -> Subscription -> agreement locks. A policy conflict
    rolls back only the attempted entitlement change. The positive deposit and
    provider dedupe metadata are committed together by the caller, as in CisPay.
    """
    from app.database.crud.transaction import create_transaction

    if (
        record.currency != 'RUB'
        or type(record.amount_kopeks) is not int
        or not 0 < record.amount_kopeks <= 2_147_483_647
        or not isinstance(charge_id, str)
        or not 1 <= len(charge_id) <= 255
        or charge_id.strip() != charge_id
    ):
        raise ValueError('Invalid confirmed recurring payment')
    try:
        if subscription.user_id != record.user_id or subscription.traffic_accounting_mode != 'split':
            raise LedgerConflict('Recurring target ownership or accounting changed')
        renewal = await fulfill_recurrent_renewal(
            db,
            user_id=record.user_id,
            subscription_id=subscription.id,
            period_days=record.charge_days,
            amount_kopeks=record.amount_kopeks,
            payment_method=payment_method,
            charge_id=charge_id,
        )
    except LedgerConflict:
        # Read only after the failed savepoint was rolled back. Never translate
        # unexpected database/programming errors into a successful payment.
        await db.refresh(subscription)
        user = await db.get(User, record.user_id, populate_existing=True)
        if user is None or user.status == 'deleted':
            raise LedgerConflict('Recurring payer is unavailable') from None
        duplicate = await db.scalar(
            select(Transaction.id).where(
                Transaction.payment_method == payment_method.value,
                Transaction.external_id == charge_id,
            )
        )
        if duplicate is not None:
            raise LedgerConflict('Recurring charge already recorded') from None
        user.balance_kopeks += record.amount_kopeks
        tx = await create_transaction(
            db,
            user_id=user.id,
            type=TransactionType.DEPOSIT,
            amount_kopeks=record.amount_kopeks,
            description=f'{payment_method.value}: оплата по изменённой подписке',
            payment_method=payment_method,
            external_id=charge_id,
            commit=False,
        )
        record.status = 'CANCELLED'
        return None, tx
    return renewal, await db.get(Transaction, renewal.transaction_id)


async def deliver_recurrent_renewal(db, renewal=None, *, intent_id=None):
    """Post-commit best effort; the durable receipt is the sole delivery queue."""
    import structlog

    from app.services.bs_traffic.renewal import deliver_renewal

    if intent_id is None:
        intent_id = renewal.intent_id if renewal is not None else None
    if intent_id is None:
        return False
    try:
        return await deliver_renewal(db, intent_id)
    except Exception:
        structlog.get_logger(__name__).warning('Recurring split delivery remains pending', intent_id=intent_id)
        return False


async def notify_recurrent_credit(db, *, user_id, amount_kopeks, bot=None):
    """Use the existing localized deposit template, never a renewal success."""
    import structlog

    from app.services.notification_delivery_service import notification_delivery_service

    try:
        user = await db.get(User, user_id)
        await notification_delivery_service.notify_balance_topup(
            user,
            amount_kopeks,
            user.balance_kopeks,
            bot=bot,
        )
    except Exception:
        structlog.get_logger(__name__).warning('Recurring credit notification unavailable', user_id=user_id)
