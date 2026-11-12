"""Atomic, idempotent recurring daily charge for an activated split account."""

import hashlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm.attributes import set_committed_value

from app.database.crud.user import lock_user_for_pricing
from app.database.models import (
    BsTrafficRenewalIntent,
    PaymentMethod,
    Subscription,
    Tariff,
    Transaction,
    TransactionType,
)
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner
from app.services.bs_traffic.policy import _read_policy, capture_bs_drain_before_extension
from app.services.bs_traffic.renewal import RenewalBalanceError, RenewalResult, _commit_paid_renewal
from app.services.bs_traffic.renewal_grace import require_no_unresolved_grace_restore
from app.services.bs_traffic.tariff_settings import BsTrafficSettings, require_matching_traffic_policy
from app.services.cispay_recurrent_guard import require_no_cispay
from app.services.grace_access_echo import undo_grace_overlay_echo
from app.services.pricing_engine import PricingEngine
from app.services.traffic_reset_policy import should_reset_traffic_on_daily_charge


class DailyBalanceError(RenewalBalanceError):
    def __init__(self, price_kopeks):
        super().__init__('Insufficient balance for recurring daily charge')
        self.price_kopeks = price_kopeks


async def renew_daily_from_balance(db, *, user_id, subscription_id, account_generation, expected_due_at):
    """Commit nothing; return receipt/replay, or None when this cycle is not due.

    expected_due_at is the server's observed subscription end, not client input.
    An old observed cycle remains replayable after restart and concurrent workers
    cannot convert it into a second daily debit. Prices and reset policy are read
    under locks; daily payments never consume the one-off promotional offer.
    """
    if (
        not isinstance(expected_due_at, datetime)
        or expected_due_at.tzinfo is None
        or expected_due_at.utcoffset() is None
    ):
        raise LedgerConflict('Aware observed daily due date required')
    due = expected_due_at.astimezone(UTC).isoformat()
    key = 'split-daily:' + hashlib.sha256(f'{user_id}:{subscription_id}:{due}'.encode()).hexdigest()
    async with db.begin_nested():
        state, _ = await _lock(db, user_id, subscription_id, account_generation)
        await require_activated_owner(db, subscription_id)
        receipt = await db.scalar(
            select(BsTrafficRenewalIntent).where(
                BsTrafficRenewalIntent.subscription_id == subscription_id, BsTrafficRenewalIntent.operation_key == key
            )
        )
        if receipt is not None:
            payment = await db.get(Transaction, receipt.transaction_id)
            total = receipt.parameters.get('total_amount_kopeks')
            if (
                receipt.parameters.get('kind') != 'daily'
                or receipt.parameters.get('expected_due_at') != due
                or receipt.account_generation != account_generation
                or receipt.charged_kopeks != total
                or type(total) is not int
                or payment is None
                or payment.user_id != user_id
                or payment.external_id != key
                or payment.payment_method != PaymentMethod.BALANCE.value
                or payment.amount_kopeks != -total
                or not payment.is_completed
                or payment.type != TransactionType.SUBSCRIPTION_PAYMENT.value
            ):
                raise LedgerConflict('Daily payment receipt does not match this cycle')
            return RenewalResult(
                receipt.id,
                payment.id,
                receipt.parameters['grants'],
                datetime.fromisoformat(receipt.parameters['old_end_date']),
                receipt.intended_expires_at,
                receipt.charged_kopeks,
                True,
                receipt.state not in ('verified', 'superseded'),
            )
        user = await lock_user_for_pricing(db, user_id)
        sub = await db.get(Subscription, subscription_id, populate_existing=True)
        if (
            user.status != 'active'
            or user.restriction_subscription
            or sub.is_trial
            or sub.is_daily_paused
            or sub.status not in ('active', 'expired', 'limited')
            or sub.end_date != expected_due_at
        ):
            return None
        await require_no_unresolved_grace_restore(db, subscription_id)
        if await db.scalar(
            select(BsTrafficRenewalIntent.id)
            .where(
                BsTrafficRenewalIntent.subscription_id == subscription_id, BsTrafficRenewalIntent.state == 'attempted'
            )
            .limit(1)
        ):
            raise LedgerConflict('Previous paid renewal must be reconciled')
        await require_no_cispay(db, user_id=user_id, subscription_id=subscription_id)
        await undo_grace_overlay_echo(db, sub)
        await db.flush()
        now = await db.scalar(select(func.clock_timestamp()))
        if (
            sub.end_date is None
            or sub.end_date > now
            or (sub.last_daily_charge_at is not None and sub.last_daily_charge_at > now - timedelta(days=1))
        ):
            return None
        await capture_bs_drain_before_extension(db, sub)
        tariff = await db.scalar(
            select(Tariff)
            .where(Tariff.id == sub.tariff_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if tariff is None or not tariff.is_daily or not tariff.is_active or tariff.daily_price_kopeks <= 0:
            raise LedgerConflict('Active daily tariff with positive base price required')
        policy = _read_policy(state.access_policy)
        try:
            require_matching_traffic_policy(tariff, policy)
            bs = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
        except ValueError as error:
            raise LedgerConflict('Daily traffic settings changed') from error
        set_committed_value(sub, 'tariff', tariff)
        price = PricingEngine.daily_subscription_price(sub, tariff, user)
        if type(price) is not int or not 0 <= price <= 2_147_483_647:
            raise LedgerConflict('Invalid recurring daily price')
        if user.balance_kopeks < price:
            raise DailyBalanceError(price)
        old_end = sub.end_date
        new_end = max(old_end, now + timedelta(days=1))
        return await _commit_paid_renewal(
            db,
            state=state,
            user=user,
            sub=sub,
            tariff=tariff,
            policy=policy,
            bs=bs,
            account_generation=account_generation,
            key=key,
            total=price,
            charge=price,
            payment_method=PaymentMethod.BALANCE,
            now=now,
            old_end=old_end,
            new_end=new_end,
            period_days=1,
            reset=should_reset_traffic_on_daily_charge(tariff),
            consume_promo_offer=False,
            description=f'Суточная оплата тарифа «{tariff.name}»',
            daily_charge_at=now,
            extra_parameters={'kind': 'daily', 'expected_due_at': due},
        )
