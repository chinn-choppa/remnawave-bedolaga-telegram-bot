"""Explicit free daily flags and separately confirmed, atomic paid resumption."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm.attributes import set_committed_value

from app.database.crud.user import lock_user_for_pricing
from app.database.models import (
    BsTrafficRenewalIntent,
    PaymentMethod,
    Subscription,
    Tariff,
    TrafficAccountingState,
    Transaction,
    TransactionType,
)
from app.services.bs_traffic.daily_funding import renewal_daily_funding
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner, summary
from app.services.bs_traffic.policy import _read_policy, capture_bs_drain_before_extension, effective_group_mode
from app.services.bs_traffic.renewal import _commit_paid_renewal
from app.services.bs_traffic.renewal_grace import require_no_unresolved_grace_restore
from app.services.bs_traffic.renewal_result import RenewalResult
from app.services.bs_traffic.tariff_settings import BsTrafficSettings, require_matching_traffic_policy
from app.services.cispay_recurrent_guard import require_no_cispay
from app.services.grace_access_echo import undo_grace_overlay_echo
from app.services.panel_sync.traffic_strategy import get_traffic_reset_strategy
from app.services.pricing_engine import PricingEngine
from app.services.traffic_reset_policy import should_reset_traffic_on_daily_charge


class DailyResumeConsentRequired(LedgerConflict):
    """A flag setter never authorizes a paid day."""


class DailyResumeQuoteChanged(LedgerConflict):
    """The confirmed financial/source terms changed before any debit."""


@dataclass(frozen=True)
class DailyPauseResult:
    is_paused: bool
    changed: bool


@dataclass(frozen=True)
class DailyResumeQuote:
    price_kopeks: int
    terms_hash: str
    old_end_date: datetime
    intended_end_date: datetime  # Estimate/minimum; actual day starts at payment.
    tariff_id: int
    tariff_name: str
    period_days: int = 1


def _digest(terms):
    return hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _identity(user_id, subscription_id, operation_key):
    if any(type(value) is not int or value <= 0 for value in (user_id, subscription_id)):
        raise LedgerConflict('Explicit owned daily subscription required')
    try:
        if not isinstance(operation_key, str) or str(UUID(operation_key)) != operation_key:
            raise ValueError
    except ValueError:
        raise LedgerConflict('Canonical daily resume UUID required') from None
    return 'split-daily-resume:' + hashlib.sha256(f'{user_id}:{subscription_id}:{operation_key}'.encode()).hexdigest()


async def _receipt(db, user_id, subscription_id, operation_key):
    key = _identity(user_id, subscription_id, operation_key)
    return await db.scalar(
        select(BsTrafficRenewalIntent)
        .join(Subscription)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            BsTrafficRenewalIntent.operation_key == key,
        )
    )


async def find_daily_resume(db, *, user_id, subscription_id, operation_key, expected_price_kopeks, expected_terms_hash):
    if (
        type(expected_price_kopeks) is not int
        or not 0 <= expected_price_kopeks <= 2_147_483_647
        or not isinstance(expected_terms_hash, str)
        or len(expected_terms_hash) != 64
    ):
        raise LedgerConflict('Confirmed daily resume price and terms required')
    row = await _receipt(db, user_id, subscription_id, operation_key)
    if row is None:
        return None
    payment = await db.get(Transaction, row.transaction_id)
    values = row.parameters
    terms = values.get('daily_resume_terms')
    if (
        values.get('kind') != 'daily_resume'
        or not isinstance(terms, dict)
        or terms.get('duration_days') != 1
        or values.get('period_days') != 1
        or terms.get('price_kopeks') != expected_price_kopeks
        or terms.get('old_end_date') != values.get('old_end_date')
        or terms.get('tariff_id') != values.get('tariff_id')
        or terms.get('account_generation') != row.account_generation
        or values.get('daily_resume_terms_hash') != expected_terms_hash
        or _digest(terms) != expected_terms_hash
        or row.charged_kopeks != expected_price_kopeks
        or values.get('total_amount_kopeks') != expected_price_kopeks
        or payment is None
        or payment.user_id != user_id
        or payment.external_id != row.operation_key
        or payment.amount_kopeks != -expected_price_kopeks
        or not payment.is_completed
        or payment.type != TransactionType.SUBSCRIPTION_PAYMENT.value
        or payment.payment_method != PaymentMethod.BALANCE.value
        or values.get('payment_receipt')
        != {
            'method': PaymentMethod.BALANCE.value,
            'external_id': row.operation_key,
            'amount_kopeks': -expected_price_kopeks,
        }
    ):
        raise LedgerConflict('Daily resume receipt does not match confirmed request')
    return RenewalResult(
        row.id,
        payment.id,
        values['grants'],
        datetime.fromisoformat(values['old_end_date']),
        row.intended_expires_at,
        row.charged_kopeks,
        True,
        row.state not in ('verified', 'superseded'),
    )


async def find_daily_resume_by_operation(db, *, user_id, subscription_id, operation_key):
    row = await _receipt(db, user_id, subscription_id, operation_key)
    if row is None:
        return None
    return await find_daily_resume(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        operation_key=operation_key,
        expected_price_kopeks=row.charged_kopeks,
        expected_terms_hash=row.parameters.get('daily_resume_terms_hash'),
    )


async def _context(db, *, user_id, subscription_id, account_generation):
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    await require_activated_owner(db, subscription_id)
    user = await lock_user_for_pricing(db, user_id)
    sub = await db.get(Subscription, subscription_id, populate_existing=True)
    if user.status != 'active' or user.restriction_subscription or sub.is_trial:
        raise LedgerConflict('Daily controls are unavailable for this owner')
    await require_no_cispay(db, user_id=user_id, subscription_id=subscription_id)
    await require_no_unresolved_grace_restore(db, subscription_id)
    if await db.scalar(
        select(BsTrafficRenewalIntent.id)
        .where(BsTrafficRenewalIntent.subscription_id == subscription_id, BsTrafficRenewalIntent.state == 'attempted')
        .limit(1)
    ):
        raise LedgerConflict('Previous daily delivery must be reconciled')
    await undo_grace_overlay_echo(db, sub)
    await db.flush()
    tariff = await db.scalar(
        select(Tariff)
        .where(Tariff.id == sub.tariff_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if (
        tariff is None
        or not tariff.is_daily
        or sub.end_date is None
        or sub.status not in ('active', 'limited', 'expired', 'disabled')
    ):
        raise LedgerConflict('Paid daily subscription required')
    policy = _read_policy(state.access_policy)
    try:
        require_matching_traffic_policy(tariff, policy)
        bs = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
    except ValueError as error:
        raise LedgerConflict('Daily traffic policy changed') from error
    set_committed_value(sub, 'tariff', tariff)
    now = await db.scalar(select(func.clock_timestamp()))
    return state, user, sub, tariff, policy, bs, now


async def _needs_paid_resume(db, target, sub, policy, now):
    if sub.end_date <= now or sub.status in ('expired', 'disabled'):
        return True
    if sub.status == 'limited':
        balances = await summary(db, **target, now=now)
        allowed = any(
            effective_group_mode(policy, group, now) == 'unlimited'
            or (
                effective_group_mode(policy, group, now) == 'finite'
                and not balances[group]['needs_review']
                and balances[group]['unencumbered_bytes'] > 0
            )
            for group in ('ordinary', 'bs')
        )
        if not allowed:
            raise LedgerConflict('Whole-account limited daily rights require explicit reconciliation')
    return False


async def set_daily_pause(db, *, user_id, subscription_id, account_generation, paused):
    """Caller commits; an explicit flag target can never turn into a payment."""
    if type(paused) is not bool:
        raise LedgerConflict('Explicit daily pause target required')
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with db.begin_nested():
        state, _, sub, _, policy, _, now = await _context(db, **target)
        if not paused and await _needs_paid_resume(db, target, sub, policy, now):
            raise DailyResumeConsentRequired('A separate confirmed daily payment is required')
        changed = bool(sub.is_daily_paused) != paused
        if changed:
            sub.is_daily_paused = paused
            state.revision += 1
            await db.flush()
        return DailyPauseResult(paused, changed)


async def _prepare_resume(db, target):
    state, user, sub, tariff, policy, bs, now = await _context(db, **target)
    if not await _needs_paid_resume(db, target, sub, policy, now):
        raise LedgerConflict('Current daily window is already paid; use free unpause')
    if not tariff.is_active or tariff.daily_price_kopeks <= 0:
        raise LedgerConflict('Active daily tariff with positive base price required')
    await capture_bs_drain_before_extension(db, sub)
    policy = _read_policy(state.access_policy)
    price = PricingEngine.daily_subscription_price(sub, tariff, user)
    if type(price) is not int or not 0 <= price <= 2_147_483_647:
        raise LedgerConflict('Invalid daily resume price')
    reset = should_reset_traffic_on_daily_charge(tariff)
    calendar = await db.get(TrafficAccountingState, sub.id)
    if calendar is None or calendar.calendar_anchor is None:
        raise LedgerConflict('Paid daily calendar is not initialized')
    intended_end = max(sub.end_date, now + timedelta(days=1))
    if intended_end == sub.end_date:
        raise LedgerConflict('Daily resume would not extend the already paid window')
    terms = dict(
        account_generation=target['account_generation'],
        ownership_cycle=state.ownership_cycle,
        tariff_id=tariff.id,
        price_kopeks=price,
        duration_days=1,
        old_end_date=sub.end_date.isoformat(),
        last_daily_charge_at=sub.last_daily_charge_at.isoformat() if sub.last_daily_charge_at else None,
        is_daily_paused=bool(sub.is_daily_paused),
        status=sub.status,
        device_limit=sub.device_limit,
        daily_devices_billing_version=sub.daily_devices_billing_version,
        daily_price_kopeks=tariff.daily_price_kopeks,
        policy=policy,
        bs=bs.model_dump(mode='json'),
        ordinary_limit_gb=tariff.traffic_limit_gb,
        reset_base=reset,
        calendar_strategy=get_traffic_reset_strategy(tariff).value,
        calendar_anchor=calendar.calendar_anchor.isoformat(),
        calendar_timezone=calendar.calendar_timezone,
    )
    quote = DailyResumeQuote(price, _digest(terms), sub.end_date, intended_end, tariff.id, tariff.name)
    return quote, terms, state, user, sub, tariff, policy, bs, now


async def quote_daily_resume(db, *, user_id, subscription_id, account_generation):
    return (
        await _prepare_resume(
            db, dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
        )
    )[0]


async def resume_daily_from_balance(
    db, *, user_id, subscription_id, account_generation, operation_key, expected_price_kopeks, expected_terms_hash
):
    request = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        operation_key=operation_key,
        expected_price_kopeks=expected_price_kopeks,
        expected_terms_hash=expected_terms_hash,
    )
    historical = await find_daily_resume(db, **request)
    if historical is not None:
        return historical
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with db.begin_nested():
        await _lock(db, **target)
        historical = await find_daily_resume(db, **request)
        if historical is not None:
            return historical
        quote, terms, state, user, sub, tariff, policy, bs, now = await _prepare_resume(db, target)
        if quote.price_kopeks != expected_price_kopeks or quote.terms_hash != expected_terms_hash:
            raise DailyResumeQuoteChanged('Daily resume price or paid state changed')
        return await _commit_paid_renewal(
            db,
            state=state,
            user=user,
            sub=sub,
            tariff=tariff,
            policy=policy,
            bs=bs,
            account_generation=account_generation,
            key=_identity(user_id, subscription_id, operation_key),
            total=quote.price_kopeks,
            charge=quote.price_kopeks,
            payment_method=PaymentMethod.BALANCE,
            now=now,
            old_end=quote.old_end_date,
            new_end=quote.intended_end_date,
            period_days=1,
            reset=terms['reset_base'],
            consume_promo_offer=False,
            description=f'Возобновление суточного тарифа «{tariff.name}»',
            daily_charge_at=now,
            extra_parameters={
                'kind': 'daily_resume',
                **renewal_daily_funding(
                    sub=sub,
                    state=state,
                    kind='daily_resume',
                    operation_key=_identity(user_id, subscription_id, operation_key),
                    total=quote.price_kopeks,
                    now=now,
                    new_end=quote.intended_end_date,
                ),
                'daily_resume_terms': terms,
                'daily_resume_terms_hash': quote.terms_hash,
            },
        )
