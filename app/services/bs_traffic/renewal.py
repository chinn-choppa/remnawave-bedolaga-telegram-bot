"""One transactional split balance renewal; caller commits then delivers intent."""

import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.config import settings
from app.database.crud.transaction import create_transaction
from app.database.crud.user import lock_user_for_pricing, subtract_user_balance
from app.database.models import (
    BsTrafficRenewalIntent,
    PaymentMethod,
    Subscription,
    Tariff,
    TrafficAccountingState,
    Transaction,
    TransactionType,
)
from app.services.bs_traffic.base_renewal import reset_renewal_bases
from app.services.bs_traffic.ledger import LedgerConflict, _key, _lock, require_activated_owner
from app.services.bs_traffic.policy import _read_policy, capture_bs_drain_before_extension
from app.services.bs_traffic.renewal_result import RenewalResult
from app.services.bs_traffic.tariff_settings import BsTrafficSettings, require_matching_traffic_policy
from app.services.panel_sync.traffic_strategy import get_traffic_reset_strategy
from app.services.pricing_engine import pricing_engine
from app.services.traffic_accounting_schedule import next_base_refresh


class RenewalBalanceError(LedgerConflict):
    """The locked balance could not fund this renewal."""


async def find_balance_renewal(
    db,
    *,
    user_id,
    subscription_id,
    period_days,
    operation_key,
    expected_price_kopeks=None,
    charge_balance_amount=None,
    payment_method=PaymentMethod.BALANCE,
):
    """Acknowledge historical owned funding without activating or mutating rights.

    Account generation, current owner mode and sales settings may legitimately
    have changed since payment. The immutable request and actual money receipt
    still have to match exactly; this path never performs panel IO.
    """
    _key(operation_key)
    if type(period_days) is not int or period_days <= 0 or not isinstance(payment_method, PaymentMethod):
        raise LedgerConflict('Invalid historical renewal request')
    key = 'split-renewal:' + hashlib.sha256(f'{user_id}:{subscription_id}:{operation_key}'.encode()).hexdigest()
    receipt = await db.scalar(
        select(BsTrafficRenewalIntent)
        .join(Subscription, Subscription.id == BsTrafficRenewalIntent.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            BsTrafficRenewalIntent.operation_key == key,
        )
    )
    if receipt is None:
        from app.services.bs_traffic.enrollment import find_renewal_enrollment

        return await find_renewal_enrollment(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            key=key,
            period_days=period_days,
            expected_price_kopeks=expected_price_kopeks,
            charge_balance_amount=charge_balance_amount,
            payment_method=payment_method,
        )
    total = receipt.parameters.get('total_amount_kopeks')
    charge = total if charge_balance_amount is None else charge_balance_amount
    payment = await db.get(Transaction, receipt.transaction_id)
    if (
        type(total) is not int
        or total < 0
        or receipt.parameters.get('period_days') != period_days
        or (expected_price_kopeks is not None and expected_price_kopeks != total)
        or receipt.charged_kopeks != charge
        or payment is None
        or payment.user_id != user_id
        or payment.external_id != key
        or payment.amount_kopeks != -total
        or not payment.is_completed
        or payment.type != TransactionType.SUBSCRIPTION_PAYMENT.value
        or payment.payment_method != payment_method.value
        or receipt.parameters.get('payment_receipt')
        != {
            'method': payment_method.value,
            'external_id': key,
            'amount_kopeks': -total,
        }
    ):
        raise LedgerConflict('Renewal receipt does not match request')
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


async def renew_from_balance(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    period_days,
    expected_price_kopeks,
    operation_key,
    charge_balance_amount=None,
    payment_method=PaymentMethod.BALANCE,
):
    """Reprice under owner/tariff locks; atomically persist debit and all rights.

    A stable caller operation ID permits offline replay before repricing. No
    network, commit or notification occurs inside this transaction.
    """
    _key(operation_key)
    if type(period_days) is not int or period_days <= 0:
        raise LedgerConflict('Invalid renewal period')
    if type(expected_price_kopeks) is not int or not 0 <= expected_price_kopeks <= 2_147_483_647:
        raise LedgerConflict('Invalid renewal price')
    charge = expected_price_kopeks if charge_balance_amount is None else charge_balance_amount
    if (
        type(charge) is not int
        or not 0 <= charge <= expected_price_kopeks
        or not isinstance(payment_method, PaymentMethod)
    ):
        raise LedgerConflict('Invalid trusted renewal payment allocation')
    historical = await find_balance_renewal(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        period_days=period_days,
        operation_key=operation_key,
        expected_price_kopeks=expected_price_kopeks,
        charge_balance_amount=charge,
        payment_method=payment_method,
    )
    if historical is not None:
        return historical
    key = 'split-renewal:' + hashlib.sha256(f'{user_id}:{subscription_id}:{operation_key}'.encode()).hexdigest()
    async with db.begin_nested():
        state, _ = await _lock(db, user_id, subscription_id, account_generation)
        await require_activated_owner(db, subscription_id)
        receipt = await db.scalar(
            select(BsTrafficRenewalIntent).where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.operation_key == key,
            )
        )
        if receipt is not None:
            payment = await db.get(Transaction, receipt.transaction_id)
            if (
                receipt.parameters.get('period_days') != period_days
                or receipt.charged_kopeks != charge
                or receipt.parameters.get('total_amount_kopeks') != expected_price_kopeks
                or receipt.account_generation != account_generation
                or payment is None
                or payment.user_id != user_id
                or payment.external_id != key
                or payment.amount_kopeks != -expected_price_kopeks
                or not payment.is_completed
                or payment.type != TransactionType.SUBSCRIPTION_PAYMENT.value
                or payment.payment_method != payment_method.value
            ):
                raise LedgerConflict('Renewal receipt does not match request')
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
        from app.services.bs_traffic.renewal_grace import require_no_unresolved_grace_restore

        await require_no_unresolved_grace_restore(db, subscription_id)
        user = await lock_user_for_pricing(db, user_id)
        sub = await db.get(Subscription, subscription_id, populate_existing=True)
        if (
            user.status != 'active'
            or user.restriction_subscription
            or sub.status not in ('active', 'trial', 'limited', 'expired')
        ):
            raise LedgerConflict('Subscription cannot be renewed')
        if await db.scalar(
            select(BsTrafficRenewalIntent.id)
            .where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.state == 'attempted',
            )
            .limit(1)
        ):
            raise LedgerConflict('Previous renewal delivery must be reconciled')
        from app.services.cispay_recurrent_guard import require_no_cispay
        from app.services.grace_access_echo import undo_grace_overlay_echo

        await require_no_cispay(db, user_id=user_id, subscription_id=subscription_id)
        await undo_grace_overlay_echo(db, sub)
        await db.flush()
        await capture_bs_drain_before_extension(db, sub)
        tariff = await db.scalar(
            select(Tariff)
            .where(Tariff.id == sub.tariff_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        policy = _read_policy(state.access_policy)
        try:
            require_matching_traffic_policy(tariff, policy)
            bs = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
        except ValueError as error:
            raise LedgerConflict('Tariff traffic settings changed') from error
        if not tariff.has_configured_price_for_period(period_days):
            raise LedgerConflict('Renewal period is not configured')
        # Relationship must refer to the freshly locked tariff, not an earlier price snapshot.
        from sqlalchemy.orm.attributes import set_committed_value

        set_committed_value(sub, 'tariff', tariff)
        price = await pricing_engine.calculate_renewal_price(db, sub, period_days, user=user)
        total = int(price.final_total)
        if total != expected_price_kopeks:
            raise LedgerConflict('Renewal price changed')
        now = await db.scalar(select(func.clock_timestamp()))
        old_end = sub.end_date
        if old_end is None:
            raise LedgerConflict('Paid expiry is missing')
        try:
            base = max(old_end, now) if not sub.is_trial or settings.TRIAL_ADD_REMAINING_DAYS_TO_PAID else now
            new_end = base + timedelta(days=period_days)
        except OverflowError:
            raise LedgerConflict('Renewal period exceeds supported expiry') from None
        reset = bool(settings.RESET_TRAFFIC_ON_PAYMENT or sub.is_trial)
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
            total=total,
            charge=charge,
            payment_method=payment_method,
            now=now,
            old_end=old_end,
            new_end=new_end,
            period_days=period_days,
            reset=reset,
            consume_promo_offer=bool(price.promo_offer_discount),
            description=f'Продление подписки на {period_days} дней',
        )


async def _topology_successor(db, *, state, sub, now, old_end):
    """Read-only permission for the next paid period under the owner's locks."""
    from app.services.bs_traffic.tariff_switch_topology import pending_topology_transition

    transition = await pending_topology_transition(db, state)
    successor_grace_id = None
    inherited_transition = None
    if transition is not None:
        if transition.state == 'pending' and old_end <= now and transition.intended_expires_at <= now:
            from app.services.bs_traffic.grace_topology import eligible_expired_topology_successor

            successor_grace_id = await eligible_expired_topology_successor(
                db, state=state, sub=sub, receipt=transition, now=now
            )
            if successor_grace_id is None:
                from app.services.bs_traffic.tariff_switch_topology import inherit_expired_transition

                inherited_transition = await inherit_expired_transition(
                    db, state=state, sub=sub, receipt=transition, now=now
                )
        if successor_grace_id is None and inherited_transition is None:
            raise LedgerConflict('Previous topology delivery must be reconciled before renewal')
    return transition, successor_grace_id, inherited_transition


async def prepare_renewal_topology(db, *, user_id, subscription_id, account_generation):
    """Reject known-undeliverable new checkout before requesting more money.

    This is a local preflight, not a reservation or panel authorization. The
    financial coordinator repeats the same checks before applying payment.
    Existing paid receipt replay must precede this new-checkout-only guard.
    """
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    await require_activated_owner(db, subscription_id)
    sub = await db.get(Subscription, subscription_id, populate_existing=True)
    now = await db.scalar(select(func.clock_timestamp()))
    if sub.end_date is None:
        raise LedgerConflict('Paid expiry is missing')
    await _topology_successor(db, state=state, sub=sub, now=now, old_end=sub.end_date)


async def _commit_paid_renewal(
    db,
    *,
    state,
    user,
    sub,
    tariff,
    policy,
    bs,
    account_generation,
    key,
    total,
    charge,
    payment_method,
    now,
    old_end,
    new_end,
    period_days,
    reset,
    consume_promo_offer,
    description,
    extra_parameters=None,
    daily_charge_at=None,
    transaction_external_id=None,
):
    """Apply server-selected terms under caller-owned locks and savepoint.

    Periodic and daily coordinators own eligibility, repricing and idempotency.
    This internal helper has no network, commit or notification side effects.
    """
    transition, successor_grace_id, inherited_transition = await _topology_successor(
        db, state=state, sub=sub, now=now, old_end=old_end
    )
    user_id, subscription_id = user.id, sub.id
    transaction_key = key if transaction_external_id is None else transaction_external_id
    if not isinstance(transaction_key, str) or not 1 <= len(transaction_key) <= 255:
        raise LedgerConflict('Invalid trusted payment transaction identity')
    source = await db.get(TrafficAccountingState, subscription_id)
    if source is None or source.calendar_anchor is None:
        raise LedgerConflict('Renewal calendar is not initialized')
    strategy = get_traffic_reset_strategy(tariff).value
    boundary = next_base_refresh(
        strategy, now=now, anchor=source.calendar_anchor, timezone=ZoneInfo(source.calendar_timezone)
    )
    grant_end = min(new_end, boundary) if boundary else new_end
    amounts = {}
    if policy['ordinary_mode'] == 'finite':
        amounts['ordinary'] = tariff.traffic_limit_gb * 1024**3
    if bs.enabled and policy.get('bs_purchase_enabled', True) and policy['bs_mode'] == 'finite':
        amounts['bs'] = bs.limit_gb * 1024**3
    if not await subtract_user_balance(
        db,
        user,
        charge,
        description,
        consume_promo_offer=consume_promo_offer,
        mark_as_paid_subscription=True,
        commit=False,
    ):
        raise RenewalBalanceError('Insufficient balance')
    grants = (
        await reset_renewal_bases(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            operation_key=key,
            amounts=amounts,
            starts_at=now,
            expires_at=grant_end,
            reset_base=reset,
        )
        if amounts
        else {}
    )
    if sub.is_trial:
        from app.database.crud.subscription import apply_trial_conversion_defaults

        sub.is_trial = False
        apply_trial_conversion_defaults(sub)
    sub.end_date = new_end
    sub.pending_panel_expire_at = new_end
    sub.status = 'active'
    sub.is_daily_paused = False
    if daily_charge_at is not None:
        sub.last_daily_charge_at = daily_charge_at
    state.revision += 1
    transaction = await create_transaction(
        db,
        user_id=user_id,
        type=TransactionType.SUBSCRIPTION_PAYMENT,
        amount_kopeks=total,
        description=description,
        payment_method=payment_method,
        external_id=transaction_key,
        commit=False,
    )
    receipt = BsTrafficRenewalIntent(
        subscription_id=subscription_id,
        operation_key=key,
        transaction_id=transaction.id,
        charged_kopeks=charge,
        panel_scope=state.panel_scope,
        account_generation=account_generation,
        intended_expires_at=new_end,
        state='pending',
        parameters={
            **(extra_parameters or {}),
            **(inherited_transition or {}),
            'period_days': period_days,
            'total_amount_kopeks': total,
            'amounts': amounts,
            'payment_receipt': {
                'method': payment_method.value,
                'external_id': transaction_key,
                'amount_kopeks': -total,
            },
            'grants': grants,
            'old_end_date': old_end.isoformat(),
            'reset_base': reset,
            'tariff_id': tariff.id,
            'calendar_strategy': strategy,
            'calendar_timezone': source.calendar_timezone,
            'calendar_anchor': source.calendar_anchor.isoformat(),
            'next_refresh_at': boundary.isoformat() if boundary else None,
        },
    )
    db.add(receipt)
    await db.flush()
    if transition is not None:
        # Preserve removal authority in the new funded receipt, or record
        # that a verified grace overlay already removed the old squads.
        # Supersession shares the payment savepoint; unknown writes never enter.
        transition.state = 'superseded'
        transition.parameters = dict(transition.parameters, superseded_by_intent_id=receipt.id)
        current_policy = dict(state.access_policy)
        if inherited_transition is not None:
            current_policy['topology_transition_id'] = receipt.id
        else:
            receipt.parameters = dict(
                receipt.parameters,
                topology_supersession={'intent_id': transition.id, 'grace_id': successor_grace_id},
            )
            current_policy.pop('topology_transition_id')
        state.access_policy = current_policy
        await db.flush()
    return RenewalResult(receipt.id, transaction.id, grants, old_end, new_end, charge)


async def deliver_renewal(db, intent_id):
    """Best-effort first delivery; runtime recovers the persisted receipt."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.panel_binding import bind_reader
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from app.services.remnawave_service import RemnaWaveService

    intent = await db.get(BsTrafficRenewalIntent, intent_id)
    sub = await db.get(Subscription, intent.subscription_id)
    factory = async_sessionmaker(db.bind, expire_on_commit=False)
    async with RemnaWaveService().get_api_client() as api:
        reader = await bind_reader(factory, api, panel_scope=intent.panel_scope)
        state = await advance_paid_renewal(
            reader,
            user_id=sub.user_id,
            subscription_id=sub.id,
            account_generation=intent.account_generation,
            operation_id=intent.id,
        )
    return state in ('verified', 'already_verified', 'superseded')
