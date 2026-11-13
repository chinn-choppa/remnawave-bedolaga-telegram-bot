"""Atomic, confirmed-term switches between compatible activated split tariffs.

Periodic tariffs retain group topology across calendar and mode changes. Finite mode entry
requires a fresh explicit baseline; paid top-ups retain their original terms.
The caller collects outside locks, commits once and delivers through panel_sync.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm.attributes import set_committed_value

from app.config import settings
from app.database.crud.subscription import calc_device_limit_on_tariff_switch
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
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.intake import settle_known_group
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner
from app.services.bs_traffic.policy import _read_policy, _topology, project_access
from app.services.bs_traffic.renewal import RenewalBalanceError
from app.services.bs_traffic.renewal_result import RenewalResult
from app.services.bs_traffic.tariff_settings import BsTrafficSettings, require_matching_traffic_policy
from app.services.bs_traffic.tariff_switch_bases import switch_bases
from app.services.bs_traffic.tariff_switch_boundary import adopt_mode_baselines
from app.services.panel_sync.traffic_strategy import get_traffic_reset_strategy
from app.services.pricing_engine import TariffSwitchResult, pricing_engine
from app.services.tariff_switch_policy import remaining_days_for_switch, should_reset_used_traffic
from app.services.traffic_accounting_schedule import next_base_refresh


class SwitchQuoteChanged(LedgerConflict):
    """Confirmed terms changed before any debit in this operation."""


@dataclass(frozen=True)
class SwitchQuote:
    price_kopeks: int
    terms_hash: str
    current_tariff_id: int
    expected_end_date: datetime
    remaining_days: int
    is_upgrade: bool
    external_squad_uuid: str | None
    ordinary_mode: str
    bs_mode: str
    ordinary_limit_gb: int
    bs_limit_gb: int
    device_limit: int
    billing_kind: str | None = None
    source_daily_rate_kopeks: int | None = None
    target_daily_rate_kopeks: int | None = None
    paid_window_end: datetime | None = None


def _identity(user_id, subscription_id, operation_key):
    try:
        if not isinstance(operation_key, str) or str(UUID(operation_key)) != operation_key:
            raise ValueError
    except ValueError:
        raise LedgerConflict('Canonical UUID operation identity required') from None
    return 'split-switch:' + hashlib.sha256(f'{user_id}:{subscription_id}:{operation_key}'.encode()).hexdigest()


def _confirmed(price, digest):
    if type(price) is not int or not 0 <= price <= 2_147_483_647:
        raise LedgerConflict('Invalid confirmed switch price')
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise LedgerConflict('Invalid confirmed switch terms')


async def find_tariff_switch(
    db, *, user_id, subscription_id, new_tariff_id, expected_price_kopeks, expected_terms_hash, operation_key
):
    key = _identity(user_id, subscription_id, operation_key)
    _confirmed(expected_price_kopeks, expected_terms_hash)
    if type(new_tariff_id) is not int or new_tariff_id <= 0:
        raise LedgerConflict('Invalid target tariff')
    row = await db.scalar(
        select(BsTrafficRenewalIntent)
        .join(Subscription)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            BsTrafficRenewalIntent.operation_key == key,
        )
    )
    if row is None:
        return None
    from app.services.bs_traffic.enrollment import terms_hash

    payment = await db.get(Transaction, row.transaction_id)
    if (
        row.parameters.get('switch_terms_hash') != expected_terms_hash
        or not isinstance(row.parameters.get('switch_terms'), dict)
        or terms_hash(row.parameters['switch_terms']) != expected_terms_hash
        or row.parameters.get('tariff_id') != new_tariff_id
        or row.charged_kopeks != expected_price_kopeks
        or payment is None
        or payment.user_id != user_id
        or payment.external_id != key
        or payment.amount_kopeks != -expected_price_kopeks
        or not payment.is_completed
        or payment.type != TransactionType.SUBSCRIPTION_PAYMENT.value
        or payment.payment_method != PaymentMethod.BALANCE.value
        or row.parameters.get('total_amount_kopeks') != expected_price_kopeks
        or row.parameters.get('payment_receipt')
        != {'method': PaymentMethod.BALANCE.value, 'external_id': key, 'amount_kopeks': -expected_price_kopeks}
    ):
        raise LedgerConflict('Switch receipt does not match request')
    return RenewalResult(
        row.id,
        payment.id,
        row.parameters['grants'],
        datetime.fromisoformat(row.parameters['old_end_date']),
        row.intended_expires_at,
        row.charged_kopeks,
        True,
        row.state not in ('verified', 'superseded'),
    )


async def _prepare(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    new_tariff_id,
    evidence,
    target_evidence=None,
    current_squads=None,
):
    if not settings.is_tariffs_mode():
        raise LedgerConflict('Tariff sales are disabled')
    if type(new_tariff_id) is not int or new_tariff_id <= 0:
        raise LedgerConflict('Invalid target tariff')
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    await require_activated_owner(db, subscription_id)
    from app.services.bs_traffic.tariff_switch_topology import pending_topology_transition

    if await pending_topology_transition(db, state) is not None:
        raise LedgerConflict('Previous topology transition requires delivery')
    user = await lock_user_for_pricing(db, user_id)
    sub = await db.get(Subscription, subscription_id, populate_existing=True)
    from app.services.grace_access_echo import undo_grace_overlay_echo

    await undo_grace_overlay_echo(db, sub)
    await db.flush()
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        user.status != 'active'
        or user.restriction_subscription
        or sub.is_trial
        or sub.status not in ('active', 'limited')
        or sub.end_date is None
        or sub.end_date <= now
        or sub.tariff_id == new_tariff_id
    ):
        raise LedgerConflict('Subscription cannot switch tariffs')
    from app.services.bs_traffic.renewal_grace import require_no_unresolved_grace_restore
    from app.services.cispay_recurrent_guard import require_no_cispay

    await require_no_unresolved_grace_restore(db, subscription_id)
    await require_no_cispay(db, user_id=user_id, subscription_id=subscription_id)
    if await db.scalar(
        select(BsTrafficRenewalIntent.id)
        .where(BsTrafficRenewalIntent.subscription_id == subscription_id, BsTrafficRenewalIntent.state == 'attempted')
        .limit(1)
    ):
        raise LedgerConflict('Previous delivery requires reconciliation')
    tariffs = list(
        await db.scalars(
            select(Tariff)
            .where(Tariff.id.in_([sub.tariff_id, new_tariff_id]))
            .order_by(Tariff.id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
    )
    by_id = {item.id: item for item in tariffs}
    old, new = by_id.get(sub.tariff_id), by_id.get(new_tariff_id)
    if old is None or new is None or not new.is_active:
        raise LedgerConflict('Tariff not available')
    if bool(old.is_daily) != bool(new.is_daily):
        raise LedgerConflict('split_switch_daily_unsupported')
    if old.is_daily and (type(new.daily_price_kopeks) is not int or new.daily_price_kopeks <= 0):
        raise LedgerConflict('Active daily tariff with positive base price required')
    if old.is_daily and sub.is_daily_paused:
        raise LedgerConflict('Paused daily subscription must be unpaused first')
    if settings.TARIFF_SWITCH_RESET_FREE_DAYS and old.is_free and not old.is_daily:
        raise LedgerConflict('Free tariff requires paid purchase')
    promo = pricing_engine.resolve_promo_group(user)
    if not new.is_available_for_promo_group(promo.id if promo else None):
        raise LedgerConflict('Tariff not available for promo group')
    if settings.is_multi_tariff_enabled() and await db.scalar(
        select(Subscription.id)
        .where(
            Subscription.user_id == user_id,
            Subscription.id != subscription_id,
            Subscription.tariff_id == new_tariff_id,
            Subscription.status.in_(['active', 'trial', 'limited']),
            Subscription.end_date > now,
        )
        .limit(1)
    ):
        raise LedgerConflict('Another subscription already owns target tariff')
    policy = _read_policy(state.access_policy)
    try:
        require_matching_traffic_policy(old, policy)
        bs = BsTrafficSettings.model_validate(new.bs_traffic_settings)
        target_policy = dict(
            policy,
            tariff_id=new.id,
            ordinary_mode='finite' if new.traffic_limit_gb else 'unlimited',
            bs_mode='finite' if bs.limit_gb else 'unlimited',
        )
        topology_changed = (
            sorted(new.allowed_squads or []) != policy['topology']['ordinary_squads']
            or list(bs.squad_uuids) != policy['topology']['bs_squads']
        )
        if topology_changed:
            if not isinstance(target_evidence, TopologyEvidence):
                raise ValueError('Fresh target evidence required')
            target_policy.update(topology=_topology(target_evidence.topology), fingerprint=target_evidence.fingerprint)
        require_matching_traffic_policy(new, target_policy)
    except ValueError as error:
        raise LedgerConflict('split_switch_topology_or_modes_unsupported') from error
    if not bs.enabled or policy.get('bs_purchase_enabled', True) is False:
        raise LedgerConflict('Disabled BS sales require reconciliation')
    strategy = get_traffic_reset_strategy(new).value
    from app.services.bs_traffic.calendar_renewal import funded_calendar_strategy

    old_strategy = await funded_calendar_strategy(db, state=state, sub=sub)
    calendar_changed = old_strategy != strategy
    transition = None
    if topology_changed:
        from app.services.bs_traffic.grace_capture import _open_grace

        if await _open_grace(db, subscription_id):
            raise LedgerConflict('Topology transition waits for grace completion')
        from app.database.models import BsTrafficDelivery
        from app.services.bs_traffic.tariff_switch_topology import capture_transition

        unresolved_drop = await db.scalar(
            select(BsTrafficDelivery.id)
            .where(
                BsTrafficDelivery.subscription_id == subscription_id,
                BsTrafficDelivery.connection_drop['state'].as_string().in_(('pending', 'attempted', 'unknown')),
            )
            .limit(1)
        )
        if unresolved_drop is not None:
            raise LedgerConflict('Unresolved disconnect requires reconciliation before topology change')
        projection = await project_access(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            evidence=evidence,
            grace_active=False,
        )
        transition = capture_transition(
            projection,
            source_evidence=evidence,
            target_evidence=target_evidence,
            current_squads=current_squads,
            now=now,
            panel_identity=state.panel_scope,
        )
    if (
        not isinstance(evidence, TopologyEvidence)
        or not evidence.is_fresh(now, panel_identity=state.panel_scope)
        or evidence.fingerprint != policy['fingerprint']
    ):
        raise LedgerConflict('Fresh unchanged split topology required')
    source = await db.get(TrafficAccountingState, subscription_id)
    if source is None or source.calendar_anchor is None:
        raise LedgerConflict('Paid calendar is missing')
    days = remaining_days_for_switch(sub.end_date, now)
    device_limit = calc_device_limit_on_tariff_switch(
        current_device_limit=sub.device_limit,
        old_tariff_device_limit=old.device_limit,
        new_tariff_device_limit=new.device_limit,
        max_device_limit=new.max_device_limit,
    )
    daily = None
    if old.is_daily:
        from app.services.bs_traffic.daily_funding import (
            funding_identity,
            funding_root,
            prorated_daily_charge,
            resolve_daily_funding,
        )

        funding = await resolve_daily_funding(db, state=state, sub=sub)
        now = await db.scalar(select(func.clock_timestamp()))
        projected = SimpleNamespace(
            device_limit=device_limit, daily_devices_billing_version=sub.daily_devices_billing_version
        )
        rate = pricing_engine.daily_subscription_price(projected, new, user)
        charge = prorated_daily_charge(funding.full_rate_kopeks, rate, funding.window_end, now)
        price = TariffSwitchResult(charge, rate > funding.full_rate_kopeks, charge, 0, 0)
        daily = dict(
            source=funding_identity(funding),
            root=funding_root(funding),
            source_full_rate_kopeks=funding.full_rate_kopeks,
            target_full_rate_kopeks=rate,
            source_device_limit=funding.device_limit,
            daily_devices_billing_version=funding.daily_devices_billing_version,
            window_start=funding.window_start.isoformat(),
            window_end=funding.window_end.isoformat(),
            last_daily_charge_at=funding.last_daily_charge_at.isoformat(),
        )
    else:
        price = pricing_engine.calculate_tariff_switch_cost(old, new, days, user=user)
    if (price.is_upgrade and not settings.TARIFF_SWITCH_UPGRADE_ENABLED) or (
        not price.is_upgrade and not settings.TARIFF_SWITCH_DOWNGRADE_ENABLED
    ):
        raise LedgerConflict('Switch direction disabled')
    terms = {
        'account_generation': account_generation,
        'ownership_cycle': state.ownership_cycle,
        'current_tariff_id': old.id,
        'tariff_id': new.id,
        'end_date': sub.end_date.isoformat(),
        'price': price.upgrade_cost,
        'remaining_days': days,
        'ordinary_limit_gb': new.traffic_limit_gb,
        'ordinary_mode': target_policy['ordinary_mode'],
        'bs_mode': target_policy['bs_mode'],
        'bs': bs.model_dump(mode='json'),
        'device_limit': device_limit,
        'external_squad_uuid': new.external_squad_uuid,
        'topology': policy['fingerprint'],
        'calendar_strategy': strategy,
        'calendar_anchor': source.calendar_anchor.isoformat(),
        'calendar_timezone': source.calendar_timezone,
        'reset_base': should_reset_used_traffic(price.upgrade_cost),
    }
    if daily is not None:
        terms['daily'] = daily
    if transition is not None:
        terms['topology_transition'] = transition
    if calendar_changed:
        from app.services.bs_traffic.calendar_renewal import calendar_switch_source

        terms['source_calendar'] = await calendar_switch_source(
            db,
            state=state,
            sub=sub,
            strategy=old_strategy,
            anchor=source.calendar_anchor,
            timezone=source.calendar_timezone,
        )
    digest = hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    quote = SwitchQuote(
        price.upgrade_cost,
        digest,
        old.id,
        sub.end_date,
        days,
        price.is_upgrade,
        new.external_squad_uuid,
        terms['ordinary_mode'],
        terms['bs_mode'],
        new.traffic_limit_gb,
        bs.limit_gb,
        terms['device_limit'],
        'daily' if daily else None,
        daily['source_full_rate_kopeks'] if daily else None,
        daily['target_full_rate_kopeks'] if daily else None,
        sub.end_date if daily else None,
    )
    return quote, state, user, sub, new, policy, bs, now, source, terms, price


async def quote_tariff_switch(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    new_tariff_id,
    evidence,
    target_evidence=None,
    current_squads=None,
):
    return (
        await _prepare(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            new_tariff_id=new_tariff_id,
            evidence=evidence,
            target_evidence=target_evidence,
            current_squads=current_squads,
        )
    )[0]


async def switch_tariff_from_balance(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    new_tariff_id,
    expected_price_kopeks,
    expected_terms_hash,
    operation_key,
    evidence,
    collection_job_ids=(),
    target_evidence=None,
    current_squads=None,
):
    request = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        new_tariff_id=new_tariff_id,
        expected_price_kopeks=expected_price_kopeks,
        expected_terms_hash=expected_terms_hash,
        operation_key=operation_key,
    )
    historical = await find_tariff_switch(db, **request)
    if historical is not None:
        return historical
    key = _identity(user_id, subscription_id, operation_key)
    async with db.begin_nested():
        await _lock(db, user_id, subscription_id, account_generation)
        historical = await find_tariff_switch(db, **request)
        if historical is not None:
            return historical
        quote, state, user, sub, tariff, policy, bs, now, source, terms, price = await _prepare(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            new_tariff_id=new_tariff_id,
            evidence=evidence,
            target_evidence=target_evidence,
            current_squads=current_squads,
        )
        if quote.price_kopeks != expected_price_kopeks or quote.terms_hash != expected_terms_hash:
            raise SwitchQuoteChanged('Switch price or confirmed terms changed')
        boundary = next_base_refresh(
            terms['calendar_strategy'],
            now=now,
            anchor=source.calendar_anchor,
            timezone=ZoneInfo(source.calendar_timezone),
        )
        amounts = {}
        if terms['ordinary_mode'] == 'finite':
            amounts['ordinary'] = tariff.traffic_limit_gb * 1024**3
        if terms['bs_mode'] == 'finite':
            amounts['bs'] = bs.limit_gb * 1024**3
        target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
        if 'source_calendar' in terms:
            from app.services.bs_traffic.calendar_renewal import refresh_calendar_base

            await refresh_calendar_base(db, **target, now=now)
        for group in ('ordinary', 'bs'):
            if policy[f'{group}_mode'] == 'finite':
                await settle_known_group(db, **target, group=group)
        mode_boundary = await adopt_mode_baselines(
            db,
            **target,
            new_modes={group: terms[f'{group}_mode'] for group in ('ordinary', 'bs')},
            collection_job_ids=collection_job_ids,
            target_topology=terms.get('topology_transition', {}).get('target', {}).get('topology'),
        )
        cutoffs = dict(policy.get('finite_since', {}))
        cutoffs.update(
            {
                group: value['finite_since']
                for group, value in mode_boundary.items()
                if policy[f'{group}_mode'] == 'unlimited'
            }
        )
        target_policy = dict(
            policy, tariff_id=tariff.id, ordinary_mode=terms['ordinary_mode'], bs_mode=terms['bs_mode']
        )
        if 'topology_transition' in terms:
            target_policy.update(terms['topology_transition']['target'])
        if cutoffs:
            target_policy['finite_since'] = cutoffs
        state.access_policy = target_policy
        await db.flush()
        description = f'Смена тарифа на {tariff.name}'
        if not await subtract_user_balance(
            db,
            user,
            quote.price_kopeks,
            description,
            consume_promo_offer=bool(price.offer_discount_pct and quote.price_kopeks),
            mark_as_paid_subscription=True,
            commit=False,
        ):
            raise RenewalBalanceError('Insufficient balance')
        grants, excess, mode_consumption = await switch_bases(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            operation_key=key,
            amounts=amounts,
            starts_at=now,
            expires_at=min(sub.end_date, boundary) if boundary else sub.end_date,
            reset=terms['reset_base'],
        )
        sub.tariff_id = tariff.id
        set_committed_value(sub, 'tariff', tariff)
        sub.traffic_limit_gb = tariff.traffic_limit_gb
        sub.device_limit = terms['device_limit']
        sub.connected_squads = list(tariff.allowed_squads or [])
        sub.pending_panel_expire_at = sub.end_date
        sub.status = 'active'
        state.access_policy = target_policy
        state.revision += 1
        transaction = await create_transaction(
            db,
            user_id=user_id,
            type=TransactionType.SUBSCRIPTION_PAYMENT,
            amount_kopeks=quote.price_kopeks,
            description=description,
            payment_method=PaymentMethod.BALANCE,
            external_id=key,
            commit=False,
        )
        daily_parameters = {}
        if 'daily' in terms:
            from app.services.bs_traffic.daily_funding import build_daily_funding
            from app.services.bs_traffic.enrollment import terms_hash

            daily = terms['daily']
            snapshot = build_daily_funding(
                sub=sub,
                kind='tariff_switch',
                full_rate_kopeks=daily['target_full_rate_kopeks'],
                window_start=daily['window_start'],
                window_end=daily['window_end'],
                last_daily_charge_at=daily['last_daily_charge_at'],
                source=dict(
                    kind='tariff_switch',
                    operation_key=key,
                    user_id=user_id,
                    subscription_id=subscription_id,
                    ownership_cycle=state.ownership_cycle,
                    account_generation=account_generation,
                    panel_scope=state.panel_scope,
                    previous=daily['source'],
                    root=daily['root'],
                ),
            )
            remaining = sub.end_date - now
            daily_parameters = dict(
                daily_funding=snapshot,
                daily_funding_hash=terms_hash(snapshot),
                daily_priced_at=now.isoformat(),
                daily_remaining_us=(remaining.days * 86400 + remaining.seconds) * 1000000 + remaining.microseconds,
            )
        row = BsTrafficRenewalIntent(
            subscription_id=subscription_id,
            operation_key=key,
            transaction_id=transaction.id,
            charged_kopeks=quote.price_kopeks,
            panel_scope=state.panel_scope,
            account_generation=account_generation,
            intended_expires_at=sub.end_date,
            state='pending',
            parameters={
                **daily_parameters,
                'kind': 'tariff_switch',
                'switch_terms_hash': quote.terms_hash,
                'switch_terms': terms,
                'period_days': 0,
                'total_amount_kopeks': quote.price_kopeks,
                'amounts': amounts,
                'payment_receipt': {
                    'method': PaymentMethod.BALANCE.value,
                    'external_id': key,
                    'amount_kopeks': -quote.price_kopeks,
                },
                'grants': grants,
                'old_end_date': sub.end_date.isoformat(),
                'reset_base': terms['reset_base'],
                'tariff_id': tariff.id,
                'calendar_strategy': terms['calendar_strategy'],
                'calendar_timezone': source.calendar_timezone,
                'calendar_anchor': source.calendar_anchor.isoformat(),
                'next_refresh_at': boundary.isoformat() if boundary else None,
                'consumption_excess_bytes': excess,
                'mode_boundary': mode_boundary,
                'mode_consumption': mode_consumption,
            },
        )
        db.add(row)
        await db.flush()
        if 'topology_transition' in terms:
            from app.services.bs_traffic.node_membership import capture_memberships

            await capture_memberships(
                db,
                state=state,
                sub=sub,
                receipt=row,
                transition=terms['topology_transition'],
                now=now,
                baselines=mode_boundary,
            )
            state.access_policy = dict(state.access_policy, topology_transition_id=row.id)
            await db.flush()
        return RenewalResult(row.id, transaction.id, grants, sub.end_date, sub.end_date, quote.price_kopeks)
