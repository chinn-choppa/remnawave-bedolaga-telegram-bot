"""Capture funded initial BS terms before commit; no panel IO or wallet mutation."""

import hashlib
import json
from datetime import datetime

from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficEnrollment,
    BsTrafficLedger,
    GuestPurchase,
    Subscription,
    Tariff,
    Transaction,
    User,
)
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.tariff_settings import normalize_bs_settings


def enabled_bs_tariff(tariff):
    if tariff is None or getattr(tariff, 'bs_traffic_settings', None) is None:
        return False
    return normalize_bs_settings(tariff.bs_traffic_settings, tariff.allowed_squads)['enabled']


def terms_hash(terms):
    return hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def pending_enrollment(db, subscription_id):
    return await db.scalar(
        select(BsTrafficEnrollment).where(
            BsTrafficEnrollment.subscription_id == subscription_id,
            BsTrafficEnrollment.state != 'verified',
        )
    )


async def require_no_pending_enrollment(db, subscription_id):
    if await pending_enrollment(db, subscription_id) is not None:
        raise LedgerConflict('Paid BS enrollment is still pending')


async def validate_funding(db, job):
    """Repeated before granting, using an immutable identity and exact amount."""
    if terms_hash(job.terms) != job.terms_hash:
        raise LedgerConflict('Enrollment terms changed')
    source = job.terms['source']
    if job.source_kind == 'payment':
        tx = await db.get(Transaction, job.transaction_id, populate_existing=True)
        if (
            tx is None
            or tx.user_id != job.user_id
            or tx.type != 'subscription_payment'
            or not tx.is_completed
            or tx.amount_kopeks != source['amount_kopeks']
            or tx.payment_method != source['payment_method']
            or tx.external_id != source['external_id']
        ):
            raise LedgerConflict('Enrollment payment receipt changed')
    elif job.source_kind == 'guest':
        purchase = await db.get(GuestPurchase, job.guest_purchase_id, populate_existing=True)
        if (
            purchase is None
            or purchase.user_id != job.user_id
            or purchase.tariff_id != job.tariff_id
            or purchase.paid_at is None
            or purchase.status not in ('paid', 'delivered', 'pending_activation')
            or purchase.amount_kopeks != source['amount_kopeks']
            or purchase.currency != 'RUB'
            or purchase.payment_method != source['payment_method']
            or purchase.payment_id != source['external_id']
        ):
            raise LedgerConflict('Enrollment guest receipt changed')
    elif job.source_kind != 'admin' or job.actor_id != source.get('actor_id'):
        raise LedgerConflict('Enrollment provenance is invalid')


async def stage_paid_enrollment(db, subscription, *, transaction):
    return await _stage(db, subscription, source_kind='payment', transaction=transaction)


async def stage_admin_enrollment(db, subscription, *, actor, permission='users:subscription'):
    if subscription.is_trial or subscription.tariff_id is None or not settings.is_tariffs_mode():
        return None
    tariff = await db.get(Tariff, subscription.tariff_id)
    if not enabled_bs_tariff(tariff):
        return None
    if actor is None:
        raise LedgerConflict('Authorized actor required')
    from app.services.permission_service import PermissionService

    if permission not in ('users:subscription', 'bulk_actions:execute'):
        raise LedgerConflict('Unsupported subscription issue permission')
    allowed, _ = await PermissionService.check_permission(db, actor, permission)
    if not allowed:
        raise LedgerConflict('Actor cannot issue subscription rights')
    return await _stage(db, subscription, source_kind='admin', actor=actor)


async def stage_guest_enrollment(db, subscription, *, purchase):
    return await _stage(db, subscription, source_kind='guest', purchase=purchase)


async def _stage(db, subscription, *, source_kind, transaction=None, actor=None, purchase=None):
    if subscription.is_trial or subscription.tariff_id is None or not settings.is_tariffs_mode():
        return None
    if subscription.traffic_accounting_mode in ('split', 'split_pending'):
        return None
    # Ordinary tariffs do not touch the separate enrollment store. Eligibility
    # is checked again under the owner/tariff locks before creating any rights.
    candidate = await db.get(Tariff, subscription.tariff_id)
    if not enabled_bs_tariff(candidate):
        return None
    await db.flush()
    owner = await db.scalar(select(User).where(User.id == subscription.user_id).with_for_update())
    sub = await db.scalar(select(Subscription).where(Subscription.id == subscription.id).with_for_update())
    old = await db.scalar(
        select(BsTrafficEnrollment)
        .where(
            BsTrafficEnrollment.subscription_id == sub.id,
        )
        .order_by(BsTrafficEnrollment.id.desc())
        .limit(1)
        .with_for_update()
    )
    identity = (
        source_kind,
        transaction.id if transaction is not None else None,
        actor.id if actor is not None else None,
        purchase.id if purchase is not None else None,
    )
    if source_kind in ('payment', 'guest'):
        funding_column = (
            BsTrafficEnrollment.transaction_id if source_kind == 'payment' else BsTrafficEnrollment.guest_purchase_id
        )
        prior_funding = await db.scalar(
            select(BsTrafficEnrollment)
            .where(
                BsTrafficEnrollment.source_kind == source_kind,
                funding_column == (identity[1] if source_kind == 'payment' else identity[3]),
            )
            .with_for_update()
        )
        if prior_funding is not None:
            if prior_funding.subscription_id != sub.id or prior_funding.user_id != sub.user_id:
                raise LedgerConflict('Payment already funds another subscription enrollment')
            await validate_funding(db, prior_funding)
            return prior_funding
    if old is not None:
        if (old.source_kind, old.transaction_id, old.actor_id, old.guest_purchase_id) == identity:
            await validate_funding(db, old)
            return old
        if old.state != 'verified' or sub.traffic_accounting_mode != 'managed':
            raise LedgerConflict('Previous paid enrollment must complete first')
    tariff = await db.scalar(select(Tariff).where(Tariff.id == sub.tariff_id).with_for_update(read=True))
    if not enabled_bs_tariff(tariff):
        return None
    now = await db.scalar(select(func.clock_timestamp()))
    # Single-tariff provisioning uses the owner's canonical account. A global
    # sales-mode flag is not an ownership proof: reject actual sibling claims.
    panel_id = sub.remnawave_id or (owner.remnawave_id if not settings.is_multi_tariff_enabled() else None)
    shared_panel = panel_id is not None and await db.scalar(
        select(Subscription.id).where(Subscription.id != sub.id, Subscription.remnawave_id == panel_id).limit(1)
    )
    other_live = not settings.is_multi_tariff_enabled() and await db.scalar(
        select(Subscription.id)
        .where(
            Subscription.id != sub.id,
            Subscription.user_id == sub.user_id,
            Subscription.status.in_(('active', 'trial', 'limited')),
            Subscription.end_date > now,
        )
        .limit(1)
    )
    if shared_panel or other_live:
        raise LedgerConflict('Separate traffic requires exclusive subscription ownership of the panel account')
    if sub.status != 'active' or not isinstance(sub.end_date, datetime) or sub.end_date <= now:
        raise LedgerConflict('Enrollment requires a live paid subscription')
    bs = normalize_bs_settings(tariff.bs_traffic_settings, tariff.allowed_squads)
    from app.services.panel_sync.traffic_strategy import get_traffic_reset_strategy

    if transaction is not None:
        source = dict(
            amount_kopeks=transaction.amount_kopeks,
            payment_method=transaction.payment_method,
            external_id=transaction.external_id,
        )
    elif purchase is not None:
        source = dict(
            amount_kopeks=purchase.amount_kopeks,
            payment_method=purchase.payment_method,
            external_id=purchase.payment_id,
        )
    else:
        source = {'actor_id': actor.id}
    terms = dict(
        version=1,
        ordinary_source_preexisting=sub.traffic_accounting_mode in ('managed', 'migrating'),
        tariff_id=tariff.id,
        paid_until=sub.end_date.isoformat(),
        paid_from=sub.start_date.isoformat(),
        captured_at=now.isoformat(),
        ordinary_squads=sorted(tariff.allowed_squads or []),
        bs_squads=bs['squad_uuids'],
        ordinary_limit_bytes=(sub.traffic_limit_gb or 0) * 1024**3,
        ordinary_base_bytes=max(0, (sub.traffic_limit_gb or 0) - (sub.purchased_traffic_gb or 0)) * 1024**3,
        bs_limit_bytes=bs['limit_gb'] * 1024**3,
        calendar_strategy=get_traffic_reset_strategy(tariff).value,
        calendar_timezone=settings.REMNAWAVE_TRAFFIC_RESET_TIMEZONE,
        source=source,
    )
    job = BsTrafficEnrollment(
        subscription_id=sub.id,
        user_id=sub.user_id,
        tariff_id=tariff.id,
        source_kind=source_kind,
        transaction_id=identity[1],
        actor_id=identity[2],
        guest_purchase_id=identity[3],
        terms=terms,
        terms_hash=terms_hash(terms),
        state='pending',
    )
    await validate_funding(db, job)
    ledger = await db.get(BsTrafficLedger, sub.id)
    if ledger is not None and sub.traffic_accounting_mode == 'managed':
        from app.database.models import BsTrafficOwnershipCycle
        from app.services.bs_traffic.ownership_cycle import begin_next_cycle

        cycle = await db.get(BsTrafficOwnershipCycle, (sub.id, ledger.ownership_cycle))
        if cycle is not None and cycle.state == 'managed':
            if source_kind not in ('payment', 'guest'):
                raise LedgerConflict('A new paid receipt is required to re-enable separate traffic')
            job.ownership_cycle = await begin_next_cycle(
                db,
                user_id=sub.user_id,
                subscription_id=sub.id,
                account_generation=ledger.account_generation,
                allow_funded_pending_sync=True,
            )
        elif old is not None:
            raise LedgerConflict('Verified ownership return is required before re-enrollment')
    db.add(job)
    await db.flush()
    return job


async def attach_renewal_request(db, job, transaction, *, operation_key, period_days, total, charge, old_end_date):
    """Freeze an existing paid renewal request in the same enrollment transaction."""
    from app.services.bs_traffic.ledger import _key

    _key(operation_key)
    await validate_funding(db, job)
    key = 'split-renewal:' + hashlib.sha256(f'{job.user_id}:{job.subscription_id}:{operation_key}'.encode()).hexdigest()
    if (
        job.state != 'pending'
        or job.account_generation is not None
        or job.transaction_id != transaction.id
        or transaction.external_id not in (None, key)
        or transaction.amount_kopeks != -total
    ):
        raise LedgerConflict('Enrollment renewal payment does not match request')
    transaction.external_id = key
    terms = dict(job.terms)
    terms['source'] = dict(terms['source'], external_id=key)
    terms['renewal_request'] = dict(
        operation_key=key,
        period_days=period_days,
        total_amount_kopeks=total,
        charged_kopeks=charge,
        old_end_date=old_end_date.isoformat(),
    )
    job.terms, job.terms_hash = terms, terms_hash(terms)
    await db.flush()


async def find_renewal_enrollment(
    db, *, user_id, subscription_id, key, period_days, expected_price_kopeks, charge_balance_amount, payment_method
):
    """Historical acknowledgment only; never re-enroll or deliver old rights."""
    from app.services.bs_traffic.renewal_result import RenewalResult

    job = await db.scalar(
        select(BsTrafficEnrollment)
        .join(Transaction, Transaction.id == BsTrafficEnrollment.transaction_id)
        .join(Subscription, Subscription.id == BsTrafficEnrollment.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            BsTrafficEnrollment.user_id == user_id,
            Transaction.external_id == key,
        )
    )
    if job is None:
        return None
    await validate_funding(db, job)
    request = job.terms.get('renewal_request', {})
    total = request.get('total_amount_kopeks')
    charge = total if charge_balance_amount is None else charge_balance_amount
    if (
        type(total) is not int
        or total < 0
        or request.get('operation_key') != key
        or request.get('period_days') != period_days
        or request.get('charged_kopeks') != charge
        or (expected_price_kopeks is not None and expected_price_kopeks != total)
        or job.terms['source']['amount_kopeks'] != -total
        or job.terms['source']['payment_method'] != payment_method.value
    ):
        raise LedgerConflict('Enrollment renewal receipt does not match request')
    return RenewalResult(
        None,
        job.transaction_id,
        {},
        datetime.fromisoformat(request['old_end_date']),
        datetime.fromisoformat(job.terms['paid_until']),
        charge,
        True,
        job.state != 'verified',
    )
