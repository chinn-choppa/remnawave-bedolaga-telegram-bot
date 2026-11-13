"""Immutable proof of one funded daily window; never reads mutable tariff prices."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import func, select

from app.database.models import BsTrafficEnrollment, BsTrafficRenewalIntent, Transaction
from app.services.bs_traffic.enrollment import terms_hash, validate_funding
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.ownership_cycle import renewal_floor


@dataclass(frozen=True)
class DailyFunding:
    tariff_id: int
    window_start: datetime
    window_end: datetime
    last_daily_charge_at: datetime
    full_rate_kopeks: int
    device_limit: int
    daily_devices_billing_version: int
    source_kind: str
    receipt_id: int
    transaction_id: int
    digest: str
    root_source: dict | None = None


def _date(value):
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(UTC)
    except (ValueError, TypeError) as error:
        raise LedgerConflict('Invalid daily funding date') from error


def _integer(value, *, minimum=0):
    if type(value) is not int or value < minimum:
        raise LedgerConflict('Invalid daily funding integer')
    return value


def build_daily_funding(*, sub, kind, full_rate_kopeks, window_start, window_end, last_daily_charge_at, source):
    """Only server checkout writers may attest an explicitly purchased 24-hour window."""
    start, end, charged_at = map(_date, (window_start, window_end, last_daily_charge_at))
    if kind not in ('enrollment', 'daily', 'daily_resume', 'tariff_switch') or end - start != timedelta(days=1):
        raise LedgerConflict('Daily funding requires an explicit one-day purchase')
    if not isinstance(source, dict) or source.get('kind') != kind:
        raise LedgerConflict('Invalid daily funding source')
    version = _integer(sub.daily_devices_billing_version)
    if version not in (0, 1):
        raise LedgerConflict('Unknown daily device billing version')
    return dict(
        version=1,
        tariff_id=_integer(sub.tariff_id, minimum=1),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        last_daily_charge_at=charged_at.isoformat(),
        full_rate_kopeks=_integer(full_rate_kopeks),
        device_limit=_integer(sub.device_limit),
        daily_devices_billing_version=version,
        source=dict(source),
    )


def renewal_daily_funding(*, sub, state, kind, operation_key, total, now, new_end):
    snapshot = build_daily_funding(
        sub=sub,
        kind=kind,
        full_rate_kopeks=total,
        window_start=now,
        window_end=new_end,
        last_daily_charge_at=now,
        source=dict(
            kind=kind,
            operation_key=operation_key,
            user_id=sub.user_id,
            subscription_id=sub.id,
            ownership_cycle=state.ownership_cycle,
            account_generation=state.account_generation,
            panel_scope=state.panel_scope,
        ),
    )
    return {'daily_funding': snapshot, 'daily_funding_hash': terms_hash(snapshot)}


def enrollment_daily_funding(*, sub, transaction):
    """Caller proves duration=1 and absence of extension/bonus before calling."""
    return build_daily_funding(
        sub=sub,
        kind='enrollment',
        full_rate_kopeks=-transaction.amount_kopeks,
        window_start=_date(sub.end_date) - timedelta(days=1),
        window_end=sub.end_date,
        last_daily_charge_at=sub.last_daily_charge_at,
        source=dict(kind='enrollment', transaction_id=transaction.id, user_id=sub.user_id, subscription_id=sub.id),
    )


def _read(snapshot):
    if not isinstance(snapshot, dict) or type(snapshot.get('version')) is not int or snapshot['version'] != 1:
        raise LedgerConflict('Missing daily funding snapshot')
    for field in ('tariff_id', 'full_rate_kopeks', 'device_limit', 'daily_devices_billing_version'):
        _integer(snapshot.get(field), minimum=1 if field == 'tariff_id' else 0)
    if not isinstance(snapshot.get('source'), dict):
        raise LedgerConflict('Missing daily funding source')
    if snapshot['daily_devices_billing_version'] not in (0, 1):
        raise LedgerConflict('Unknown daily device billing version')
    start, end, charged = (
        _date(snapshot.get(field)) for field in ('window_start', 'window_end', 'last_daily_charge_at')
    )
    if end - start != timedelta(days=1):
        raise LedgerConflict('Daily funding window is not one day')
    return start, end, charged


async def resolve_daily_funding(db, *, state, sub):
    """Caller holds User→Subscription→ledger locks. Latest invalid proof blocks fallback."""
    if state.subscription_id != sub.id or sub.is_trial:
        raise LedgerConflict('Daily funding ownership changed')
    floor = await renewal_floor(db, state)
    receipt = await db.scalar(
        select(BsTrafficRenewalIntent)
        .where(
            BsTrafficRenewalIntent.subscription_id == sub.id,
            BsTrafficRenewalIntent.id > floor,
            BsTrafficRenewalIntent.transaction_id.is_not(None),
        )
        .order_by(BsTrafficRenewalIntent.id.desc())
        .limit(1)
    )
    chain = []
    projected = SimpleNamespace(
        **{
            field: getattr(sub, field)
            for field in (
                'id',
                'user_id',
                'is_trial',
                'tariff_id',
                'end_date',
                'last_daily_charge_at',
                'device_limit',
                'daily_devices_billing_version',
            )
        }
    )
    while receipt is not None and receipt.parameters.get('kind') == 'tariff_switch':
        if len(chain) >= 64:
            raise LedgerConflict('Daily switch proof exceeds bounded history')
        snapshot = receipt.parameters.get('daily_funding')
        start, end, charged = _read(snapshot)
        terms = receipt.parameters.get('switch_terms')
        daily = terms.get('daily') if isinstance(terms, dict) else None
        source = snapshot['source']
        if (
            not isinstance(daily, dict)
            or terms_hash(terms) != receipt.parameters.get('switch_terms_hash')
            or terms_hash(snapshot) != receipt.parameters.get('daily_funding_hash')
            or source.get('kind') != 'tariff_switch'
            or source.get('operation_key') != receipt.operation_key
            or source.get('user_id') != sub.user_id
            or source.get('subscription_id') != sub.id
            or source.get('ownership_cycle') != state.ownership_cycle
            or source.get('account_generation') != state.account_generation
            or source.get('panel_scope') != state.panel_scope
            or receipt.subscription_id != sub.id
            or receipt.id <= floor
            or receipt.account_generation != state.account_generation
            or receipt.panel_scope != state.panel_scope
            or terms.get('ownership_cycle') != state.ownership_cycle
            or terms.get('account_generation') != state.account_generation
            or snapshot['tariff_id'] != projected.tariff_id
            or terms.get('tariff_id') != projected.tariff_id
            or receipt.parameters.get('tariff_id') != projected.tariff_id
            or snapshot['device_limit'] != projected.device_limit
            or terms.get('device_limit') != projected.device_limit
            or snapshot['daily_devices_billing_version'] != projected.daily_devices_billing_version
            or end != projected.end_date
            or charged != projected.last_daily_charge_at
            or receipt.intended_expires_at != end
            or _date(terms.get('end_date')) != end
            or _date(receipt.parameters.get('old_end_date')) != end
            or daily.get('target_full_rate_kopeks') != snapshot['full_rate_kopeks']
            or daily.get('window_start') != snapshot['window_start']
            or daily.get('window_end') != snapshot['window_end']
            or daily.get('last_daily_charge_at') != snapshot['last_daily_charge_at']
            or daily.get('daily_devices_billing_version') != projected.daily_devices_billing_version
            or source.get('previous') != daily.get('source')
            or source.get('root') != daily.get('root')
        ):
            raise LedgerConflict('Daily switch proof changed')
        priced_at = _date(receipt.parameters.get('daily_priced_at'))
        amount = prorated_daily_charge(
            daily.get('source_full_rate_kopeks'), snapshot['full_rate_kopeks'], end, priced_at
        )
        remaining = end - priced_at
        remaining_us = (remaining.days * 86400 + remaining.seconds) * 1000000 + remaining.microseconds
        if (
            not start <= priced_at < end
            or amount != terms.get('price')
            or receipt.parameters.get('daily_remaining_us') != remaining_us
        ):
            raise LedgerConflict('Daily switch price proof changed')
        await _validate_switch_payment(db, receipt, sub.user_id, amount)
        previous = daily.get('source')
        if not isinstance(previous, dict):
            raise LedgerConflict('Missing preceding daily proof')
        latest_previous = await db.scalar(
            select(BsTrafficRenewalIntent.id)
            .where(
                BsTrafficRenewalIntent.subscription_id == sub.id,
                BsTrafficRenewalIntent.id > floor,
                BsTrafficRenewalIntent.id < receipt.id,
                BsTrafficRenewalIntent.transaction_id.is_not(None),
            )
            .order_by(BsTrafficRenewalIntent.id.desc())
            .limit(1)
        )
        if (previous.get('kind') == 'enrollment' and latest_previous is not None) or (
            previous.get('kind') != 'enrollment' and latest_previous != previous.get('receipt_id')
        ):
            raise LedgerConflict('Daily proof skips the preceding financial receipt')
        chain.append((receipt, snapshot, previous, daily))
        projected.tariff_id = _integer(terms.get('current_tariff_id'), minimum=1)
        projected.device_limit = _integer(daily.get('source_device_limit'))
        if previous.get('kind') == 'enrollment':
            receipt = None
        elif previous.get('kind') in ('daily', 'daily_resume', 'tariff_switch'):
            previous_id = _integer(previous.get('receipt_id'), minimum=1)
            if not floor < previous_id < receipt.id:
                raise LedgerConflict('Invalid daily proof ordering')
            receipt = await db.get(BsTrafficRenewalIntent, previous_id)
            if receipt is None or receipt.subscription_id != sub.id:
                raise LedgerConflict('Missing owned daily source')
        else:
            raise LedgerConflict('Unknown daily source kind')
    result = await _resolve_root(db, state=state, sub=projected, receipt=receipt)
    for item, snapshot, previous, daily in reversed(chain):
        if previous != funding_identity(result) or daily['source_full_rate_kopeks'] != result.full_rate_kopeks:
            raise LedgerConflict('Daily switch source binding changed')
        if snapshot['source'].get('root') != funding_root(result):
            raise LedgerConflict('Daily switch root binding changed')
        result = _result(snapshot, item, 'tariff_switch', root=funding_root(result))
    return result


def funding_identity(funding):
    return dict(
        kind=funding.source_kind,
        receipt_id=funding.receipt_id,
        transaction_id=funding.transaction_id,
        digest=funding.digest,
    )


def funding_root(funding):
    return funding.root_source or funding_identity(funding)


def prorated_daily_charge(source_rate, target_rate, end, now):
    source_rate, target_rate = _integer(source_rate), _integer(target_rate)
    remaining = _date(end) - _date(now)
    remaining_us = (remaining.days * 86400 + remaining.seconds) * 1000000 + remaining.microseconds
    if not 0 < remaining_us <= 86_400_000_000:
        raise LedgerConflict('Paid daily window expired or invalid')
    return max(0, target_rate - source_rate) * remaining_us // 86_400_000_000


async def _validate_switch_payment(db, receipt, user_id, amount):
    payment = await db.get(Transaction, receipt.transaction_id)
    expected = dict(method='balance', external_id=receipt.operation_key, amount_kopeks=-amount)
    if (
        payment is None
        or payment.user_id != user_id
        or not payment.is_completed
        or payment.type != 'subscription_payment'
        or payment.payment_method != 'balance'
        or payment.external_id != receipt.operation_key
        or payment.amount_kopeks != -amount
        or receipt.charged_kopeks != amount
        or receipt.parameters.get('total_amount_kopeks') != amount
        or receipt.parameters.get('period_days') != 0
        or receipt.parameters.get('payment_receipt') != expected
    ):
        raise LedgerConflict('Daily switch payment changed')


def _result(snapshot, receipt, kind, *, root=None):
    start, end, charged = _read(snapshot)
    return DailyFunding(
        snapshot['tariff_id'],
        start,
        end,
        charged,
        snapshot['full_rate_kopeks'],
        snapshot['device_limit'],
        snapshot['daily_devices_billing_version'],
        kind,
        receipt.id,
        receipt.transaction_id,
        terms_hash(snapshot),
        root,
    )


async def _resolve_root(db, *, state, sub, receipt):
    if receipt is None:
        receipt = await db.scalar(
            select(BsTrafficEnrollment)
            .where(
                BsTrafficEnrollment.subscription_id == sub.id,
                BsTrafficEnrollment.ownership_cycle == state.ownership_cycle,
            )
            .order_by(BsTrafficEnrollment.id.desc())
            .limit(1)
        )
        if receipt is None or receipt.user_id != sub.user_id or receipt.source_kind != 'payment':
            raise LedgerConflict('No proven daily enrollment funding')
        await validate_funding(db, receipt)
        snapshot = receipt.terms.get('daily_funding')
        _read(snapshot)
        if (
            snapshot['source']
            != dict(
                kind='enrollment', transaction_id=receipt.transaction_id, user_id=sub.user_id, subscription_id=sub.id
            )
            or snapshot['full_rate_kopeks'] != -receipt.terms['source']['amount_kopeks']
            or _date(receipt.terms.get('paid_until')) != sub.end_date
            or receipt.tariff_id != sub.tariff_id
        ):
            raise LedgerConflict('Daily enrollment funding changed')
        kind = 'enrollment'
    else:
        values = receipt.parameters
        kind = values.get('kind')
        total = _integer(values.get('total_amount_kopeks'))
        if kind not in ('daily', 'daily_resume') or values.get('period_days') != 1 or receipt.charged_kopeks != total:
            raise LedgerConflict('Latest receipt does not prove a full daily rate')
        payment = await db.get(Transaction, receipt.transaction_id)
        expected = dict(method='balance', external_id=receipt.operation_key, amount_kopeks=-total)
        if (
            payment is None
            or payment.user_id != sub.user_id
            or not payment.is_completed
            or payment.type != 'subscription_payment'
            or payment.payment_method != 'balance'
            or payment.external_id != receipt.operation_key
            or payment.amount_kopeks != -total
            or values.get('payment_receipt', expected) != expected
            or values.get('tariff_id') != sub.tariff_id
            or receipt.intended_expires_at != sub.end_date
        ):
            raise LedgerConflict('Daily payment identity changed')
        start = _date(receipt.intended_expires_at) - timedelta(days=1)
        old_end = _date(values.get('old_end_date'))
        if kind == 'daily':
            if _date(values.get('expected_due_at')) != old_end or old_end > start:
                raise LedgerConflict('Daily due window changed')
        else:
            terms = values.get('daily_resume_terms')
            if (
                not isinstance(terms, dict)
                or terms_hash(terms) != values.get('daily_resume_terms_hash')
                or terms.get('price_kopeks') != total
                or terms.get('duration_days') != 1
                or terms.get('tariff_id') != sub.tariff_id
                or terms.get('account_generation') != state.account_generation
                or terms.get('ownership_cycle') != state.ownership_cycle
                or _date(terms.get('old_end_date')) != old_end
                or old_end >= receipt.intended_expires_at
                or terms.get('device_limit') != sub.device_limit
                or terms.get('daily_devices_billing_version') != sub.daily_devices_billing_version
            ):
                raise LedgerConflict('Daily resume terms changed')
        expected_source = dict(
            kind=kind,
            operation_key=receipt.operation_key,
            user_id=sub.user_id,
            subscription_id=sub.id,
            ownership_cycle=state.ownership_cycle,
            account_generation=state.account_generation,
            panel_scope=state.panel_scope,
        )
        snapshot = values.get('daily_funding')
        if snapshot is None:
            if 'daily_funding' in values or 'daily_funding_hash' in values or sub.last_daily_charge_at != start:
                raise LedgerConflict('Legacy daily window is not proven')
            snapshot = build_daily_funding(
                sub=sub,
                kind=kind,
                full_rate_kopeks=total,
                window_start=start,
                window_end=receipt.intended_expires_at,
                last_daily_charge_at=start,
                source=expected_source,
            )
        elif not isinstance(snapshot, dict):
            raise LedgerConflict('Invalid daily funding snapshot')
        elif (
            terms_hash(snapshot) != values.get('daily_funding_hash')
            or snapshot.get('source') != expected_source
            or snapshot.get('full_rate_kopeks') != total
        ):
            raise LedgerConflict('Daily funding snapshot changed')
    start, end, charged = _read(snapshot)
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        receipt.account_generation != state.account_generation
        or receipt.panel_scope != state.panel_scope
        or snapshot['tariff_id'] != sub.tariff_id
        or end != sub.end_date
        or charged != sub.last_daily_charge_at
        or not start <= now < end
        or snapshot['device_limit'] != sub.device_limit
        or snapshot['daily_devices_billing_version'] != sub.daily_devices_billing_version
        or (kind != 'enrollment' and charged != start)
    ):
        raise LedgerConflict('Daily funding no longer matches the paid subscription')
    return DailyFunding(
        snapshot['tariff_id'],
        start,
        end,
        charged,
        snapshot['full_rate_kopeks'],
        snapshot['device_limit'],
        snapshot['daily_devices_billing_version'],
        kind,
        receipt.id,
        receipt.transaction_id,
        terms_hash(snapshot),
    )


def purchase_daily_funding(*, sub, tariff, transaction, period_days, checkout_started_at, checkout_finished_at):
    """Attest only a one-day checkout whose new window starts during this write.

    Extension and carried bonus windows cannot satisfy this envelope. The
    independently recorded charge timestamp is preserved, not used as expiry.
    """
    if not tariff.is_daily or type(period_days) is not int or period_days != 1:
        return None
    # Older cart checkouts can sell a daily tariff without recording its charge
    # anchor. They remain successful purchases, but cannot attest a funded day.
    if sub.last_daily_charge_at is None:
        return None
    start = _date(sub.end_date) - timedelta(days=1)
    if not _date(checkout_started_at) <= start <= _date(checkout_finished_at):
        return None
    return enrollment_daily_funding(sub=sub, transaction=transaction)
