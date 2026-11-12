"""Replenish a paid calendar interval without charging or extending paid time.

Late scheduling grants only the current interval. Immutable grant starts keep
older or mixed usage out of this interval; the ordinary intake resolves it.
The caller commits calendar position and both groups together, then delivers
the existing access projection. No network or public enrollment happens here.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficEnrollment,
    BsTrafficRenewalIntent,
    Subscription,
    Transaction,
    User,
)
from app.services.bs_traffic.base_renewal import reset_renewal_bases
from app.services.bs_traffic.ledger import LedgerConflict, _amount, _lock, require_activated_owner
from app.services.bs_traffic.policy import _read_policy
from app.services.traffic_accounting_schedule import next_base_refresh


@dataclass(frozen=True)
class CalendarResult:
    status: str
    grants: dict[str, int] = field(default_factory=dict)
    starts_at: datetime | None = None
    next_refresh_at: datetime | None = None


def _date(value):
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result
    except (TypeError, ValueError):
        raise LedgerConflict('Calendar receipt requires aware timestamps') from None


def current_interval(strategy, *, now, anchor, timezone):
    if strategy not in ('DAY', 'WEEK', 'MONTH', 'MONTH_ROLLING'):
        raise LedgerConflict('Unsupported calendar strategy')
    # Constant work even after years offline; 40 days spans every supported
    # calendar period (including month ends and DST), at most 42 DAY steps.
    cursor = next_base_refresh(strategy, now=now - timedelta(days=40), anchor=anchor, timezone=timezone)
    previous = None
    for _ in range(42):
        if cursor > now:
            if previous is None:
                raise LedgerConflict('Calendar interval precedes its anchor')
            return previous, cursor
        previous = cursor
        cursor = next_base_refresh(strategy, now=cursor, anchor=anchor, timezone=timezone)
    raise LedgerConflict('Calendar boundary inspection budget exceeded')


async def refresh_calendar_base(db, *, user_id, subscription_id, account_generation, now=None):
    """A trusted scheduler supplies time only for deterministic internal tests."""
    async with db.begin_nested():
        state, _ = await _lock(db, user_id, subscription_id, account_generation)
        await require_activated_owner(db, subscription_id)
        moment = now or await db.scalar(select(func.clock_timestamp()))
        if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() is None:
            raise LedgerConflict('Aware calendar time required')
        sub = await db.get(Subscription, subscription_id, populate_existing=True)
        user = await db.get(User, user_id, populate_existing=True)
        if sub.end_date is None or sub.end_date <= moment:
            return CalendarResult('expired')
        if user.status != 'active' or sub.status not in ('active', 'trial', 'limited'):
            return CalendarResult('inactive')
        from app.services.bs_traffic.ownership_cycle import renewal_floor

        receipt_floor = await renewal_floor(db, state)
        receipt = await db.scalar(
            select(BsTrafficRenewalIntent)
            .where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.transaction_id.is_not(None),
                BsTrafficRenewalIntent.id > receipt_floor,
            )
            .order_by(BsTrafficRenewalIntent.id.desc())
            .limit(1)
        )
        policy = _read_policy(state.access_policy)
        target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
        result = await _refresh_receipt(db, receipt, state, sub, moment, {'ordinary', 'bs'}, **target)
        # An ordinary-only renewal cannot revoke the earlier paid BS calendar.
        # Select its latest funded terms, never infer them from today's tariff.
        if (
            receipt is not None
            and 'bs' not in (receipt.parameters or {}).get('amounts', {})
            and policy['bs_mode'] == 'finite'
        ):
            older = await db.scalar(
                select(BsTrafficRenewalIntent)
                .where(
                    BsTrafficRenewalIntent.subscription_id == subscription_id,
                    BsTrafficRenewalIntent.transaction_id.is_not(None),
                    BsTrafficRenewalIntent.id < receipt.id,
                    BsTrafficRenewalIntent.id > receipt_floor,
                    BsTrafficRenewalIntent.parameters['amounts']['bs'].as_string().is_not(None),
                )
                .order_by(BsTrafficRenewalIntent.id.desc())
                .limit(1)
            )
            if older is None:
                older = await db.scalar(
                    select(BsTrafficEnrollment).where(
                        BsTrafficEnrollment.subscription_id == subscription_id,
                        BsTrafficEnrollment.ownership_cycle == state.ownership_cycle,
                        BsTrafficEnrollment.calendar_terms.is_not(None),
                    )
                )
            extra = await _refresh_receipt(db, older, state, sub, moment, {'bs'}, **target)
            following = [date for date in (result.next_refresh_at, extra.next_refresh_at) if date is not None]
            result = CalendarResult(
                'refreshed' if extra.grants else result.status,
                result.grants | extra.grants,
                result.starts_at or extra.starts_at,
                min(following) if following else None,
            )
        if receipt is None and policy['bs_mode'] == 'finite':
            enrollment = await db.scalar(
                select(BsTrafficEnrollment).where(
                    BsTrafficEnrollment.subscription_id == subscription_id,
                    BsTrafficEnrollment.ownership_cycle == state.ownership_cycle,
                    BsTrafficEnrollment.calendar_terms.is_not(None),
                )
            )
            if enrollment is not None:
                extra = await _refresh_receipt(db, enrollment, state, sub, moment, {'bs'}, **target)
                following = [date for date in (result.next_refresh_at, extra.next_refresh_at) if date is not None]
                result = CalendarResult(
                    'refreshed' if extra.grants else result.status,
                    result.grants | extra.grants,
                    result.starts_at or extra.starts_at,
                    min(following) if following else None,
                )
        return result


async def _refresh_receipt(db, receipt, state, sub, moment, groups, *, user_id, subscription_id, account_generation):
    enrolled = isinstance(receipt, BsTrafficEnrollment)
    initial = receipt is None
    if initial:
        from app.services.bs_traffic.ownership_cycle import current_transfer

        receipt = await current_transfer(db, subscription_id)
        if receipt is None or receipt.calendar_terms is None:
            return CalendarResult('no_paid_calendar')
    values = receipt.calendar_terms if initial or enrolled else receipt.parameters
    if (
        not isinstance(values, dict)
        or receipt.account_generation != account_generation
        or receipt.panel_scope != state.panel_scope
        or values.get('tariff_id') != sub.tariff_id
    ):
        raise LedgerConflict('Paid calendar binding changed')
    paid_until = min(
        sub.end_date, _date(values.get('paid_until')) if initial or enrolled else receipt.intended_expires_at
    )
    if paid_until <= moment:
        return CalendarResult('expired')
    if initial and sub.is_trial:
        raise LedgerConflict('Transferred paid calendar cannot finance a trial')
    strategy = values.get('calendar_strategy')
    if strategy == 'NO_RESET' or values.get('next_refresh_at') is None:
        return CalendarResult('not_due')
    due = _date(values['next_refresh_at'])
    if due > moment:
        return CalendarResult('not_due', next_refresh_at=due if due < paid_until else None)
    if enrolled:
        from app.services.bs_traffic.enrollment import validate_funding

        await validate_funding(db, receipt)
        frozen = receipt.terms
        if (
            values.get('paid_until') != frozen.get('paid_until')
            or values.get('amounts') != ({'bs': frozen['bs_limit_bytes']} if frozen['bs_limit_bytes'] else {})
            or values.get('calendar_strategy') != frozen.get('calendar_strategy')
            or values.get('calendar_timezone') != frozen.get('calendar_timezone')
        ):
            raise LedgerConflict('Enrollment calendar terms changed')
    elif not initial:
        payment = await db.get(Transaction, receipt.transaction_id)
        expected = values.get('payment_receipt') or {
            'method': 'balance',
            'external_id': receipt.operation_key,
            'amount_kopeks': -receipt.charged_kopeks,
        }
        if (
            not isinstance(expected, dict)
            or payment is None
            or payment.user_id != user_id
            or payment.type != 'subscription_payment'
            or not payment.is_completed
            or payment.external_id != expected.get('external_id')
            or payment.payment_method != expected.get('method')
            or payment.amount_kopeks != expected.get('amount_kopeks')
        ):
            raise LedgerConflict('Paid calendar receipt is invalid')
    policy = _read_policy(state.access_policy)
    if initial:
        from app.services.bs_traffic.initial_calendar import validate_initial_calendar

        await validate_initial_calendar(db, receipt, values, policy=policy)
    try:
        timezone = ZoneInfo(values['calendar_timezone'])
    except (KeyError, ValueError, TypeError) as error:
        raise LedgerConflict('Paid calendar policy changed') from error
    anchor = _date(values.get('calendar_anchor'))
    start, following = current_interval(strategy, now=moment, anchor=anchor, timezone=timezone)
    if start < due:
        raise LedgerConflict('Paid calendar position is inconsistent')
    raw = values.get('amounts')
    if not isinstance(raw, dict) or not set(raw) <= {'ordinary', 'bs'}:
        raise LedgerConflict('Paid calendar amounts missing')
    if not groups.intersection(raw):
        return CalendarResult('no_paid_calendar')
    amounts = {}
    for group, amount in raw.items():
        _amount(amount)
        if policy[f'{group}_mode'] != 'finite':
            raise LedgerConflict('Paid calendar group mode changed')
        if group in groups:
            amounts[group] = amount
    if (
        not initial
        and not enrolled
        and 'ordinary' in groups
        and policy['ordinary_mode'] == 'finite'
        and 'ordinary' not in amounts
    ):
        raise LedgerConflict('Ordinary paid calendar amount missing')
    end = min(following, paid_until)
    calendar_identity = (
        f'transfer:{receipt.manifest_hash}' if initial else f'enrollment:{receipt.id}' if enrolled else str(receipt.id)
    )
    grants = (
        await reset_renewal_bases(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            operation_key=f'calendar:{calendar_identity}:{start.isoformat()}',
            amounts=amounts,
            starts_at=start,
            expires_at=end,
            reset_base=True,
            settle_before_grant=False,
            funded_calendar=True,
        )
        if amounts
        else {}
    )
    updated = dict(
        values,
        next_refresh_at=following.isoformat() if following < paid_until else None,
        calendar_last_boundary=start.isoformat(),
    )
    if initial or enrolled:
        receipt.calendar_terms = updated
    else:
        receipt.parameters = updated
    await db.flush()
    return CalendarResult('refreshed', grants, start, following if following < paid_until else None)
