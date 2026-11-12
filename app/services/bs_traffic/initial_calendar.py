"""Retain the verified paid source calendar without creating a payment receipt."""

from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.database.models import Subscription, TrafficAccountingState, TrafficAllowance
from app.services.bs_traffic.ledger import LedgerConflict, _amount
from app.services.traffic_accounting_schedule import next_base_refresh


async def capture_initial_calendar(db, *, manifest, source, policy, now):
    """Called only inside the locked, verified managed transfer transaction."""
    sub = await db.get(Subscription, manifest.subscription_id)
    if sub.is_trial or manifest.paid_until is None or manifest.paid_until <= now:
        return None
    group = manifest.destination_group
    if (policy is not None and policy[f'{group}_mode'] != 'finite') or manifest.source_phase == 'unlimited':
        return None
    original = sum(item.original_bytes for item in manifest.grants if item.source_key.startswith('base:'))
    budget = source.base_budget_bytes or 0
    _amount(budget, zero=True)
    if original == 0 and budget > 0:
        # Zero remainders are not imported, but the retained source can prove
        # an exhausted current paid base. Never infer it from today's tariff.
        spent = list(
            await db.scalars(
                select(TrafficAllowance)
                .where(
                    TrafficAllowance.subscription_id == manifest.subscription_id,
                    TrafficAllowance.operation_key.startswith('base:'),
                    TrafficAllowance.retired_at.is_(None),
                    TrafficAllowance.expires_at > now,
                )
                .limit(2)
            )
        )
        if len(spent) == 1 and spent[0].remaining_bytes == 0 and spent[0].granted_bytes == budget:
            original = budget
    if original == 0:
        return None
    _amount(original)
    if budget and budget != original:
        raise LedgerConflict('Transferred calendar base budget is ambiguous')
    try:
        following = manifest.next_refresh_at or next_base_refresh(
            manifest.business_strategy,
            now=now,
            anchor=manifest.calendar_anchor,
            timezone=ZoneInfo(manifest.calendar_timezone),
        )
    except (KeyError, ValueError) as error:
        raise LedgerConflict('Transferred calendar is invalid') from error
    if following is not None and following <= now:
        raise LedgerConflict('Transferred calendar refresh is already due')
    return {
        'version': 1,
        'tariff_id': manifest.tariff_id,
        'paid_until': manifest.paid_until.isoformat(),
        'captured_at': now.isoformat(),
        'amounts': {group: original},
        'destination_group': group,
        'manifest_hash': manifest.fingerprint,
        'calendar_strategy': manifest.business_strategy,
        'calendar_timezone': manifest.calendar_timezone,
        'calendar_anchor': manifest.calendar_anchor.isoformat(),
        'source_next_refresh_at': manifest.next_refresh_at.isoformat() if manifest.next_refresh_at else None,
        'source_budget_bytes': budget,
        'topology': policy['topology'] if policy is not None else None,
        'next_refresh_at': following.isoformat() if following else None,
    }


async def validate_initial_calendar(db, transfer, values, *, policy):
    """Source history remains read-only after transfer; a changed source is unsafe."""
    source = await db.get(TrafficAccountingState, transfer.subscription_id, populate_existing=True)
    if (
        not isinstance(values, dict)
        or values.get('version') != 1
        or source is None
        or values.get('manifest_hash') != transfer.manifest_hash
        or values.get('destination_group') != transfer.destination_group
        or (values.get('topology') is not None and values['topology'] != policy['topology'])
        or set(values.get('amounts', {})) != {transfer.destination_group}
        or source.business_strategy != values.get('calendar_strategy')
        or source.calendar_timezone != values.get('calendar_timezone')
        or source.calendar_anchor is None
        or source.calendar_anchor.isoformat() != values.get('calendar_anchor')
        or (source.next_refresh_at.isoformat() if source.next_refresh_at else None)
        != values.get('source_next_refresh_at')
        or (source.base_budget_bytes or 0) != values.get('source_budget_bytes')
    ):
        raise LedgerConflict('Transferred paid calendar source changed')
