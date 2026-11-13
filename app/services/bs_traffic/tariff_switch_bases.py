"""Replace only base provenance and carry confirmed usage across tariff changes."""

from datetime import datetime

from sqlalchemy import or_, select

from app.database.models import BsTrafficAllowance, BsTrafficLedger, BsTrafficManagedTransfer, BsTrafficRenewalIntent
from app.services.bs_traffic.base_extension import covers_interval
from app.services.bs_traffic.base_renewal import reset_renewal_bases
from app.services.bs_traffic.ledger import LedgerConflict


async def switch_bases(
    db, *, user_id, subscription_id, account_generation, operation_key, amounts, starts_at, expires_at, reset
):
    """Caller holds ownership locks and has settled old finite groups.

    Target policy and entry baselines must be saved in the same transaction.
    Carry contains confirmed consumption only, never unresolved usage.
    """
    transfer = await db.get(BsTrafficManagedTransfer, subscription_id)
    imported = {}
    if transfer is not None:
        if transfer.account_generation != account_generation or not isinstance(transfer.grants, list):
            raise LedgerConflict('Invalid imported base provenance')
        for item in transfer.grants:
            if item['source_key'].startswith('base:'):
                imported[item['grant_id']] = item['original_bytes']
    bases = list(
        await db.scalars(
            select(BsTrafficAllowance)
            .where(
                BsTrafficAllowance.subscription_id == subscription_id,
                BsTrafficAllowance.retired_at.is_(None),
                covers_interval(BsTrafficAllowance, starts_at, starts_at, instant=True),
                or_(BsTrafficAllowance.operation_key.startswith('base:'), BsTrafficAllowance.id.in_(imported)),
            )
            .limit(201)
        )
    )
    if len(bases) > 200:
        raise LedgerConflict('Base inspection budget exceeded')
    # A later no-reset renewal references the SAME base. Its receipt must not
    # hide the excess consumption saved when that base was first created.
    base_ids = [base.id for base in bases]
    previous = list(
        await db.scalars(
            select(BsTrafficRenewalIntent)
            .where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.account_generation == account_generation,
                BsTrafficRenewalIntent.parameters['kind'].as_string() == 'tariff_switch',
                or_(
                    *(
                        BsTrafficRenewalIntent.parameters['grants'][group].as_integer().in_(base_ids)
                        for group in ('ordinary', 'bs')
                    )
                ),
            )
            .limit(201)
        )
    )
    if len(previous) > 200:
        raise LedgerConflict('Base consumption provenance exceeds inspection budget')
    saved_excess = {}
    for receipt in previous:
        for group, identifier in receipt.parameters['grants'].items():
            value = receipt.parameters.get('consumption_excess_bytes', {}).get(group, 0)
            if type(value) is not int or value < 0:
                raise LedgerConflict('Invalid saved base consumption')
            saved_excess[identifier] = value
    consumed = dict.fromkeys(('ordinary', 'bs'), 0)
    ledger = await db.get(BsTrafficLedger, subscription_id)
    latest = await db.scalar(
        select(BsTrafficRenewalIntent)
        .where(
            BsTrafficRenewalIntent.subscription_id == subscription_id,
            BsTrafficRenewalIntent.account_generation == account_generation,
            BsTrafficRenewalIntent.parameters['kind'].as_string() == 'tariff_switch',
            BsTrafficRenewalIntent.parameters['switch_terms']['ownership_cycle'].as_integer() == ledger.ownership_cycle,
            BsTrafficRenewalIntent.transaction_id.is_not(None),
        )
        .order_by(BsTrafficRenewalIntent.id.desc())
        .limit(1)
    )
    if latest is not None:
        for group, value in latest.parameters.get('mode_consumption', {}).items():
            if group not in consumed or type(value.get('bytes')) is not int or value['bytes'] < 0:
                raise LedgerConflict('Invalid mode consumption provenance')
            if datetime.fromisoformat(value['expires_at']) > starts_at:
                consumed[group] = value['bytes']
    for group in ('ordinary', 'bs'):
        current = [base for base in bases if base.traffic_group == group]
        if current:
            consumed[group] = sum(
                imported.get(base.id, base.granted_bytes) - base.remaining_bytes + saved_excess.get(base.id, 0)
                for base in current
            )
    if reset:
        consumed = dict.fromkeys(consumed, 0)
    # The coordinator settled only old finite groups before changing policy.
    grants = (
        await reset_renewal_bases(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            operation_key=operation_key,
            amounts=amounts,
            starts_at=starts_at,
            expires_at=expires_at,
            reset_base=True,
            settle_before_grant=False,
        )
        if amounts
        else {}
    )
    excess = {}
    if not reset:
        for group, identifier in grants.items():
            row = await db.get(BsTrafficAllowance, identifier)
            row.remaining_bytes = max(0, row.granted_bytes - consumed[group])
            excess[group] = max(0, consumed[group] - row.granted_bytes)
    for base in bases:
        if base.traffic_group not in amounts:
            base.retired_at = starts_at
    await db.flush()
    carry = {group: {'bytes': value, 'expires_at': expires_at.isoformat()} for group, value in consumed.items()}
    return grants, excess, carry
