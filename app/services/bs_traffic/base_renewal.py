"""Transactional base refresh for paid split renewal, not a checkout endpoint.

The caller supplies persisted entitlement terms, owns payment/expiry commit and
must schedule panel delivery. This primitive grants no new subscription days.
Only base provenance is replaceable: independent top-ups retain their terms.
"""

from sqlalchemy import or_, select

from app.database.models import BsTrafficAllowance as Allowance, BsTrafficBaseExtension, BsTrafficManagedTransfer
from app.services.bs_traffic.base_extension import extend_base_group, operation_identity, operation_variants
from app.services.bs_traffic.intake import grant_with_reported_settlement
from app.services.bs_traffic.ledger import (
    LedgerConflict,
    _amount,
    _group,
    _interval,
    _key,
    _lock,
    grant,
    require_activated_owner,
)
from app.services.bs_traffic.policy import _read_policy


async def reset_base_group(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    group,
    operation_key,
    amount_bytes,
    starts_at,
    expires_at,
    reset_base=True,
    settle_before_grant=True,
    funded_calendar=False,
):
    """Replace one finite group's base, atomically and idempotently.

    Old balances are retired, not deleted: pending historical usage retains
    its frozen eligible grants. Conflicts roll back the complete refresh.
    Unlimited/mode changes and non-reset renewals belong to the coordinator.
    """
    if type(reset_base) is not bool:
        raise LedgerConflict('Explicit base reset decision required')
    if type(settle_before_grant) is not bool:
        raise LedgerConflict('Explicit settlement ordering required')
    _key(operation_key)
    if type(funded_calendar) is not bool or (funded_calendar and not operation_key.startswith('calendar:')):
        raise LedgerConflict('Explicit funded calendar operation required')
    _group(group)
    _amount(amount_bytes)
    _interval(starts_at, expires_at)
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    if not reset_base:
        return await extend_base_group(
            db,
            **target,
            group=group,
            operation_key=operation_key,
            amount_bytes=amount_bytes,
            starts_at=starts_at,
            expires_at=expires_at,
        )
    key = operation_identity(operation_key)
    terms = dict(group=group, operation_key=key, amount_bytes=amount_bytes, starts_at=starts_at, expires_at=expires_at)
    async with db.begin_nested():
        state, _ = await _lock(db, **target)
        await require_activated_owner(db, subscription_id)
        if (
            await db.scalar(
                select(BsTrafficBaseExtension.id).where(
                    BsTrafficBaseExtension.subscription_id == subscription_id,
                    BsTrafficBaseExtension.traffic_group == group,
                    BsTrafficBaseExtension.operation_key == key,
                )
            )
            is not None
        ):
            raise LedgerConflict('Renewal operation already extended its shared base')
        existing = await db.scalar(
            select(Allowance).where(
                Allowance.subscription_id == subscription_id,
                Allowance.traffic_group == group,
                Allowance.operation_key.in_(operation_variants(operation_key)),
            )
        )
        if existing is not None:
            # Never retire a later base when replaying an earlier paid receipt.
            if existing.operation_key == operation_variants(operation_key)[2]:
                raise LedgerConflict('Legacy no-reset receipt requires reconciliation')
            return await grant(db, **target, **(terms | {'operation_key': existing.operation_key}))
        policy = _read_policy(state.access_policy)
        stopped = group == 'bs' and policy.get('bs_purchase_enabled') is False
        if stopped and funded_calendar:
            from datetime import datetime

            deadline = datetime.fromisoformat(policy['bs_drain_until'])
            if expires_at > deadline or not operation_key.startswith('calendar:'):
                raise LedgerConflict('Calendar grant exceeds previously funded BS horizon')
        if policy[f'{group}_mode'] != 'finite' or (stopped and not funded_calendar):
            raise LedgerConflict('Group does not accept new finite base grants')
        transfer = await db.get(BsTrafficManagedTransfer, subscription_id)
        imported = []
        if transfer is not None and transfer.destination_group == group:
            if transfer.account_generation != account_generation:
                raise LedgerConflict('Imported base belongs to another account generation')
            if not isinstance(transfer.grants, list) or len(transfer.grants) > 200:
                raise LedgerConflict('Imported base provenance is invalid or exceeds inspection budget')
            for item in transfer.grants:
                if not isinstance(item, dict) or not isinstance(item.get('source_key'), str):
                    raise LedgerConflict('Imported base provenance is invalid')
                if item['source_key'].startswith('base:'):
                    if type(item.get('grant_id')) is not int or item['grant_id'] <= 0:
                        raise LedgerConflict('Imported base identity is invalid')
                    imported.append(item['grant_id'])
        bases = list(
            await db.scalars(
                select(Allowance)
                .where(
                    Allowance.subscription_id == subscription_id,
                    Allowance.traffic_group == group,
                    or_(Allowance.operation_key.startswith('base:'), Allowance.id.in_(imported)),
                    or_(Allowance.retired_at.is_(None), Allowance.retired_at > starts_at),
                )
                .order_by(Allowance.id)
                .limit(201)
            )
        )
        if len(bases) > 200 or any(item.starts_at >= starts_at for item in bases):
            raise LedgerConflict('Base refresh is superseded or exceeds inspection budget')
        issue = grant_with_reported_settlement if settle_before_grant else grant
        identifier = await issue(db, **target, **terms)
        for item in bases:
            item.retired_at = starts_at
        state.revision += 1
        await db.flush()
        return identifier


async def reset_renewal_bases(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    operation_key,
    amounts,
    starts_at,
    expires_at,
    reset_base=True,
    settle_before_grant=True,
    funded_calendar=False,
):
    """Apply the finite-group terms of ONE persisted paid renewal atomically.

    The financial coordinator selects the finite groups from its saved tariff
    terms. Unlimited groups receive no finite grants. Stopped BS sales permit
    only separately validated calendar fulfillment of existing paid terms.
    This function deliberately owns neither balance nor subscription expiry.
    """
    _key(operation_key)
    if type(funded_calendar) is not bool or (funded_calendar and not operation_key.startswith('calendar:')):
        raise LedgerConflict('Explicit funded calendar operation required')
    _interval(starts_at, expires_at)
    if type(amounts) is not dict or not amounts or not set(amounts) <= {'ordinary', 'bs'}:
        raise LedgerConflict('Explicit finite renewal groups are required')
    for amount in amounts.values():
        _amount(amount)
    async with db.begin_nested():
        results = {}
        for group in ('ordinary', 'bs'):
            if group in amounts:
                results[group] = await reset_base_group(
                    db,
                    user_id=user_id,
                    subscription_id=subscription_id,
                    account_generation=account_generation,
                    group=group,
                    operation_key=operation_key,
                    amount_bytes=amounts[group],
                    starts_at=starts_at,
                    expires_at=expires_at,
                    reset_base=reset_base,
                    settle_before_grant=settle_before_grant,
                    funded_calendar=funded_calendar,
                )
        return results
