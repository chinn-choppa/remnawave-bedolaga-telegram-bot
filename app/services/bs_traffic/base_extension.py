"""No-reset renewals extend coverage over the same historical byte balance."""

import hashlib
import json
from datetime import UTC

from sqlalchemy import and_, exists, func, or_, select

from app.database.models import BsTrafficAllowance as Allowance, BsTrafficBaseExtension as Extension


def operation_identity(operation_key):
    return 'base:renew:' + hashlib.sha256(operation_key.encode()).hexdigest()


def operation_variants(operation_key):
    return [operation_identity(operation_key)] + [
        'base:renew:' + hashlib.sha256(f'{operation_key}:{reset}'.encode()).hexdigest() for reset in (True, False)
    ]


def covers_interval(allowance, start, end, *, instant=False):
    """No interval crosses an unpaid gap; balances are selected only once."""
    original = and_(
        allowance.starts_at <= start, allowance.expires_at > end if instant else allowance.expires_at >= end
    )
    extension = exists(
        select(Extension.id).where(
            Extension.allowance_id == allowance.id,
            Extension.subscription_id == allowance.subscription_id,
            Extension.traffic_group == allowance.traffic_group,
            Extension.starts_at <= start,
            Extension.expires_at > end if instant else Extension.expires_at >= end,
        )
    )
    return or_(original, extension)


def effective_expiry(allowance):
    last = (
        select(func.max(Extension.expires_at))
        .where(
            Extension.allowance_id == allowance.id,
            Extension.subscription_id == allowance.subscription_id,
            Extension.traffic_group == allowance.traffic_group,
        )
        .correlate(allowance)
        .scalar_subquery()
    )
    return func.greatest(allowance.expires_at, func.coalesce(last, allowance.expires_at))


async def extension_boundary(db, subscription_id, now):
    values = []
    for column in (Extension.starts_at, Extension.expires_at):
        moment = await db.scalar(
            select(func.min(column)).where(
                Extension.subscription_id == subscription_id,
                column > now,
            )
        )
        if moment is not None:
            values.append(moment)
    return min(values) if values else None


async def extend_base_group(
    db, *, user_id, subscription_id, account_generation, group, operation_key, amount_bytes, starts_at, expires_at
):
    from app.database.models import BsTrafficManagedTransfer
    from app.services.bs_traffic.ledger import (
        LedgerConflict,
        _amount,
        _group,
        _interval,
        _key,
        _lock,
        require_activated_owner,
    )
    from app.services.bs_traffic.policy import _read_policy

    _key(operation_key)
    _group(group)
    _amount(amount_bytes)
    _interval(starts_at, expires_at)
    key = operation_identity(operation_key)
    digest = hashlib.sha256(
        json.dumps(
            [
                user_id,
                subscription_id,
                account_generation,
                group,
                amount_bytes,
                starts_at.astimezone(UTC).isoformat(),
                expires_at.astimezone(UTC).isoformat(),
            ],
            separators=(',', ':'),
        ).encode()
    ).hexdigest()
    async with db.begin_nested():
        state, _ = await _lock(db, user_id, subscription_id, account_generation)
        await require_activated_owner(db, subscription_id)
        if (
            await db.scalar(
                select(Allowance.id).where(
                    Allowance.subscription_id == subscription_id,
                    Allowance.traffic_group == group,
                    Allowance.operation_key.in_(operation_variants(operation_key)),
                )
            )
            is not None
        ):
            raise LedgerConflict('Renewal operation already reset its base')
        old = await db.scalar(
            select(Extension).where(
                Extension.subscription_id == subscription_id,
                Extension.traffic_group == group,
                Extension.operation_key == key,
            )
        )
        if old is not None:
            if old.request_hash != digest:
                raise LedgerConflict('Base extension replay changed terms')
            return old.allowance_id
        policy = _read_policy(state.access_policy)
        if policy[f'{group}_mode'] != 'finite' or (group == 'bs' and policy.get('bs_purchase_enabled') is False):
            raise LedgerConflict('Group does not accept base extension')
        transfer = await db.get(BsTrafficManagedTransfer, subscription_id)
        imported = {}
        if transfer is not None and transfer.destination_group == group:
            if transfer.account_generation != account_generation or not isinstance(transfer.grants, list):
                raise LedgerConflict('Invalid imported base provenance')
            if len(transfer.grants) > 200:
                raise LedgerConflict('Imported base inspection budget exceeded')
            for item in transfer.grants:
                if not isinstance(item, dict) or not isinstance(item.get('source_key'), str):
                    raise LedgerConflict('Invalid imported base provenance')
                if item['source_key'].startswith('base:'):
                    _amount(item.get('original_bytes'))
                    if type(item.get('grant_id')) is not int or item['grant_id'] <= 0:
                        raise LedgerConflict('Invalid imported base identity')
                    imported[item['grant_id']] = item['original_bytes']
        bases = list(
            await db.scalars(
                select(Allowance)
                .where(
                    Allowance.subscription_id == subscription_id,
                    Allowance.traffic_group == group,
                    Allowance.retired_at.is_(None),
                    or_(Allowance.operation_key.startswith('base:'), Allowance.id.in_(imported)),
                )
                .limit(2)
            )
        )
        if len(bases) != 1:
            raise LedgerConflict('No-reset requires one authoritative shared base')
        base = bases[0]
        if imported.get(base.id, base.granted_bytes) != amount_bytes or base.starts_at >= starts_at:
            raise LedgerConflict('No-reset base resize or stale renewal requires reconciliation')
        latest = await db.scalar(select(func.max(Extension.requested_at)).where(Extension.allowance_id == base.id))
        if latest is not None and latest >= starts_at:
            raise LedgerConflict('Base extension is superseded')
        intervals = list(
            (
                await db.execute(
                    select(Extension.starts_at, Extension.expires_at)
                    .where(
                        Extension.allowance_id == base.id,
                    )
                    .order_by(Extension.id.desc())
                    .limit(201)
                )
            ).all()
        )
        if len(intervals) > 200:
            raise LedgerConflict('Base extension inspection budget exceeded')
        if any(end > expires_at for _, end in intervals):
            raise LedgerConflict('Base extension is superseded')
        # Merge only overlapping funded intervals. An expired subscription has
        # a real gap which must remain ineligible for retrospective usage.
        coverage_start = starts_at
        for left, right in [(base.starts_at, base.expires_at), *reversed(intervals)]:
            if left <= coverage_start <= right:
                coverage_start = left
        db.add(
            Extension(
                subscription_id=subscription_id,
                allowance_id=base.id,
                traffic_group=group,
                operation_key=key,
                request_hash=digest,
                requested_at=starts_at,
                starts_at=coverage_start,
                expires_at=expires_at,
            )
        )
        state.revision += 1
        await db.flush()
        return base.id
