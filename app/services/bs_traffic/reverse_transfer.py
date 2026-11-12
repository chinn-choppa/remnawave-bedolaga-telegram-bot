"""Explicit drained split ownership return; no reset and no inferred flush barrier."""

from datetime import datetime

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficAllowance as Allowance,
    BsTrafficDelivery,
    BsTrafficEnrollment,
    BsTrafficGraceIntent,
    BsTrafficPanelBinding,
    BsTrafficRenewalIntent,
    BsTrafficUsage,
    BsTrafficUsageCursor,
    PaidTrafficResetIntent,
    Subscription,
    TrafficAccountingState,
    TrafficBaseGrantIntent,
    User,
)
from app.services.bs_traffic.base_extension import covers_interval, effective_expiry
from app.services.bs_traffic.grace_capture import _open_grace
from app.services.bs_traffic.ledger import LedgerConflict, require_activated_owner
from app.services.bs_traffic.ownership_cycle import current_transfer
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.readback import _recovery_lock


async def inspect_reverse(db, reader, *, user_id, subscription_id, account_generation, pending=False):
    """Locked local manifest. Its counters are historical, not a panel flush proof."""
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    sub = await db.get(Subscription, subscription_id, populate_existing=True)
    source = await db.get(TrafficAccountingState, subscription_id, populate_existing=True)
    receipt = await current_transfer(db, subscription_id)
    now = await db.scalar(select(func.clock_timestamp()))
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == reader.panel_scope,
            BsTrafficPanelBinding.endpoint_hash == reader.endpoint_hash,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    if (
        binding is None
        or ledger.panel_scope != reader.panel_scope
        or receipt is None
        or receipt.account_generation != account_generation
        or source is None
        or source.phase != 'transferred'
        or source.panel_identity != receipt.source_identity
        or sub.traffic_accounting_mode != ('split_pending' if pending else 'split')
        or source.pending_sync_fields
        or source.activation_requested
        or source.grace_excluded
        or sub.is_trial
        or sub.end_date is None
    ):
        raise LedgerConflict('Reverse ownership is not ready')
    if not pending:
        await require_activated_owner(db, subscription_id)
    if (ledger.restriction_attempt or {}).get('state') == 'attempted':
        raise LedgerConflict('An unknown restriction still owns the panel')
    policy = _read_policy(ledger.access_policy)
    deadline = policy.get('bs_drain_until')
    if policy.get('bs_purchase_enabled', True) or deadline is None or datetime.fromisoformat(deadline) > now:
        raise LedgerConflict('Paid BS rights are still draining')
    if await _open_grace(db, subscription_id):
        raise LedgerConflict('Open grace cannot cross ownership')
    for model, predicate in [
        (BsTrafficEnrollment, BsTrafficEnrollment.state != 'verified'),
        (TrafficBaseGrantIntent, TrafficBaseGrantIntent.state == 'pending'),
        (PaidTrafficResetIntent, PaidTrafficResetIntent.state.in_(('pending', 'attempted', 'blocked'))),
        (BsTrafficUsage, BsTrafficUsage.state == 'pending'),
        (BsTrafficUsageCursor, BsTrafficUsageCursor.needs_review.is_(True)),
        (BsTrafficDelivery, BsTrafficDelivery.state.in_(('pending', 'attempted', 'attention'))),
        (BsTrafficGraceIntent, BsTrafficGraceIntent.state.not_in(('verified', 'cancelled', 'not_sent'))),
        (BsTrafficRenewalIntent, BsTrafficRenewalIntent.state.in_(('pending', 'attempted'))),
    ]:
        if (
            await db.scalar(
                select(model.subscription_id).where(model.subscription_id == subscription_id, predicate).limit(1)
            )
            is not None
        ):
            raise LedgerConflict('Unresolved accounting or panel operation prevents ownership return')
    drops = await db.scalars(
        select(BsTrafficDelivery).where(
            BsTrafficDelivery.subscription_id == subscription_id, BsTrafficDelivery.connection_drop.is_not(None)
        )
    )
    for delivery in drops:
        drop = delivery.connection_drop or {}
        if drop.get('state') == 'attempted' or (delivery.state == 'verified' and drop.get('state') == 'pending'):
            raise LedgerConflict('An unresolved connection drop prevents ownership return')
    bs_right = await db.scalar(
        select(Allowance.id)
        .where(
            Allowance.subscription_id == subscription_id,
            Allowance.ownership_cycle == ledger.ownership_cycle,
            Allowance.traffic_group == 'bs',
            Allowance.retired_at.is_(None),
            effective_expiry(Allowance) > now,
        )
        .limit(1)
    )
    if bs_right is not None:
        raise LedgerConflict('Paid BS rights have not expired')
    if not pending:
        future = await db.scalar(
            select(Allowance.id)
            .where(
                Allowance.subscription_id == subscription_id,
                Allowance.ownership_cycle == ledger.ownership_cycle,
                Allowance.traffic_group == 'ordinary',
                Allowance.retired_at.is_(None),
                Allowance.remaining_bytes > 0,
                effective_expiry(Allowance) > now,
                ~covers_interval(Allowance, now, now, instant=True),
            )
            .limit(1)
        )
        if future is not None:
            raise LedgerConflict('Future ordinary rights must not become immediately spendable')
    frozen = None
    if pending:
        from app.database.models import BsTrafficOwnershipCycle

        cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, ledger.ownership_cycle))
        if cycle is None or cycle.state != 'reverse_pending' or cycle.reverse_manifest is None:
            raise LedgerConflict('Missing reverse ownership manifest')
        frozen = cycle.reverse_manifest
    rows = (
        await db.execute(
            select(Allowance, effective_expiry(Allowance))
            .where(
                Allowance.subscription_id == subscription_id,
                Allowance.ownership_cycle == ledger.ownership_cycle,
                Allowance.traffic_group == 'ordinary',
                Allowance.retired_at.is_(None),
                Allowance.id.in_([item['id'] for item in frozen['grants']])
                if frozen is not None
                else covers_interval(Allowance, now, now, instant=True),
            )
            .order_by(Allowance.id)
            .limit(1001)
        )
    ).all()
    if len(rows) > 1000:
        raise LedgerConflict('Reverse transfer grant budget exceeded')
    calendar = dict(
        strategy=source.business_strategy,
        timezone=source.calendar_timezone,
        anchor=source.calendar_anchor.isoformat() if source.calendar_anchor else None,
        next_refresh_at=source.next_refresh_at.isoformat() if source.next_refresh_at else None,
        base_budget_bytes=source.base_budget_bytes,
    )
    from app.services.bs_traffic.ownership_cycle import renewal_floor

    floor = await renewal_floor(db, ledger)
    max_renewal_id = await db.scalar(
        select(func.coalesce(func.max(BsTrafficRenewalIntent.id), 0)).where(
            BsTrafficRenewalIntent.subscription_id == subscription_id
        )
    )
    paid = await db.scalar(
        select(BsTrafficRenewalIntent)
        .where(
            BsTrafficRenewalIntent.subscription_id == subscription_id,
            BsTrafficRenewalIntent.transaction_id.is_not(None),
            BsTrafficRenewalIntent.id > floor,
        )
        .order_by(BsTrafficRenewalIntent.id.desc())
        .limit(1)
    )
    if paid is None and receipt.calendar_terms is not None and 'ordinary' in receipt.calendar_terms.get('amounts', {}):
        from app.services.bs_traffic.initial_calendar import validate_initial_calendar

        await validate_initial_calendar(db, receipt, receipt.calendar_terms, policy=policy)
        values = receipt.calendar_terms
        calendar = dict(
            strategy=values['calendar_strategy'],
            timezone=values['calendar_timezone'],
            anchor=values['calendar_anchor'],
            next_refresh_at=values['next_refresh_at'],
            base_budget_bytes=values['amounts']['ordinary'],
        )
    if paid is not None:
        from app.services.bs_traffic.renewal_delivery import _payment_receipt

        if (
            paid.account_generation != account_generation
            or paid.panel_scope != ledger.panel_scope
            or paid.parameters.get('tariff_id') != sub.tariff_id
        ):
            raise LedgerConflict('Ordinary paid calendar binding changed')
        if await _payment_receipt(db, paid, user_id) is None:
            raise LedgerConflict('Ordinary calendar funding is missing')
        values = paid.parameters
        calendar = dict(
            strategy=values['calendar_strategy'],
            timezone=values['calendar_timezone'],
            anchor=values['calendar_anchor'],
            next_refresh_at=values['next_refresh_at'],
            base_budget_bytes=values.get('amounts', {}).get('ordinary', 0),
        )
    # Calendar debt must be reconciled by the split calendar coordinator first.
    if frozen is not None:
        calendar = frozen['calendar']
    elif calendar['strategy'] != 'NO_RESET' and calendar['next_refresh_at'] is not None:
        # Persist the next future boundary without manufacturing missed grants.
        from zoneinfo import ZoneInfo

        from app.services.traffic_accounting_schedule import next_base_refresh

        if (
            datetime.fromisoformat(calendar['next_refresh_at']) <= now
            and sub.end_date > now
            and policy['ordinary_mode'] == 'finite'
            and (calendar['base_budget_bytes'] or 0) > 0
        ):
            from app.services.bs_traffic.calendar_renewal import current_interval

            current_start, _ = current_interval(
                calendar['strategy'],
                now=now,
                anchor=datetime.fromisoformat(calendar['anchor']),
                timezone=ZoneInfo(calendar['timezone']),
            )
            if not any(
                row.operation_key.startswith('base:renew:') and row.starts_at >= current_start for row, _ in rows
            ):
                raise LedgerConflict('Due ordinary calendar grant must be settled before ownership return')
        calendar['next_refresh_at'] = next_base_refresh(
            calendar['strategy'],
            now=now,
            anchor=datetime.fromisoformat(calendar['anchor']),
            timezone=ZoneInfo(calendar['timezone']),
        ).isoformat()
    imported = {item['grant_id']: item for item in receipt.grants}
    return dict(
        subscription_id=subscription_id,
        account_generation=account_generation,
        panel_scope=ledger.panel_scope,
        ownership_cycle=ledger.ownership_cycle,
        max_renewal_id=max_renewal_id,
        revision=ledger.revision,
        source_identity=source.panel_identity,
        tariff_id=sub.tariff_id,
        paid_until=sub.end_date.isoformat(),
        subscription_status=sub.status,
        user_status=await db.scalar(select(User.status).where(User.id == user_id)),
        device_limit=sub.device_limit,
        policy=policy,
        calendar=calendar,
        grants=[
            dict(
                id=row.id,
                key=row.operation_key,
                is_base=row.operation_key.startswith('base:')
                or imported.get(row.id, {}).get('source_key', '').startswith('base:'),
                granted_bytes=imported.get(row.id, {}).get('original_bytes', row.granted_bytes),
                remaining_bytes=row.remaining_bytes,
                expires_at=expiry.isoformat(),
            )
            for row, expiry in rows
        ],
    )


async def stage_reverse(db, reader, *, user_id, subscription_id, account_generation, source, counters):
    """Freeze exact full source/target and commit before the only permitted PATCH."""
    from dataclasses import replace
    from datetime import timedelta
    from uuid import uuid4

    from app.config import settings
    from app.services.bs_traffic.grace_intent import _encode
    from app.services.bs_traffic.identity import account_generation as generation_of
    from app.services.bs_traffic.ownership_cycle import archive_forward, encode_row
    from app.services.bs_traffic.readback import _observed_squads

    if settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
        raise LedgerConflict('Reverse transfer requires explicit uncertain usage write-off policy')
    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    manifest = await inspect_reverse(db, reader, **keys)
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        generation_of(panel_scope=reader.panel_scope, account=source) != account_generation
        or generation_of(panel_scope=reader.panel_scope, account=counters) != account_generation
        or source.used_traffic_bytes != counters.used_bytes
        or source.traffic_limit_bytes != 0
        or source.traffic_limit_strategy != 'NO_RESET'
        or counters.traffic_limit_bytes != 0
        or counters.traffic_limit_strategy != 'NO_RESET'
        or counters.lifetime_bytes < counters.used_bytes
    ):
        raise LedgerConflict('Reverse panel source is inconsistent')
    policy = manifest['policy']
    managed_squads = set(policy['topology']['ordinary_squads']) | set(policy['topology']['bs_squads'])
    if not set(_observed_squads(source)) <= managed_squads:
        raise LedgerConflict('Unexpected panel selection prevents reverse transfer')
    amount = sum(row['remaining_bytes'] for row in manifest['grants'])
    unlimited = policy['ordinary_mode'] == 'unlimited'
    if policy['ordinary_mode'] not in ('finite', 'unlimited'):
        raise LedgerConflict('Ordinary ownership must have funded finite or unlimited policy')
    quota = 0 if unlimited else max(1, source.used_traffic_bytes + amount)
    if quota >= 2**53:
        raise LedgerConflict('Reverse quota exceeds exact panel range')
    alive = (
        datetime.fromisoformat(manifest['paid_until']) > now
        and manifest['subscription_status'] == 'active'
        and manifest['user_status'] == 'active'
    )
    squads = policy['topology']['ordinary_squads'] if alive and (unlimited or amount) else []
    target = replace(
        source,
        status='ACTIVE' if squads else 'DISABLED',
        expire_at=max(datetime.fromisoformat(manifest['paid_until']), now + timedelta(minutes=5)),
        traffic_limit_bytes=quota,
        traffic_limit_strategy='NO_RESET',
        active_internal_squads=[{'uuid': value} for value in squads],
        external_squad_uuid=source.external_squad_uuid if squads else None,
        hwid_device_limit=manifest['device_limit'],
    )
    cycle = await archive_forward(db, ledger)
    if cycle.state != 'split':
        raise LedgerConflict('Reverse transfer already owns this cycle')
    cursor_rows = (
        await db.scalars(select(BsTrafficUsageCursor).where(BsTrafficUsageCursor.subscription_id == subscription_id))
    ).all()
    cycle.cursor_archive = [encode_row(row) for row in cursor_rows]
    cycle.state = 'reverse_pending'
    (await db.get(Subscription, subscription_id)).traffic_accounting_mode = 'split_pending'
    ledger.revision += 1
    manifest['revision'] = ledger.revision
    cycle.reverse_manifest = manifest
    history = (
        []
        if cycle.reverse_attempt is None
        else [
            *(cycle.reverse_attempt.get('history', [])),
            {k: v for k, v in cycle.reverse_attempt.items() if k != 'history'},
        ]
    )
    if len(history) > 32:
        raise LedgerConflict('Reverse known-unsent retry budget requires attention')
    boundaries = [now + timedelta(seconds=15)] + [
        datetime.fromisoformat(item['expires_at']) for item in manifest['grants']
    ]
    if alive:
        boundaries.append(datetime.fromisoformat(manifest['paid_until']))
    cycle.reverse_attempt = dict(
        history=history,
        state='attempted',
        token=str(uuid4()),
        source=_encode(source, paid=False),
        target=_encode(target, paid=False),
        counters=encode_counter(counters),
        attempted_at=now.isoformat(),
        deadline=min(boundaries).isoformat(),
    )
    await db.flush()
    return cycle.ownership_cycle


def encode_counter(counters):
    return dict(used_bytes=counters.used_bytes, lifetime_bytes=counters.lifetime_bytes)


async def complete_reverse(db, reader, *, user_id, subscription_id, account_generation, observed, counters):
    """Full readback only. Preserve every old grant and import each live remainder once."""
    from app.database.models import BsTrafficOwnershipCycle, TrafficAllowance
    from app.services.bs_traffic.cutover_intent import _hash
    from app.services.bs_traffic.grace_intent import _decode
    from app.services.bs_traffic.grace_restore import full_restore_confirmed
    from app.services.bs_traffic.identity import account_generation as generation_of

    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, ledger.ownership_cycle))
    if cycle is None or cycle.reverse_attempt is None:
        raise LedgerConflict('No durable reverse attempt')
    if cycle.state == 'managed':
        return 'managed'
    manifest = await inspect_reverse(db, reader, **keys, pending=True)
    if manifest != cycle.reverse_manifest:
        raise LedgerConflict('Reverse ownership terms changed')
    target_data = cycle.reverse_attempt['target']
    target = _decode(target_data, _hash(target_data), paid=False)
    initial = cycle.reverse_attempt['counters']
    now = await db.scalar(select(func.clock_timestamp()))
    if target.status == 'ACTIVE' and observed.status == 'EXPIRED' and target.expire_at <= now:
        from dataclasses import replace

        target = replace(target, status='EXPIRED')
    if (
        not full_restore_confirmed(observed, target)
        or generation_of(panel_scope=reader.panel_scope, account=counters) != account_generation
        or counters.used_bytes != observed.used_traffic_bytes
        or counters.traffic_limit_bytes != target.traffic_limit_bytes
        or counters.traffic_limit_strategy != 'NO_RESET'
        or counters.used_bytes < initial['used_bytes']
        or counters.lifetime_bytes < initial['lifetime_bytes']
    ):
        return 'unconfirmed'
    now = await db.scalar(select(func.clock_timestamp()))
    old_rows = (
        await db.scalars(
            select(TrafficAllowance).where(
                TrafficAllowance.subscription_id == subscription_id, TrafficAllowance.retired_at.is_(None)
            )
        )
    ).all()
    for row in old_rows:
        row.retired_at = now
    for item in manifest['grants']:
        db.add(
            TrafficAllowance(
                subscription_id=subscription_id,
                operation_key=('base:' if item['is_base'] else '') + f'reverse:{ledger.ownership_cycle}:{item["id"]}',
                granted_bytes=item['granted_bytes'],
                remaining_bytes=item['remaining_bytes'],
                expires_at=datetime.fromisoformat(item['expires_at']),
            )
        )
    split_rows = (
        await db.scalars(
            select(Allowance).where(
                Allowance.subscription_id == subscription_id,
                Allowance.ownership_cycle == ledger.ownership_cycle,
                Allowance.retired_at.is_(None),
            )
        )
    ).all()
    for row in split_rows:
        row.retired_at = now
    state = await db.get(TrafficAccountingState, subscription_id)
    state.used_bytes, state.lifetime_bytes = counters.used_bytes, counters.lifetime_bytes
    state.phase = 'unlimited' if manifest['policy']['ordinary_mode'] == 'unlimited' else 'ready'
    state.revision += 1
    # A growing counter is explicitly written off across the ownership boundary;
    # subsequent ordinary projection must restore the full transferred remainder.
    rights_expired = any(datetime.fromisoformat(item['expires_at']) <= now for item in manifest['grants'])
    state.projected_revision = (
        state.revision
        if counters.used_bytes == initial['used_bytes'] and not rights_expired and target.expire_at > now
        else state.projected_revision
    )
    state.quota_disabled = target.status == 'DISABLED'
    calendar = manifest['calendar']
    state.business_strategy = calendar['strategy']
    state.calendar_timezone = calendar['timezone']
    state.calendar_anchor = datetime.fromisoformat(calendar['anchor']) if calendar['anchor'] else None
    state.next_refresh_at = datetime.fromisoformat(calendar['next_refresh_at']) if calendar['next_refresh_at'] else None
    state.base_budget_bytes = calendar['base_budget_bytes']
    state.next_delivery_at = None
    subscription = await db.get(Subscription, subscription_id)
    subscription.traffic_accounting_mode = 'managed'
    if target.expire_at > subscription.end_date:
        subscription.grace_tail_expire_at = target.expire_at
    cycle.state, cycle.managed_at = 'managed', now
    cycle.reverse_attempt = dict(
        cycle.reverse_attempt, state='verified', verified_at=now.isoformat(), readback_counters=encode_counter(counters)
    )
    ledger.revision += 1
    await db.flush()
    return 'managed'
