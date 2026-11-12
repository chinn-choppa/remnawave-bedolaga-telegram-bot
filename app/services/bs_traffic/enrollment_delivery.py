"""Resume funded initial enrollment via the existing managed transfer and senders."""

import asyncio
from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficCutoverIntent,
    BsTrafficDelivery,
    BsTrafficLedger,
    TrafficAccountingState,
)
from app.services.bs_traffic.enrollment_source import load_enrollment, prepare_ordinary_source
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.ledger import (
    LedgerConflict,
    grant,
    initialize as initialize_ledger,
    require_activated_owner,
)
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader


async def _transfer(reader, enrollment_id):
    from app.services.bs_traffic.managed_transfer_capture import capture_managed_transfer
    from app.services.bs_traffic.managed_transfer_commit import commit_managed_transfer
    from app.services.bs_traffic.policy import configure_groups

    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        if item.state != 'provisioned':
            return
        terms, panel_id, generation = dict(item.terms), sub.remnawave_id, item.account_generation
        target = dict(user_id=item.user_id, subscription_id=sub.id, account_generation=generation)
    async with panel_slot(reader._factory, reader.panel_scope):
        evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=terms['ordinary_squads'],
            bs_squads=terms['bs_squads'],
        )
        account = await reader.get_split_transfer_snapshot(panel_id)
    if account is None or account_generation(panel_scope=reader.panel_scope, account=account) != generation:
        raise LedgerConflict('Enrollment identity changed')
    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        await initialize_ledger(db, **target, panel_scope=reader.panel_scope, panel_user_id=panel_id)
        ledger = await db.get(BsTrafficLedger, sub.id)
        if ledger.access_policy is None or item.ownership_cycle > 0:
            await configure_groups(
                db,
                **target,
                ordinary_mode='finite' if terms['ordinary_limit_bytes'] else 'unlimited',
                bs_mode='finite' if terms['bs_limit_bytes'] else 'unlimited',
                evidence=evidence,
                expected_revision=ledger.revision,
            )
        from app.services.bs_traffic.ownership_cycle import current_transfer

        if item.ownership_cycle > 0:
            refreshed_policy = dict(ledger.access_policy, bs_purchase_enabled=True)
            refreshed_policy.pop('bs_drain_until', None)
            if refreshed_policy != ledger.access_policy:
                ledger.access_policy = refreshed_policy
                ledger.revision += 1
        existing = await current_transfer(db, sub.id)
        if existing is not None:
            if existing.operation_key != f'enrollment:{enrollment_id}:transfer':
                raise LedgerConflict('Different transfer owns enrollment')
            item.state = 'transferred'
            return
    observation = await capture_managed_transfer(reader, **target, destination_group='ordinary')
    async with reader._factory() as db, db.begin():
        item, _ = await load_enrollment(db, reader, enrollment_id)
        await commit_managed_transfer(
            db,
            **target,
            operation_key=f'enrollment:{enrollment_id}:transfer',
            observation=observation,
            enrollment_id=enrollment_id,
        )
        item.state = 'transferred'


async def _cutover(reader, enrollment_id):
    from app.services.bs_traffic.cutover_coordinator import advance_cutover
    from app.services.bs_traffic.cutover_intent import stage_cutover
    from app.services.bs_traffic.managed_cutover import prepare_managed_cutover

    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        target = dict(user_id=item.user_id, subscription_id=sub.id, account_generation=item.account_generation)
        operation_id = item.cutover_id
        if item.state != 'transferred':
            return 'ready'
        if operation_id is not None:
            old = await db.get(BsTrafficCutoverIntent, operation_id)
            if old.state in ('cancelled', 'not_sent'):
                operation_id = None
    if operation_id is None:
        plan = await prepare_managed_cutover(reader, **target)
        async with reader._factory() as db, db.begin():
            item, _ = await load_enrollment(db, reader, enrollment_id)
            if item.cutover_id is not None:
                old = await db.get(BsTrafficCutoverIntent, item.cutover_id)
                if old.state not in ('cancelled', 'not_sent'):
                    operation_id = item.cutover_id
            if operation_id is None:
                operation_id = await stage_cutover(
                    db, reader, **target, operation_key=f'enrollment:{enrollment_id}:cutover:{uuid4()}', plan=plan
                )
                item.cutover_id = operation_id
    result = await advance_cutover(reader, **target, operation_id=operation_id)
    if result != 'activated':
        return result
    async with reader._factory() as db, db.begin():
        item, _ = await load_enrollment(db, reader, enrollment_id)
        await require_activated_owner(db, target['subscription_id'])
        item.state = 'activated'
    return 'ready'


async def _seed(reader, enrollment_id):
    from app.services.traffic_accounting_schedule import next_base_refresh

    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        if item.state != 'activated':
            return
        await require_activated_owner(db, sub.id)
        source = await db.get(TrafficAccountingState, sub.id)
        now = await db.scalar(select(func.clock_timestamp()))
        terms = item.terms
        paid_until = datetime.fromisoformat(terms['paid_until'])
        following = next_base_refresh(
            terms['calendar_strategy'],
            now=now,
            anchor=source.calendar_anchor,
            timezone=ZoneInfo(terms['calendar_timezone']),
        )
        expiry = min(paid_until, following) if following else paid_until
        amount = terms['bs_limit_bytes']
        if amount:
            await grant(
                db,
                user_id=item.user_id,
                subscription_id=sub.id,
                account_generation=item.account_generation,
                group='bs',
                operation_key=f'base:enrollment:{item.id}:bs',
                amount_bytes=amount,
                starts_at=now,
                expires_at=expiry,
            )
        item.calendar_terms = dict(
            tariff_id=item.tariff_id,
            paid_until=terms['paid_until'],
            amounts={'bs': amount} if amount else {},
            calendar_strategy=terms['calendar_strategy'],
            calendar_timezone=terms['calendar_timezone'],
            calendar_anchor=source.calendar_anchor.isoformat(),
            next_refresh_at=following.isoformat() if following and following < paid_until else None,
        )
        item.state = 'seeded'


async def _deliver(reader, enrollment_id):
    from app.services.bs_traffic.preparation import prepare_delivery
    from app.services.bs_traffic.recovery_preflight import recover_from_panel
    from app.services.bs_traffic.sender import send_delivery

    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        if item.state == 'verified':
            return 'already_verified'
        if item.state != 'seeded':
            return 'pending'
        target = dict(user_id=item.user_id, subscription_id=sub.id, account_generation=item.account_generation)
        operation_id = item.delivery_id
        row = await db.get(BsTrafficDelivery, operation_id) if operation_id else None
        status = row.state if row else None
        if status == 'pending':
            row.state = 'cancelled'  # Known unsent; retain history and prepare current rights.
    if status in ('attempted', 'attention'):
        result = await recover_from_panel(reader._factory, reader, **target, operation_id=operation_id)
    elif status == 'verified':
        result = 'already_verified'
    else:
        prepared = await prepare_delivery(
            reader._factory, reader, **target, operation_key=f'enrollment:{enrollment_id}:delivery:{operation_id or 0}'
        )
        if prepared.status != 'queued':
            return prepared.status
        async with reader._factory() as db, db.begin():
            item, _ = await load_enrollment(db, reader, enrollment_id)
            item.delivery_id = prepared.operation_id
        result = await send_delivery(
            reader._factory,
            reader._api,
            **target,
            operation_id=prepared.operation_id,
            evidence=prepared.evidence,
            current_guard_hash=prepared.guard_hash,
            bound_reader=reader,
        )
    if result in ('verified', 'already_verified'):
        async with reader._factory() as db, db.begin():
            item, _ = await load_enrollment(db, reader, enrollment_id)
            item.state = 'verified'
            item.verified_at = await db.scalar(select(func.clock_timestamp()))
        return 'verified'
    return result


async def advance_enrollment(reader, *, enrollment_id):
    if not isinstance(reader, BoundPanelReader) or type(enrollment_id) is not int or enrollment_id <= 0:
        raise LedgerConflict('Bound reader and exact enrollment required')
    try:
        async with asyncio.timeout(90):
            await reader.assert_current()
            await prepare_ordinary_source(reader, enrollment_id)
            await _transfer(reader, enrollment_id)
            result = await _cutover(reader, enrollment_id)
            if result != 'ready':
                return result
            await _seed(reader, enrollment_id)
            return await _deliver(reader, enrollment_id)
    except PanelBusy:
        return 'deferred'
    except (LedgerConflict, ValueError):
        return 'blocked'
    except TimeoutError:
        return 'unconfirmed'
