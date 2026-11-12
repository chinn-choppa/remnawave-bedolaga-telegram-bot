"""Prepare funded ordinary rights on the same panel account, without counter reset."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficEnrollment,
    BsTrafficLedger,
    BsTrafficPanelBinding,
    Subscription,
    TrafficAccountingState,
    TrafficBaseGrantIntent,
    User,
)
from app.services.bs_traffic.enrollment import validate_funding
from app.services.bs_traffic.grace_capture import _open_grace
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import panel_slot


async def load_enrollment(db, reader, enrollment_id):
    item = await db.get(BsTrafficEnrollment, enrollment_id)
    if item is None:
        raise LedgerConflict('Enrollment does not exist')
    user = await db.scalar(
        select(User).where(User.id == item.user_id).with_for_update().execution_options(populate_existing=True)
    )
    sub = await db.scalar(
        select(Subscription)
        .where(Subscription.id == item.subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    item = await db.scalar(
        select(BsTrafficEnrollment)
        .where(BsTrafficEnrollment.id == enrollment_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if item.panel_scope is None:
        from app.services.bs_traffic.panel_binding import endpoint_fingerprint

        scopes = list(
            await db.scalars(
                select(BsTrafficPanelBinding.panel_scope).where(
                    BsTrafficPanelBinding.endpoint_hash == endpoint_fingerprint(reader._api.base_url),
                    BsTrafficPanelBinding.retired_at.is_(None),
                )
            )
        )
        if scopes != [reader.panel_scope]:
            raise LedgerConflict('Initial enrollment requires one current endpoint binding')
    await validate_funding(db, item)
    ledger = await db.get(BsTrafficLedger, item.subscription_id)
    if ledger is not None and item.ownership_cycle != ledger.ownership_cycle:
        raise LedgerConflict('Enrollment belongs to an earlier ownership cycle')
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        user is None
        or user.status != 'active'
        or user.restriction_subscription
        or sub is None
        or sub.user_id != item.user_id
        or sub.is_trial
        or sub.status not in ('active', 'limited')
        or sub.tariff_id != item.tariff_id
        or sub.end_date != datetime.fromisoformat(item.terms['paid_until'])
        or sub.end_date <= now
        or await _open_grace(db, sub.id)
        or (item.panel_scope is not None and item.panel_scope != reader.panel_scope)
    ):
        raise LedgerConflict('Paid enrollment target changed or grace remains open')
    return item, sub


async def prepare_ordinary_source(reader, enrollment_id):
    from app.services.panel_sync.writer import patch_managed_traffic
    from app.services.traffic_accounting import TrafficAccount
    from app.services.traffic_accounting_delivery import deliver_account_projection
    from app.services.traffic_accounting_schedule import next_base_refresh
    from app.services.traffic_accounting_service import ensure_managed_account
    from app.services.traffic_accounting_store import initialize_account
    from app.services.traffic_accounting_sync import stage_managed_sync

    await reader.assert_current()
    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        if item.state != 'pending':
            return
        if not sub.remnawave_id:
            raise LedgerConflict('Ordinary panel provisioning remains pending')
        sid, panel_id, terms, mode = sub.id, sub.remnawave_id, dict(item.terms), sub.traffic_accounting_mode
    async with panel_slot(reader._factory, reader.panel_scope):
        observed = await reader.get_split_transfer_snapshot(panel_id)
    if observed is None or observed.id != panel_id:
        raise LedgerConflict('Enrollment account is missing')
    generation = account_generation(panel_scope=reader.panel_scope, account=observed)
    async with reader._factory() as db, db.begin():
        item, _ = await load_enrollment(db, reader, enrollment_id)
        if item.account_generation is not None and item.account_generation != generation:
            raise LedgerConflict('Enrollment panel identity changed')
        item.panel_scope, item.account_generation = reader.panel_scope, generation
    if terms['ordinary_limit_bytes']:
        async with panel_slot(reader._factory, reader.panel_scope):
            await ensure_managed_account(reader._factory, reader._api, sid, timezone=terms['calendar_timezone'])
    else:
        # Unlimited ordinary has no finite migration remainder to infer. Keep its
        # identity/counters, and confirm NO_RESET before assigning managed ownership.
        if mode not in ('legacy', 'managed') or observed.traffic_limit_bytes != 0:
            raise LedgerConflict('Unlimited ordinary source changed')
        if observed.traffic_limit_strategy != 'NO_RESET':
            async with panel_slot(reader._factory, reader.panel_scope):
                await patch_managed_traffic(reader._api, user_id=panel_id)
                observed = await reader.get_split_transfer_snapshot(panel_id)
        if (
            observed is None
            or observed.traffic_limit_bytes != 0
            or observed.traffic_limit_strategy != 'NO_RESET'
            or account_generation(panel_scope=reader.panel_scope, account=observed) != generation
        ):
            raise LedgerConflict('Unlimited source was not confirmed')
        async with reader._factory() as db, db.begin():
            item, sub = await load_enrollment(db, reader, enrollment_id)
            state = await initialize_account(
                db,
                sid,
                TrafficAccount(
                    f'{observed.id}@{observed.created_at.astimezone(UTC).isoformat()}',
                    observed.lifetime_bytes,
                    observed.used_bytes,
                    (),
                ),
                strategy=terms['calendar_strategy'],
                timezone=terms['calendar_timezone'],
                anchor=observed.created_at,
                next_refresh_at=None,
            )
            if state.phase not in ('prepared', 'unlimited'):
                raise LedgerConflict('Unlimited ordinary source ownership changed')
            state.phase, sub.traffic_accounting_mode = 'unlimited', 'managed'
            await stage_managed_sync(db, sub)
    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        state = await db.get(TrafficAccountingState, sid)
        if state is None or state.phase not in ('ready', 'unlimited'):
            raise LedgerConflict('Ordinary accountant is not ready')
        key = f'bs-enrollment:{enrollment_id}:ordinary'
        intent = await db.scalar(
            select(TrafficBaseGrantIntent).where(
                TrafficBaseGrantIntent.subscription_id == sid, TrafficBaseGrantIntent.operation_key == key
            )
        )
        now = await db.scalar(select(func.clock_timestamp()))
        if terms['ordinary_limit_bytes'] and not terms.get('ordinary_source_preexisting', False) and intent is None:
            following = next_base_refresh(
                terms['calendar_strategy'],
                now=now,
                anchor=state.calendar_anchor,
                timezone=ZoneInfo(terms['calendar_timezone']),
            )
            db.add(
                TrafficBaseGrantIntent(
                    subscription_id=sid,
                    operation_key=key,
                    base_bytes=terms['ordinary_base_bytes'],
                    reset_base=True,
                    target_unlimited=False,
                    business_strategy=terms['calendar_strategy'],
                    tariff_id=sub.tariff_id,
                    requested_at=now,
                    expires_at=following or sub.end_date,
                    state='pending',
                )
            )
            state.activation_requested = True
            await stage_managed_sync(db, sub)
    await reader.assert_current()
    async with panel_slot(reader._factory, reader.panel_scope):
        await deliver_account_projection(reader._factory, reader._api, sid)
    await reader.assert_current()
    async with reader._factory() as db, db.begin():
        item, sub = await load_enrollment(db, reader, enrollment_id)
        state = await db.get(TrafficAccountingState, sid, populate_existing=True)
        if (
            state.phase not in ('ready', 'unlimited')
            or state.projected_revision != state.revision
            or state.pending_sync_fields
        ):
            raise LedgerConflict('Ordinary paid projection remains pending')
        if terms['ordinary_limit_bytes'] and not terms.get('ordinary_source_preexisting', False):
            intent = await db.scalar(
                select(TrafficBaseGrantIntent).where(
                    TrafficBaseGrantIntent.subscription_id == sid, TrafficBaseGrantIntent.operation_key == key
                )
            )
            if intent.state != 'applied':
                raise LedgerConflict('Frozen ordinary base was not applied')
        item.state = 'provisioned'
