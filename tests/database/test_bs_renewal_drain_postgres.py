"""Renewal must freeze old BS entitlement before extending ordinary access."""

from datetime import datetime, timedelta

import pytest

from app.database.crud.subscription import extend_subscription, update_daily_charge_time
from app.database.crud.tariff import update_tariff
from app.database.models import BsTrafficLedger, Subscription, Tariff
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('daily', [False, True])
async def test_renewal_freezes_bs_before_extending_paid_end(postgres_database, monkeypatch, daily):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        before = sub.end_date
        await update_tariff(db, tariff, bs_traffic_settings=None)
        if daily:
            await update_daily_charge_time(db, sub, charge_time=before + timedelta(days=1), commit=False)
        else:
            await extend_subscription(db, sub, 30, reset_used_traffic=False, commit=False)
        state = await db.get(BsTrafficLedger, sub.id)
        assert state.access_policy['bs_purchase_enabled'] is False
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == before
        assert sub.end_date > before


@pytest.mark.parametrize('disabled', [False, True])
async def test_renewal_holds_tariff_lock_and_snapshot_rolls_back(postgres_database, monkeypatch, disabled):
    from sqlalchemy import select
    from sqlalchemy.exc import DBAPIError

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff_id, before = tariff.id, sub.end_date
        if disabled:
            await update_tariff(db, tariff, bs_traffic_settings=None)
        await update_daily_charge_time(db, sub, charge_time=before + timedelta(days=1), commit=False)
        async with reader._factory() as other:
            with pytest.raises(DBAPIError) as error:
                await other.execute(select(Tariff.id).where(Tariff.id == tariff_id).with_for_update(nowait=True))
            assert error.value.orig.sqlstate == '55P03'
            await other.rollback()
        await db.rollback()
        await db.refresh(sub)
        state = await db.get(BsTrafficLedger, target['subscription_id'], populate_existing=True)
        assert sub.end_date == before
        assert 'bs_drain_until' not in state.access_policy


async def test_daily_resume_freezes_expired_bs_before_new_period(postgres_database, monkeypatch):
    from datetime import UTC

    from app.database.crud.subscription import resume_daily_subscription

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.is_daily = True
        sub.status = 'disabled'
        sub.is_daily_paused = True
        sub.end_date = datetime.now(UTC) - timedelta(days=1)
        await db.commit()
        await db.refresh(sub, ['tariff'])
        before = sub.end_date
        await update_tariff(db, tariff, bs_traffic_settings=None)
        await resume_daily_subscription(db, sub, commit=False)
        state = await db.get(BsTrafficLedger, sub.id)
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == before
        assert sub.end_date > before


@pytest.mark.parametrize('daily', [False, True])
async def test_stale_legacy_object_cannot_extend_activated_split(postgres_database, monkeypatch, daily):
    from sqlalchemy.orm.attributes import set_committed_value

    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        before = sub.end_date
        # Emulate an ORM object loaded before another transaction activated split.
        set_committed_value(sub, 'traffic_accounting_mode', 'legacy')
        with pytest.raises(LedgerConflict, match='mode changed'):
            if daily:
                await update_daily_charge_time(db, sub, charge_time=before + timedelta(days=1), commit=False)
            else:
                await extend_subscription(db, sub, 30, reset_used_traffic=False, commit=False)
        await db.commit()
        await db.refresh(sub)
        assert sub.end_date == before
        assert sub.traffic_accounting_mode == 'split'


async def test_admin_exact_expiry_freezes_bs_before_commit(postgres_database, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.cabinet.routes import admin_users
    from app.cabinet.routes.admin_users import UpdateSubscriptionRequest

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        before = sub.end_date
        await update_tariff(db, tariff, bs_traffic_settings=None)
        monkeypatch.setattr(admin_users, 'get_user_by_id', AsyncMock(return_value=SimpleNamespace(id=sub.user_id)))
        monkeypatch.setattr('app.services.cispay_recurrent_guard.require_no_cispay', AsyncMock())
        monkeypatch.setattr(admin_users, '_sync_subscription_to_panel', AsyncMock())
        monkeypatch.setattr(admin_users, '_build_subscription_info_async', AsyncMock(return_value=None))
        result = await admin_users.update_user_subscription(
            sub.user_id,
            UpdateSubscriptionRequest(
                action='set_end_date', subscription_id=sub.id, end_date=before + timedelta(days=30)
            ),
            admin=SimpleNamespace(id=2),
            db=db,
        )
        assert result.success
        await db.refresh(sub)
        state = await db.get(BsTrafficLedger, sub.id, populate_existing=True)
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == before
        assert sub.end_date == before + timedelta(days=30)
