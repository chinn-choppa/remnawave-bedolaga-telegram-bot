"""Existing top-up entrypoints must never mutate split subscriptions as legacy."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from sqlalchemy import func, select, update

from app.database.models import Subscription, TrafficPurchase, User
from app.services.traffic_accounting import AccountingConflict
from app.services.traffic_purchase_service import add_subscription_traffic, prepare_traffic_purchase
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [User.__table__, Subscription.__table__, TrafficPurchase.__table__]


async def seed(db, mode, limit):
    now = datetime.now(UTC)
    db.add(User(id=1, balance_kopeks=1000))
    await db.flush()
    sub = Subscription(
        id=1,
        user_id=1,
        start_date=now,
        end_date=now + timedelta(days=30),
        traffic_limit_gb=limit,
        purchased_traffic_gb=0,
        traffic_accounting_mode=mode,
    )
    db.add(sub)
    await db.flush()
    return sub


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
@pytest.mark.parametrize('limit', [0, 10])
async def test_standard_add_rejects_split_before_nominal_mutation(postgres_database, mode, limit):
    async with postgres_session(postgres_database, TABLES) as db:
        sub = await seed(db, mode, limit)
        with pytest.raises(AccountingConflict, match='group-aware'):
            await add_subscription_traffic(db, sub, 5, commit=False)
        assert sub.traffic_limit_gb == limit and sub.purchased_traffic_gb == 0
        assert await db.scalar(select(func.count()).select_from(TrafficPurchase)) == 0
        assert (await db.get(User, 1)).balance_kopeks == 1000


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_standard_prepare_rejects_split_before_panel_call(postgres_database, monkeypatch, mode):
    service = Mock(side_effect=AssertionError('Must not contact panel for wrong purchase flow'))
    monkeypatch.setattr('app.services.traffic_purchase_service.SubscriptionService', service)
    async with postgres_session(postgres_database, TABLES) as db:
        sub = await seed(db, mode, 10)
        with pytest.raises(AccountingConflict, match='group-aware'):
            await prepare_traffic_purchase(db, sub, requested_gb=5)
        service.assert_not_called()


@pytest.mark.parametrize('operation', ['prepare', 'add'])
@pytest.mark.parametrize('limit', [0, 10])
async def test_cached_legacy_mode_cannot_bypass_persisted_split(postgres_database, monkeypatch, operation, limit):
    service = Mock(side_effect=AssertionError('Must not contact panel'))
    monkeypatch.setattr('app.services.traffic_purchase_service.SubscriptionService', service)
    async with postgres_session(postgres_database, TABLES) as db:
        sub = await seed(db, 'legacy', limit)
        await db.execute(
            update(Subscription)
            .where(Subscription.id == sub.id)
            .values(traffic_accounting_mode='split')
            .execution_options(synchronize_session=False)
        )
        assert sub.traffic_accounting_mode == 'legacy'
        with pytest.raises(AccountingConflict, match='group-aware'):
            if operation == 'prepare':
                await prepare_traffic_purchase(db, sub, requested_gb=5)
            else:
                await add_subscription_traffic(db, sub, 5, commit=False)
        assert await db.scalar(select(func.count()).select_from(TrafficPurchase)) == 0
        service.assert_not_called()


@pytest.mark.parametrize('mode', ['legacy', 'managed'])
async def test_existing_unlimited_noop_is_preserved(postgres_database, mode):
    async with postgres_session(postgres_database, TABLES) as db:
        sub = await seed(db, mode, 0)
        assert await add_subscription_traffic(db, sub, 5, commit=False) is sub
        assert await db.scalar(select(func.count()).select_from(TrafficPurchase)) == 0
