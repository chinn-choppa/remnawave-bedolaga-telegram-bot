"""Package eligibility is rechecked under the real PostgreSQL accounting lease."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update

from app.config import settings
from app.database.models import Base, Subscription, Tariff, User
from app.services.traffic_accounting import AccountingConflict
from app.services.traffic_purchase_service import prepare_traffic_purchase
from tests.database.test_traffic_purchase_atomic_postgres import seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('case', ['valid', 'removed', 'free', 'disabled', 'over_cap', 'changed_price'])
async def test_tariff_package_rechecked_before_panel_snapshot(postgres_database, monkeypatch, case):
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        panel = await seed(db)
        tariff = Tariff(
            id=1,
            name='test',
            traffic_limit_gb=10,
            traffic_topup_enabled=case != 'disabled',
            traffic_topup_packages={'5': 100 if case != 'free' else 0},
            max_topup_traffic_gb=14 if case == 'over_cap' else 20,
        )
        db.add(tariff)
        sub = await db.get(Subscription, 1)
        sub.tariff_id = 1
        await db.commit()
        monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: True)

        @asynccontextmanager
        async def client(self):
            yield object()

        async def migration(*args, **kwargs):
            if case == 'changed_price':
                await db.execute(
                    update(Tariff)
                    .where(Tariff.id == 1)
                    .values(traffic_topup_packages={'5': 200})
                    .execution_options(synchronize_session=False)
                )

        snapshot = AsyncMock(return_value=panel)
        monkeypatch.setattr('app.services.subscription_service.SubscriptionService.get_api_client', client)
        monkeypatch.setattr('app.services.traffic_accounting_service.ensure_managed_account', migration)
        monkeypatch.setattr('app.services.traffic_accounting_service._fresh_panel', snapshot)
        if case == 'valid':
            actual, result = await prepare_traffic_purchase(db, sub, requested_gb=5)
            assert actual.id == 1 and result is panel
            snapshot.assert_awaited_once()
        else:
            with pytest.raises(AccountingConflict):
                await prepare_traffic_purchase(db, sub, requested_gb=7 if case == 'removed' else 5)
            snapshot.assert_not_awaited()
        assert (await db.get(User, 1)).balance_kopeks == 10000
