import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import Base, Tariff
from app.services.external_squad_reconciliation import reconcile_deleted_external_squads
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_concurrent_repair_clears_mapping_and_notifies_once(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        db.add(Tariff(id=1, name='Example', external_squad_uuid='missing', period_prices={}))
        db.add(Tariff(id=2, name='Another plan', external_squad_uuid='missing', period_prices={}))
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        barrier = asyncio.Barrier(2)

        async def missing(uuid):
            await barrier.wait()

        api = SimpleNamespace(get_external_squads=AsyncMock(return_value=[]), get_external_squad_by_uuid=missing)
        notify = AsyncMock(return_value=True)
        monkeypatch.setattr(
            'app.services.admin_notification_service.AdminNotificationService.send_deleted_external_squad_notification',
            notify,
        )
        results = await asyncio.wait_for(
            asyncio.gather(
                reconcile_deleted_external_squads(api, factory, bot=object()),
                reconcile_deleted_external_squads(api, factory, bot=object()),
            ),
            timeout=10,
        )
        assert sum(map(len, results)) == 2
        assert notify.await_count == 1
        db.expire_all()
        assert (await db.get(Tariff, 1)).external_squad_uuid is None
        assert (await db.get(Tariff, 2)).external_squad_uuid is None
