"""An old paid reset cannot erase physical counters after split cutover."""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import Base, Subscription
from app.services import paid_traffic_reset_service as resets
from tests.database.test_paid_daily_reset_intent_postgres import charge, panel  # noqa: F401
from tests.fixtures.postgres_db import postgres_engine


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_old_pending_reset_does_not_reset_split_counters(postgres_database, panel, mode):
    panel.failure = 'canonical_none'
    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        maker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        async with maker() as db:
            intent = await charge(db, 'topup')
            await db.refresh(intent)
            assert intent.state == 'pending'
            intent_id = intent.id
            sub = await db.get(Subscription, 10)
            sub.traffic_accounting_mode = mode
            await db.commit()
        panel.failure = None
        async with maker() as db:
            assert await resets.deliver_daily_reset(db, intent_id) is False
        assert panel.reset_calls == 0
        assert panel.enable_calls == 0
