from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import Subscription, TrafficAccountingState
from app.services.traffic_accounting import AccountingConflict
from app.services.traffic_accounting_service import ensure_managed_account, lock_accounting_subscription
from app.services.traffic_accounting_store import load_account, save_account
from app.services.traffic_accounting_worker import TrafficAccountingWorker
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_old_accounting_lock_rejects_new_owner(postgres_database, mode):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        (await db.get(Subscription, 1)).traffic_accounting_mode = mode
        await db.commit()
        with pytest.raises(AccountingConflict, match='another quota writer'):
            await lock_accounting_subscription(db, 1)
        await db.rollback()
        api = AsyncMock()
        with pytest.raises(AccountingConflict):
            await ensure_managed_account(async_sessionmaker(db.bind), api, 1)
        assert api.mock_calls == []


async def test_transferred_history_is_readable_but_cannot_be_updated(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        state = await db.get(TrafficAccountingState, 1)
        state.phase = 'transferred'
        await db.commit()
        account, state = await load_account(db, 1)
        assert account.used_bytes == 200
        with pytest.raises(AccountingConflict, match='read-only'):
            await save_account(db, 1, replace(account, lifetime_bytes=1001), expected_revision=state.revision)
        assert state.lifetime_bytes == 1000 and state.revision == 3


@pytest.mark.parametrize('mode,phase', [('split', 'ready'), ('split_pending', 'prepared'), ('managed', 'transferred')])
async def test_old_worker_does_not_schedule_transferred_owner(postgres_database, mode, phase):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        (await db.get(Subscription, 1)).traffic_accounting_mode = mode
        (await db.get(TrafficAccountingState, 1)).phase = phase
        await db.commit()
        factory = Mock()
        worker = TrafficAccountingWorker(session_factory=async_sessionmaker(db.bind), service_factory=factory)
        assert await worker.run_once() == 0
        factory.assert_not_called()
