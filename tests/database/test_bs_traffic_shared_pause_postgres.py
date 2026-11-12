from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget as Budget, BsTrafficRecoverySchedule as Schedule, User
from app.external.remnawave_api import RemnaWaveTransientError
from app.services.bs_traffic.panel_pause import record_pause, remaining_pause
from app.services.bs_traffic.queue import claim_job
from app.services.bs_traffic.recovery_schedule import register_recovery_scope, run_recovery_once
from tests.database.test_bs_traffic_ledger_postgres import SCOPE
from tests.database.test_bs_traffic_prepare_postgres import client, prepare, setup
from tests.database.test_bs_traffic_queue_postgres import JOB_TABLES, enqueue
from tests.database.test_bs_traffic_recovery_preflight_postgres import attempt
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = list(dict.fromkeys([Budget.__table__, Schedule.__table__, User.__table__, *JOB_TABLES]))
OTHER = '99999999-9999-4999-8999-999999999999'


async def test_pause_gate_stops_prepare_and_recovery_before_network(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        api = client()
        target, _ = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await record_pause(db, panel_scope=SCOPE, seconds=300)
        await db.commit()
        assert (await prepare(db, api, target)).status == 'unavailable'
        assert not await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        assert await claim_job(db, SCOPE) is None
        api.get_system_configuration.assert_not_awaited()
        api.get_split_access_snapshot.assert_not_awaited()


@pytest.mark.parametrize('lane', ['prepare', 'recovery'])
async def test_provider_failure_blocks_collection_and_other_lanes(postgres_database, lane):
    async with postgres_session(postgres_database, TABLES) as db:
        api = client()
        if lane == 'prepare':
            target = await setup(db, api)
        else:
            target, _ = await attempt(db, api)
            await register_recovery_scope(db, panel_scope=SCOPE)
            await db.commit()
        await enqueue(db)
        await db.commit()
        error = RemnaWaveTransientError('private provider data', status_code=429)
        error.retry_after_seconds = 180
        api.get_system_configuration.side_effect = error
        if lane == 'prepare':
            assert (await prepare(db, api, target)).status == 'unavailable'
        else:
            assert await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        assert await remaining_pause(db, panel_scope=SCOPE) > 170
        assert await claim_job(db, SCOPE) is None
        assert await remaining_pause(db, panel_scope=OTHER) == 0
        api.get_system_configuration.reset_mock()
        assert (await prepare(db, api, target)).status == 'unavailable'
        api.get_system_configuration.assert_not_awaited()


async def test_extending_shared_pause_never_changes_collection_slot(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        token = str(uuid4())
        until = datetime.now(UTC) + timedelta(seconds=60)
        db.add(Budget(panel_scope=SCOPE, slot_token=token, slot_until=until))
        await db.commit()
        await record_pause(db, panel_scope=SCOPE, seconds=300)
        budget = await db.get(Budget, SCOPE)
        deadline = budget.cooldown_until
        await record_pause(db, panel_scope=SCOPE, seconds=1)
        assert budget.cooldown_until == deadline
        assert budget.slot_token == token and budget.slot_until == until
        await db.commit()
        assert await remaining_pause(db, panel_scope=SCOPE) > 290


async def test_pause_arriving_after_topology_prevents_next_account_read(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        api = client()
        target = await setup(db, api)
        api.get_split_access_snapshot.reset_mock()
        original = api.get_internal_squad_topology.side_effect

        async def pause(squad):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await record_pause(other, panel_scope=SCOPE, seconds=120)
            return await original(squad)

        api.get_internal_squad_topology.side_effect = pause
        assert (await prepare(db, api, target)).status == 'unavailable'
        api.get_split_access_snapshot.assert_not_awaited()


@pytest.mark.parametrize('during_read', [False, True])
async def test_sender_respects_pause_without_calling_patch(postgres_database, during_read):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficDelivery
    from tests.database.test_bs_traffic_sender_postgres import panel, prepare as stage_sender, run

    async with postgres_session(postgres_database, TABLES) as db:
        prepared = await stage_sender(db)
        _, _, operation = prepared

        async def snapshot(panel_id):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await record_pause(other, panel_scope=SCOPE, seconds=120)
            return panel()

        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(side_effect=snapshot), update_internal_squads_once=AsyncMock()
        )
        if not during_read:
            await record_pause(db, panel_scope=SCOPE, seconds=120)
            await db.commit()
        assert await run(db, api, prepared) == ('not_sent' if during_read else 'deferred')
        row = await db.get(BsTrafficDelivery, operation)
        assert row.state == ('not_sent' if during_read else 'pending')
        if during_read:
            assert row.not_sent_reason == 'provider_backoff'
        api.update_internal_squads_once.assert_not_awaited()
        assert api.get_split_access_snapshot.await_count == int(during_read)


async def test_collection_failure_pauses_preparation_and_recovery(postgres_database):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.queue import run_once

    async with postgres_session(postgres_database, TABLES) as db:
        api = client()
        target, _ = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await enqueue(db)
        await db.commit()
        error = RemnaWaveTransientError('private response', status_code=429)
        error.retry_after_seconds = 200
        api.get_daily_nodes_usage = AsyncMock(side_effect=error)
        factory = async_sessionmaker(db.bind)
        assert await run_once(factory, api, SCOPE)
        assert await remaining_pause(db, panel_scope=SCOPE) > 190
        assert (await prepare(db, api, target)).status == 'unavailable'
        assert not await run_recovery_once(factory, api, panel_scope=SCOPE)
        api.get_daily_nodes_usage.assert_awaited_once()
        api.get_system_configuration.assert_not_awaited()
        api.get_split_access_snapshot.assert_not_awaited()


async def test_cancelled_collector_failure_without_header_still_pauses_panel(postgres_database):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.queue import cancel_job, run_once

    async with postgres_session(postgres_database, TABLES) as db:
        job = await enqueue(db)
        await db.commit()
        factory = async_sessionmaker(db.bind)

        async def failed(*args, **kwargs):
            async with factory() as other, other.begin():
                assert await cancel_job(other, job, panel_scope=SCOPE)
            raise RemnaWaveTransientError('private unavailable response', status_code=503)

        api = SimpleNamespace(get_daily_nodes_usage=AsyncMock(side_effect=failed))
        assert await run_once(factory, api, SCOPE)
        assert await remaining_pause(db, panel_scope=SCOPE) > 50
        assert (await db.get(Budget, SCOPE)).slot_token is None
