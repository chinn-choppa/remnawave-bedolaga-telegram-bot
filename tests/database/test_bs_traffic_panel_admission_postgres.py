import asyncio
from contextlib import AsyncExitStack
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget as Budget
from app.services.bs_traffic import panel_pause
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
SCOPE = '11111111-1111-4111-8111-111111111111'
OTHER = '22222222-2222-4222-8222-222222222222'


async def test_global_slots_bound_independent_sessions_and_release(postgres_database):
    from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot

    async with postgres_session(postgres_database, [Budget.__table__]) as db:
        factory = async_sessionmaker(db.bind)
        async with AsyncExitStack() as stack:
            for _ in range(4):
                await stack.enter_async_context(panel_slot(factory, SCOPE))
            with pytest.raises(PanelBusy):
                async with panel_slot(factory, SCOPE):
                    pytest.fail('fifth request must not start')
            async with panel_slot(factory, OTHER):
                pass
        async with panel_slot(factory, SCOPE):
            pass


async def test_cancelled_holder_releases_capacity(postgres_database):
    from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot

    async with postgres_session(postgres_database, [Budget.__table__]) as db:
        factory = async_sessionmaker(db.bind)
        entered = asyncio.Event()

        async def hold():
            async with panel_slot(factory, SCOPE):
                entered.set()
                await asyncio.Event().wait()

        async with AsyncExitStack() as stack:
            for _ in range(3):
                await stack.enter_async_context(panel_slot(factory, SCOPE))
            task = asyncio.create_task(hold())
            try:
                await asyncio.wait_for(entered.wait(), 3)
                with pytest.raises(PanelBusy):
                    async with panel_slot(factory, SCOPE):
                        pytest.fail('all slots held')
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            async with panel_slot(factory, SCOPE):
                pass


async def test_shared_pause_prevents_admission(postgres_database):
    from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot

    async with postgres_session(postgres_database, [Budget.__table__]) as db:
        await panel_pause.record_pause(db, panel_scope=SCOPE, seconds=60)
        await db.commit()
        with pytest.raises(PanelBusy):
            async with panel_slot(async_sessionmaker(db.bind), SCOPE):
                pytest.fail('paused request must not start')


@pytest.mark.parametrize('lane', ['prepare', 'recovery', 'collection', 'sender'])
async def test_capacity_blocks_all_split_lanes_without_http(postgres_database, lane):
    from app.services.bs_traffic.panel_admission import panel_slot
    from app.services.bs_traffic.queue import run_once
    from app.services.bs_traffic.recovery_preflight import recover_from_panel
    from app.services.bs_traffic.sender import send_delivery
    from tests.database.test_bs_traffic_prepare_postgres import client, prepare, setup
    from tests.database.test_bs_traffic_queue_postgres import enqueue
    from tests.database.test_bs_traffic_recovery_preflight_postgres import attempt
    from tests.database.test_bs_traffic_shared_pause_postgres import TABLES

    async with postgres_session(postgres_database, TABLES) as db:
        factory = async_sessionmaker(db.bind)
        api = client()
        if lane == 'recovery':
            target, operation_id = await attempt(db, api)
        else:
            target = await setup(db, api)
        if lane == 'sender':
            prepared = await prepare(db, api, target)
        if lane == 'collection':
            await enqueue(db)
            await db.commit()
        api.update_internal_squads_once = AsyncMock()
        api.get_daily_nodes_usage = AsyncMock()
        for method in vars(api).values():
            method.reset_mock()
        async with AsyncExitStack() as stack:
            for _ in range(4):
                await stack.enter_async_context(panel_slot(factory, SCOPE))
            if lane == 'prepare':
                assert (await prepare(db, api, target)).status == 'unavailable'
            elif lane == 'recovery':
                assert await recover_from_panel(factory, api, **target, operation_id=operation_id) == 'unconfirmed'
            elif lane == 'collection':
                assert not await run_once(factory, api, SCOPE)
            else:
                result = await send_delivery(
                    factory,
                    api,
                    **target,
                    operation_id=prepared.operation_id,
                    evidence=prepared.evidence,
                    current_guard_hash=prepared.guard_hash,
                )
                assert result == 'not_sent'
        api.get_system_configuration.assert_not_awaited()
        api.get_split_access_snapshot.assert_not_awaited()
        api.update_internal_squads_once.assert_not_awaited()
        api.get_daily_nodes_usage.assert_not_awaited()


async def test_exception_and_deadline_release_slot(postgres_database, monkeypatch):
    from app.services.bs_traffic import panel_admission

    async with postgres_session(postgres_database, [Budget.__table__]) as db:
        factory = async_sessionmaker(db.bind)
        async with AsyncExitStack() as stack:
            for _ in range(3):
                await stack.enter_async_context(panel_admission.panel_slot(factory, SCOPE))
            with pytest.raises(RuntimeError, match='synthetic'):
                async with panel_admission.panel_slot(factory, SCOPE):
                    raise RuntimeError('synthetic')
            monkeypatch.setattr(panel_admission, 'REQUEST_SECONDS', 0.1)
            with pytest.raises(TimeoutError):
                async with panel_admission.panel_slot(factory, SCOPE):
                    await asyncio.Event().wait()
            monkeypatch.setattr(panel_admission, 'REQUEST_SECONDS', 15)
            async with panel_admission.panel_slot(factory, SCOPE):
                pass


async def test_admission_deadline_does_not_crash_collection_worker(postgres_database, monkeypatch):
    from sqlalchemy import select

    from app.database.models import BsTrafficCollectionJob as Job
    from app.services.bs_traffic import panel_admission
    from app.services.bs_traffic.queue import run_once
    from tests.database.test_bs_traffic_queue_postgres import JOB_TABLES, enqueue

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        await enqueue(db)
        await db.commit()
        from types import SimpleNamespace

        async def hang(*args, **kwargs):
            await asyncio.Event().wait()

        api = SimpleNamespace(get_daily_nodes_usage=AsyncMock(side_effect=hang))
        factory = async_sessionmaker(db.bind)
        monkeypatch.setattr(panel_admission, 'REQUEST_SECONDS', 0.3)
        assert not await run_once(factory, api, SCOPE)
        api.get_daily_nodes_usage.assert_awaited_once()
        job = await db.scalar(select(Job))
        assert job.state == 'leased'
        assert job.attempts == 1
        # An expired local admission must not immediately replay ambiguous work.
        assert not await run_once(factory, api, SCOPE)
        assert api.get_daily_nodes_usage.await_count == 1


async def test_twenty_concurrent_callers_admit_only_four(postgres_database):
    from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot

    async with postgres_session(postgres_database, [Budget.__table__]) as db:
        factory = async_sessionmaker(db.bind)
        release, attempted = asyncio.Event(), asyncio.Event()
        counts = {'accepted': 0, 'rejected': 0}

        def registered():
            if sum(counts.values()) == 20:
                attempted.set()

        async def request():
            try:
                async with panel_slot(factory, SCOPE):
                    counts['accepted'] += 1
                    registered()
                    await release.wait()
            except PanelBusy:
                counts['rejected'] += 1
                registered()

        tasks = [asyncio.create_task(request()) for _ in range(20)]
        try:
            await asyncio.wait_for(attempted.wait(), 5)
            assert counts == {'accepted': 4, 'rejected': 16}
        finally:
            release.set()
            await asyncio.gather(*tasks)


async def test_direct_readback_cannot_bypass_capacity(postgres_database):
    from types import SimpleNamespace

    from app.database.models import User
    from app.services.bs_traffic.panel_admission import panel_slot
    from tests.database.test_bs_traffic_readback_postgres import prepare, run

    async with postgres_session(postgres_database, [Budget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock())
        async with AsyncExitStack() as stack:
            for _ in range(4):
                await stack.enter_async_context(panel_slot(async_sessionmaker(db.bind), SCOPE))
            assert await run(db, api, op, target) == 'unconfirmed'
        api.get_split_access_snapshot.assert_not_awaited()
