import asyncio

import pytest

from app.database.models import Subscription
from app.services.bs_traffic import cutover_worker
from tests.database.test_bs_traffic_cutover_coordinator_postgres import prepared
from tests.database.test_bs_traffic_cutover_sender_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_worker_pass_activates_explicit_operation_and_excludes_it_on_next_pass(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await prepared(db, monkeypatch)
        results = [item async for item in cutover_worker.cutover_pass(reader, asyncio.Event(), page_size=1)]
        assert [(item.operation_id, item.status) for item in results] == [(op, 'activated')]
        assert not [item async for item in cutover_worker.cutover_pass(reader, asyncio.Event())]
        async with reader._factory() as check:
            assert (await check.get(Subscription, keys['subscription_id'])).traffic_accounting_mode == 'split'
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_stopped_pass_does_not_read_panel_or_advance(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await prepared(db, monkeypatch)
        stop = asyncio.Event()
        stop.set()
        assert not [item async for item in cutover_worker.cutover_pass(reader, stop)]
        reader._api.restore_split_user_once.assert_not_called()


async def test_each_candidate_is_rechecked_by_coordinator(postgres_database, monkeypatch):
    from app.database.models import User

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await prepared(db, monkeypatch)
        (await db.get(User, keys['user_id'])).status = 'blocked'
        await db.commit()
        results = [item async for item in cutover_worker.cutover_pass(reader, asyncio.Event())]
        assert [(item.operation_id, item.status) for item in results] == [(op, 'accounting_pending')]
        reader._api.restore_split_user_once.assert_not_called()


async def test_worker_finishes_inflight_step_and_stops_without_waiting_interval(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, op = await prepared(db, monkeypatch)
        stop = asyncio.Event()
        original = reader._api.restore_split_user_once.side_effect

        async def stop_after_write(target):
            await original(target)
            stop.set()

        reader._api.restore_split_user_once.side_effect = stop_after_write
        await asyncio.wait_for(cutover_worker.run_cutover_worker(reader, stop, interval_seconds=300), timeout=15)
        from app.database.models import BsTrafficCutoverIntent

        async with reader._factory() as check:
            assert (await check.get(BsTrafficCutoverIntent, op)).state == 'activated'
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_pass_cancellation_preserves_durable_attempt_for_restart(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, op = await prepared(db, monkeypatch)
        original = reader._api.restore_split_user_once.side_effect

        async def interrupted(target):
            await original(target)
            raise asyncio.CancelledError

        reader._api.restore_split_user_once.side_effect = interrupted
        with pytest.raises(asyncio.CancelledError):
            async for _ in cutover_worker.cutover_pass(reader, asyncio.Event()):
                pass
        results = [item async for item in cutover_worker.cutover_pass(reader, asyncio.Event())]
        assert [(item.operation_id, item.status) for item in results] == [(op, 'activated')]
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('interval', [0, -1, True, float('nan'), float('inf'), 301])
async def test_worker_rejects_unsafe_retry_interval(interval):
    stop = asyncio.Event()
    stop.set()
    with pytest.raises(ValueError):
        await cutover_worker.run_cutover_worker(None, stop, interval_seconds=interval)


async def test_backpressure_and_stop_do_not_advance_next_candidate(postgres_database, monkeypatch):
    from tests.database.test_bs_traffic_cutover_scan_postgres import clone_candidate

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, op = await prepared(db, monkeypatch)
        await clone_candidate(db, 10)
        await db.commit()
        processed = []

        async def advance(reader, **keys):
            processed.append(keys['operation_id'])
            return 'deferred'

        monkeypatch.setattr(cutover_worker, 'advance_cutover', advance)
        stop = asyncio.Event()
        stream = cutover_worker.cutover_pass(reader, stop, page_size=1)
        try:
            assert not processed
            assert (await anext(stream)).operation_id == op
            assert processed == [op]
            stop.set()
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
            assert processed == [op]
        finally:
            await stream.aclose()


async def test_concurrent_worker_passes_send_once(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await prepared(db, monkeypatch)

        async def run():
            return [item async for item in cutover_worker.cutover_pass(reader, asyncio.Event())]

        results = await asyncio.gather(run(), run())
        assert any(item.status == 'activated' for group in results for item in group)
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_worker_waits_between_passes_and_delay_is_interruptible(monkeypatch):
    passed = asyncio.Event()
    stop = asyncio.Event()
    calls = 0

    async def one_pass(reader, stop, **kwargs):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        yield cutover_worker.CutoverResult(1, 'accounting_pending')
        passed.set()

    monkeypatch.setattr(cutover_worker, 'cutover_pass', one_pass)
    task = asyncio.create_task(cutover_worker.run_cutover_worker(None, stop, interval_seconds=5))
    try:
        await asyncio.wait_for(passed.wait(), timeout=1)
        await asyncio.sleep(0.03)
        assert calls == 1
        stop.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        stop.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
