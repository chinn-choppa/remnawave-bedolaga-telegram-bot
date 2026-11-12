import asyncio
from unittest.mock import patch

import pytest

from app.services.bs_traffic.batch import PreparationRequest, prepare_batch
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.preparation import PreparedDelivery


REQUEST = PreparationRequest(user_id=1, subscription_id=1, account_generation='a' * 64, operation_key='test')


def batch(source, **kwargs):
    return prepare_batch(None, object(), source, panel_scope='synthetic-panel', **kwargs)


async def test_large_lazy_source_is_bounded_even_when_consumer_pauses():
    consumed = active = peak = 0

    def source():
        nonlocal consumed
        for _ in range(10000):
            consumed += 1
            yield REQUEST

    async def prepare(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0)
            return PreparedDelivery('queued', 1)
        finally:
            active -= 1

    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=prepare):
        async with batch(source(), concurrency=3) as results:
            first = await anext(results)
            assert first.prepared.status == 'queued'
            assert consumed == 3
            await asyncio.sleep(0)
            assert consumed == 3
            count = 1
            async for _ in results:
                count += 1
    assert count == 10000 and peak <= 3 and active == 0


async def test_blocked_first_item_does_not_hold_ready_result_and_close_cancels_it():
    stopped = asyncio.Event()
    entered = asyncio.Event()

    async def prepare(*args, **kwargs):
        if kwargs['operation_key'] == 'blocked':
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        await entered.wait()
        return PreparedDelivery('blocked')

    blocked = PreparationRequest(1, 1, 'a' * 64, 'blocked')
    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=prepare):
        async with batch([blocked, REQUEST], concurrency=2) as results:
            result = await asyncio.wait_for(anext(results), 1)
            assert result.request == REQUEST and result.prepared.status == 'blocked'
    assert stopped.is_set()


async def test_expected_conflict_is_isolated_without_raw_error():
    async def prepare(*args, **kwargs):
        if kwargs['operation_key'] == 'conflict':
            raise LedgerConflict('private details')
        return PreparedDelivery('queued', 1)

    conflict = PreparationRequest(1, 1, 'a' * 64, 'conflict')
    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=prepare):
        async with batch([conflict, REQUEST]) as results:
            outcomes = [item async for item in results]
    assert {item.status for item in outcomes} == {'prepared', 'conflict'}
    assert 'private details' not in repr(outcomes)
    assert next(item for item in outcomes if item.status == 'conflict').prepared is None


async def test_unexpected_failure_cancels_and_awaits_other_tasks():
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def prepare(*args, **kwargs):
        if kwargs['operation_key'] == 'test':
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        await entered.wait()
        raise RuntimeError('database unavailable')

    failed = PreparationRequest(1, 1, 'a' * 64, 'failed')
    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=prepare):
        with pytest.raises(RuntimeError, match='database unavailable'):
            async with batch([REQUEST, failed]) as results:
                await anext(results)
    assert stopped.is_set()


async def test_caller_cancellation_awaits_owned_tasks():
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def prepare(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def consume():
        async with batch([REQUEST]) as results:
            await anext(results)

    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=prepare):
        task = asyncio.create_task(consume())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert stopped.is_set()


@pytest.mark.parametrize('concurrency', [0, -1, True, 1.5, 33])
async def test_invalid_concurrency(concurrency):
    with pytest.raises(ValueError):
        async with batch([], concurrency=concurrency):
            pass


async def test_empty_source_has_no_work():
    with patch('app.services.bs_traffic.batch.prepare_delivery') as prepare:
        async with batch([]) as results:
            assert [item async for item in results] == []
    prepare.assert_not_called()


async def test_source_failure_does_not_leave_admitted_tasks_running():
    def source():
        yield REQUEST
        raise RuntimeError('source failed')

    with patch('app.services.bs_traffic.batch.prepare_delivery') as prepare:
        with pytest.raises(RuntimeError, match='source failed'):
            async with batch(source(), concurrency=2) as results:
                await anext(results)
        # Cancellation happens before the first admitted coroutine can run.
        prepare.assert_not_called()


async def test_malformed_item_is_not_sent_to_database():
    with patch('app.services.bs_traffic.batch.prepare_delivery') as prepare:
        with pytest.raises(TypeError, match='PreparationRequest'):
            async with batch([{}]) as results:
                await anext(results)
    prepare.assert_not_called()


async def test_never_consumed_context_does_not_start_source_or_database():
    def source():
        raise AssertionError('must stay lazy')
        yield REQUEST

    with patch('app.services.bs_traffic.batch.prepare_delivery') as prepare:
        async with batch(source()):
            pass
    prepare.assert_not_called()


async def test_preparation_deadline_releases_slot_and_continues_source():
    stopped = asyncio.Event()

    async def prepare(*args, **kwargs):
        if kwargs['operation_key'] == 'timeout':
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        return PreparedDelivery('queued', 1)

    slow = PreparationRequest(1, 1, 'a' * 64, 'timeout')
    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=prepare):
        async with batch([slow, REQUEST], concurrency=1, timeout_seconds=0.02) as results:
            outcomes = [item async for item in results]
    assert outcomes[0].status == 'timeout' and outcomes[0].prepared is None
    assert outcomes[1].prepared.status == 'queued'
    assert stopped.is_set()


async def test_unexpected_timeout_is_not_suppressed_as_batch_deadline():
    with patch('app.services.bs_traffic.batch.prepare_delivery', side_effect=TimeoutError('unexpected')):
        with pytest.raises(TimeoutError, match='unexpected'):
            async with batch([REQUEST]) as results:
                await anext(results)


@pytest.mark.parametrize('timeout_seconds', [0, -1, True, float('nan'), float('inf'), 301, '30'])
async def test_invalid_preparation_deadline(timeout_seconds):
    with pytest.raises(ValueError):
        async with batch([], timeout_seconds=timeout_seconds):
            pass
