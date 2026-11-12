import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.bs_traffic.runtime import SplitTrafficRuntime


async def test_disabled_runtime_never_opens_services():
    runtime = SplitTrafficRuntime(service_factory=lambda: pytest.fail('service opened'))
    await runtime.start()
    assert runtime.task is None
    assert not runtime.ready('scope')
    await runtime.stop()


async def test_stop_cancels_inflight_and_revokes_readiness():
    runtime = SplitTrafficRuntime(enabled=True)
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def run():
        try:
            runtime._healthy['scope'] = asyncio.get_running_loop().time()
            entered.set()
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    runtime._run = run
    await runtime.start()
    await entered.wait()
    assert runtime.ready('scope')
    await runtime.stop()
    assert cancelled.is_set() and not runtime.ready('scope')
    assert runtime.task is None


async def test_scope_failure_revokes_health_without_stopping_other_scope():
    runtime = SplitTrafficRuntime(enabled=True)
    runtime._healthy['bad'] = asyncio.get_running_loop().time()
    runtime._process_scope = AsyncMock(side_effect=ValueError('bad state'))
    await runtime._guard_scope(SimpleNamespace(panel_scope='bad'))
    assert 'bad' not in runtime._healthy
    runtime._process_scope = AsyncMock(return_value=datetime.now(UTC))
    await runtime._guard_scope(SimpleNamespace(panel_scope='good'))
    assert 'good' in runtime._healthy


async def test_global_concurrency_is_bounded():
    runtime = SplitTrafficRuntime(enabled=True, concurrency=2)
    active = peak = 0

    async def process(reader):
        nonlocal active, peak
        active += 1
        peak = max(active, peak)
        await asyncio.sleep(0)
        active -= 1

    runtime._process_scope = process
    await asyncio.gather(*(runtime._guard_scope(SimpleNamespace(panel_scope=str(i))) for i in range(20)))
    assert peak == 2 and active == 0


@pytest.mark.parametrize('kwargs', [{'page_size': 0}, {'concurrency': 0}, {'interval_seconds': 0}])
def test_invalid_budgets_are_rejected(kwargs):
    with pytest.raises(ValueError):
        SplitTrafficRuntime(**kwargs)


async def test_completed_but_stale_fleet_round_is_not_ready():
    runtime = SplitTrafficRuntime(enabled=True)
    runtime.task = asyncio.current_task()
    runtime._process_scope = AsyncMock(return_value=datetime.now(UTC) - timedelta(seconds=120))
    await runtime._guard_scope(SimpleNamespace(panel_scope='scope'))
    assert not runtime.ready('scope')
    runtime._process_scope = AsyncMock(return_value=None)
    await runtime._guard_scope(SimpleNamespace(panel_scope='scope'))
    assert 'scope' not in runtime._healthy


async def test_optional_drop_warning_does_not_revoke_purchase_readiness(monkeypatch):
    from app.services.bs_traffic import runtime as module

    runtime = SplitTrafficRuntime(enabled=True)
    runtime.task = asyncio.current_task()
    runtime._healthy['scope'] = asyncio.get_running_loop().time()
    runtime._drop_warnings['scope'] = 'connection_drop_unknown'
    monkeypatch.setattr(module, 'split_traffic_runtime', runtime)
    assert module.runtime_status('scope') == {
        'purchase_ready': True,
        'warnings': ['connection_drop_unknown'],
    }
    runtime._healthy.clear()
    assert module.runtime_status('scope')['purchase_ready'] is False


@pytest.mark.parametrize(('bound', 'more', 'delay'), [(False, False, 30), (True, False, 1), (True, True, 0.05)])
async def test_idle_scope_waits_full_interval_but_owned_work_stays_prompt(monkeypatch, bound, more, delay):
    runtime = SplitTrafficRuntime(enabled=True, session_factory=object(), service_factory=object())

    async def run_once():
        runtime._has_bindings = bound
        runtime._more = more

    waits = []

    async def wait_for(awaitable, timeout):
        awaitable.close()
        waits.append(timeout)
        runtime._stop.set()

    runtime.run_once = run_once
    monkeypatch.setattr(asyncio, 'wait_for', wait_for)
    await runtime._run()
    assert waits == [delay]


async def test_enabled_unconfigured_start_stop_restart_is_noop():
    runtime = SplitTrafficRuntime(
        enabled=True,
        session_factory=object(),
        service_factory=lambda: SimpleNamespace(is_configured=False),
    )
    for _ in range(2):
        await runtime.start()
        task = runtime.task
        await asyncio.sleep(0)
        assert task is not None and not task.done()
        assert not runtime.ready('scope')
        await runtime.stop()
        assert task.done() and runtime.task is None
