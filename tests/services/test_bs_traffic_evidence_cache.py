import asyncio
from datetime import timedelta

import pytest

from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
from tests.services.test_bs_traffic_evidence import NOW, A, B, api


async def read(cache, client, **kwargs):
    return await cache.get(client, panel_identity='test-panel', ordinary_squads=[A], bs_squads=[B], **kwargs)


async def test_thousand_readers_share_one_collection_without_extending_freshness():
    client = api()
    configuration = client.get_system_configuration.return_value

    async def delayed_configuration():
        # Let competing readers reach the lock while collection is in flight.
        await asyncio.sleep(0.01)
        return configuration

    client.get_system_configuration.side_effect = delayed_configuration
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', clock=lambda: NOW)
    results = await asyncio.gather(*(read(cache, client) for _ in range(1000)))
    assert all(result is results[0] for result in results)
    assert results[0].valid_until == NOW + timedelta(seconds=30)
    assert client.get_system_configuration.await_count == 1
    assert client.get_internal_squad_topology.await_count == 2


async def test_expired_cache_never_masks_failure_and_recovers():
    client = api()
    now = NOW
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', clock=lambda: now)
    old = await read(cache, client)
    now += timedelta(seconds=30)
    client.get_system_configuration.side_effect = TimeoutError
    with pytest.raises(TimeoutError):
        await read(cache, client)
    client.get_system_configuration.side_effect = None
    assert (await read(cache, client)).observed_at > old.observed_at


async def test_client_and_scope_mismatch_rejected_before_network():
    client = api()
    cache = TopologyEvidenceCache(client, panel_identity='test-panel')
    with pytest.raises(ValueError):
        await read(cache, api())
    with pytest.raises(ValueError):
        await cache.get(client, panel_identity='other-panel', ordinary_squads=[A], bs_squads=[B])
    client.get_system_configuration.assert_not_awaited()


async def test_selection_separation_and_capacity_eviction():
    client = api()
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', capacity=1, clock=lambda: NOW)
    first = await read(cache, client)
    reverse = await cache.get(client, panel_identity='test-panel', ordinary_squads=[B], bs_squads=[A])
    assert reverse.fingerprint != first.fingerprint
    assert await read(cache, client) is not first
    assert client.get_system_configuration.await_count == 3


async def test_cancelled_collector_releases_lock_without_background_task():
    client = api()
    entered = asyncio.Event()

    async def blocked():
        entered.set()
        await asyncio.Event().wait()

    client.get_system_configuration.side_effect = blocked
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', clock=lambda: NOW)
    task = asyncio.create_task(read(cache, client))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    client.get_system_configuration.side_effect = None
    await asyncio.wait_for(read(cache, client), timeout=1)


@pytest.mark.parametrize('capacity', [0, -1, True, 1.5])
async def test_invalid_capacity(capacity):
    with pytest.raises(ValueError):
        TopologyEvidenceCache(api(), panel_identity='test-panel', capacity=capacity)


async def test_backwards_clock_discards_future_evidence():
    client = api()
    now = NOW
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', clock=lambda: now)
    first = await read(cache, client)
    now -= timedelta(seconds=1)
    second = await read(cache, client)
    assert second is not first
    assert second.observed_at == now


async def test_cancelled_waiter_does_not_cancel_collection():
    client = api()
    entered, release = asyncio.Event(), asyncio.Event()
    configuration = client.get_system_configuration.return_value

    async def blocked():
        entered.set()
        await release.wait()
        return configuration

    client.get_system_configuration.side_effect = blocked
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', clock=lambda: NOW)
    collector = asyncio.create_task(read(cache, client))
    await entered.wait()
    waiter = asyncio.create_task(read(cache, client))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()
    evidence = await collector
    assert await read(cache, client) is evidence
    assert client.get_system_configuration.await_count == 1


async def test_cached_waiters_do_not_take_panel_admission_slots():
    from contextlib import asynccontextmanager

    admitted = []

    @asynccontextmanager
    async def admission():
        admitted.append(True)
        yield

    client = api()
    cache = TopologyEvidenceCache(client, panel_identity='test-panel', clock=lambda: NOW)
    await asyncio.gather(*(read(cache, client, admission=admission) for _ in range(100)))
    assert len(admitted) == 1
