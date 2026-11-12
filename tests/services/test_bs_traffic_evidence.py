import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.topology import TopologyConflict


A = '00000000-0000-4000-8000-000000000001'
B = '00000000-0000-4000-8000-000000000002'
N1 = '00000000-0000-4000-8000-000000000003'
N2 = '00000000-0000-4000-8000-000000000004'
NOW = datetime(2030, 1, 1, tzinfo=UTC)


def api():
    async def topology(squad):
        return {
            'squadUuid': squad,
            'accessibleNodes': [
                {'uuid': N1 if squad == A else N2, 'configProfileUuid': A, 'activeInbounds': ['test-inbound']}
            ],
        }

    return SimpleNamespace(
        get_system_configuration=AsyncMock(
            return_value={
                'service': {'disableUserUsageRecords': False, 'cleanUsageHistory': False},
                'misc': {'userUsageIgnoreBelowBytes': 0},
            }
        ),
        get_internal_squad_topology=AsyncMock(side_effect=topology),
    )


async def collect(client, **kwargs):
    return await collect_topology_evidence(
        client, panel_identity='test-panel', ordinary_squads=[A], bs_squads=[B], **kwargs
    )


async def test_evidence_is_bound_to_panel_and_expires_from_start_of_read():
    client = api()
    moments = iter([NOW, NOW + timedelta(seconds=4)])
    evidence = await collect(client, clock=lambda: next(moments), max_age_seconds=10)
    assert evidence.panel_identity == 'test-panel'
    assert evidence.valid_until == NOW + timedelta(seconds=10)
    assert evidence.is_fresh(NOW + timedelta(seconds=9), panel_identity='test-panel')
    assert not evidence.is_fresh(NOW + timedelta(seconds=10), panel_identity='test-panel')
    assert not evidence.is_fresh(NOW, panel_identity='other-panel')
    assert not evidence.is_fresh(NOW - timedelta(seconds=1), panel_identity='test-panel')
    assert client.get_internal_squad_topology.await_count == 2


@pytest.mark.parametrize(
    ('section', 'key', 'value'),
    [
        ('service', 'disableUserUsageRecords', True),
        ('service', 'disableUserUsageRecords', 'false'),
        ('service', 'cleanUsageHistory', True),
        ('service', 'cleanUsageHistory', None),
        ('misc', 'userUsageIgnoreBelowBytes', 1),
        ('misc', 'userUsageIgnoreBelowBytes', False),
        ('misc', 'userUsageIgnoreBelowBytes', '0'),
    ],
)
async def test_accounting_configuration_must_be_explicit_and_compatible(section, key, value):
    client = api()
    client.get_system_configuration.return_value[section][key] = value
    with pytest.raises(TopologyConflict, match='accounting_configuration'):
        await collect(client)
    client.get_internal_squad_topology.assert_not_awaited()


@pytest.mark.parametrize('document', [None, {}, {'squadUuid': A}, {'squadUuid': A, 'accessibleNodes': None}])
async def test_malformed_evidence_never_validates_as_empty_access(document):
    client = api()
    client.get_internal_squad_topology = AsyncMock(return_value=document)
    with pytest.raises(TopologyConflict):
        await collect(client)


async def test_response_for_different_squad_cannot_prove_topology():
    client = api()
    client.get_internal_squad_topology = AsyncMock(return_value={'squadUuid': B, 'accessibleNodes': [{'uuid': N1}]})
    with pytest.raises(TopologyConflict, match='squad_identity'):
        await collect(client)


async def test_changed_mapping_changes_fingerprint():
    client = api()
    first = await collect(client)

    async def changed(squad):
        return {
            'squadUuid': squad,
            'accessibleNodes': [
                {'uuid': N2 if squad == A else N1, 'configProfileUuid': A, 'activeInbounds': ['test-inbound']}
            ],
        }

    client.get_internal_squad_topology.side_effect = changed
    second = await collect(client)
    assert first.fingerprint != second.fingerprint


async def test_snapshot_that_took_too_long_is_not_fresh_on_arrival():
    moments = iter([NOW, NOW + timedelta(seconds=11)])
    with pytest.raises(TopologyConflict, match='stale_evidence'):
        await collect(api(), clock=lambda: next(moments), max_age_seconds=10)


async def test_request_budget_cancels_hung_lookup():
    client = api()

    async def hang(squad):
        await asyncio.Event().wait()

    client.get_internal_squad_topology.side_effect = hang
    with pytest.raises(TimeoutError):
        await collect(client, timeout_seconds=0.01)


async def test_external_cancellation_is_not_swallowed():
    client = api()
    client.get_system_configuration.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await collect(client)


async def test_squad_count_is_bounded_before_any_request():
    client = api()
    with pytest.raises(TopologyConflict, match='too_many_squads'):
        await collect(client, max_squads=1)
    client.get_system_configuration.assert_not_awaited()


async def test_node_response_count_is_bounded():
    client = api()
    client.get_internal_squad_topology = AsyncMock(
        return_value={
            'squadUuid': A,
            'accessibleNodes': [{'uuid': N1}, {'uuid': N2}],
        }
    )
    with pytest.raises(TopologyConflict, match='too_many_nodes'):
        await collect(client, max_nodes_per_squad=1)


async def test_profile_change_on_same_node_invalidates_fingerprint():
    client = api()
    original = client.get_internal_squad_topology.side_effect

    async def with_profile(squad):
        document = await original(squad)
        document['accessibleNodes'][0].update(configProfileUuid=A, activeInbounds=['inbound-a'])
        return document

    client.get_internal_squad_topology.side_effect = with_profile
    first = await collect(client)

    async def changed_profile(squad):
        document = await with_profile(squad)
        document['accessibleNodes'][0]['configProfileUuid'] = B
        return document

    client.get_internal_squad_topology.side_effect = changed_profile
    assert (await collect(client)).fingerprint != first.fingerprint


@pytest.mark.parametrize('invalid', [None, 'inbound', [], [None]])
async def test_unknown_or_empty_inbounds_do_not_prove_access(invalid):
    client = api()
    original = client.get_internal_squad_topology.side_effect

    async def invalid_inbounds(squad):
        document = await original(squad)
        document['accessibleNodes'][0].update(configProfileUuid=A, activeInbounds=invalid)
        return document

    client.get_internal_squad_topology.side_effect = invalid_inbounds
    with pytest.raises(TopologyConflict, match='invalid_inbounds'):
        await collect(client)


@pytest.mark.parametrize('selection', [None, 1, False, A, {A: True}])
async def test_non_list_selection_is_a_configuration_conflict_before_network(selection):
    client = api()
    with pytest.raises(TopologyConflict, match='invalid_squads'):
        await collect_topology_evidence(client, panel_identity='test-panel', ordinary_squads=selection, bs_squads=[B])
    client.get_system_configuration.assert_not_awaited()
