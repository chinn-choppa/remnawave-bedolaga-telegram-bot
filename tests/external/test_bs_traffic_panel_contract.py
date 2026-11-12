from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_remnawave_3_4_3_contract import _api


SQUAD = '00000000-0000-4000-8000-000000000001'


async def test_strict_topology_lookup_preserves_missing_squad_error():
    api = _api()
    api._make_request = AsyncMock(side_effect=RemnaWaveAPIError('missing', 404))
    with pytest.raises(RemnaWaveAPIError):
        await api.get_internal_squad_topology(SQUAD)


async def test_topology_lookup_returns_full_document_for_strict_service_validation():
    api = _api()
    document = {'squadUuid': SQUAD, 'accessibleNodes': []}
    api._make_request = AsyncMock(return_value={'response': document})
    assert await api.get_internal_squad_topology(SQUAD) == document
    api._make_request.assert_awaited_once_with(
        'GET',
        f'/api/internal-squads/{SQUAD}/accessible-nodes',
        retry_transient=False,
        expected_status=200,
        max_response_bytes=8 * 1024**2,
    )


@pytest.mark.parametrize('squad', ['../../users', '', None, 'x?token=secret'])
async def test_topology_lookup_rejects_non_uuid_before_network(squad):
    api = _api()
    api._make_request = AsyncMock()
    with pytest.raises(ValueError):
        await api.get_internal_squad_topology(squad)
    api._make_request.assert_not_awaited()


@pytest.mark.parametrize('response', [{}, {'response': None}, {'response': []}])
async def test_missing_configuration_is_not_defaulted(response):
    api = _api()
    api._make_request = AsyncMock(return_value=response)
    with pytest.raises(RemnaWaveAPIError):
        await api.get_system_configuration()


async def test_configuration_preserves_explicit_false_and_zero():
    api = _api()
    document = {'service': {'disableUserUsageRecords': False}, 'misc': {'userUsageIgnoreBelowBytes': 0}}
    api._make_request = AsyncMock(return_value={'response': document})
    assert await api.get_system_configuration() == document
    api._make_request.assert_awaited_once_with(
        'GET', '/api/system/configuration', retry_transient=False, expected_status=200, max_response_bytes=1024**2
    )


@pytest.mark.parametrize('topology,budget', [(True, 8 * 1024**2), (False, 1024**2)])
async def test_topology_and_configuration_streams_are_bounded(topology, budget):
    from tests.external.test_bs_traffic_bounded_usage import api_for

    api, session, response = api_for(b'x' * (budget + 1))
    with pytest.raises(RemnaWaveAPIError, match='budget'):
        if topology:
            await api.get_internal_squad_topology(SQUAD)
        else:
            await api.get_system_configuration()
    assert len(session.calls) == 1
    response.text.assert_not_awaited()
