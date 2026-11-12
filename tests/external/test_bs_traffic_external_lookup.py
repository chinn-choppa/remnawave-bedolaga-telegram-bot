from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_remnawave_3_4_3_contract import _api


UUID = '88888888-8888-4888-8888-888888888888'


async def test_bounded_identity_lookup_preserves_custom_settings():
    api = _api()
    api._make_request = AsyncMock(
        return_value={
            'response': {'uuid': UUID, 'hwidSettings': {'enabled': False}, 'templates': [{'templateType': 'XRAY_JSON'}]}
        }
    )
    assert await api.split_external_squad_exists(UUID) is True
    api._make_request.assert_awaited_once_with(
        'GET', f'/api/external-squads/{UUID}', retry_transient=False, expected_status=200, max_response_bytes=1024**2
    )


@pytest.mark.parametrize(
    'code,status,missing', [('A182', 404, True), ('A039', 404, False), ('A182', 500, False), (None, 403, False)]
)
async def test_only_specific_not_found_is_absence(code, status, missing):
    api = _api()
    api._make_request = AsyncMock(side_effect=RemnaWaveAPIError('synthetic', status, {'errorCode': code}))
    if missing:
        assert await api.split_external_squad_exists(UUID) is False
    else:
        with pytest.raises(RemnaWaveAPIError):
            await api.split_external_squad_exists(UUID)
    assert api._make_request.await_count == 1


@pytest.mark.parametrize('value', [None, '', '../users', True])
async def test_invalid_identifier_does_not_make_request(value):
    api = _api()
    api._make_request = AsyncMock()
    with pytest.raises(ValueError):
        await api.split_external_squad_exists(value)
    api._make_request.assert_not_awaited()


@pytest.mark.parametrize('value', [None, [], {'response': {}}, {'response': {'uuid': 'other'}}])
async def test_wrong_or_missing_identity_cannot_authorize_assignment(value):
    api = _api()
    api._make_request = AsyncMock(return_value=value)
    with pytest.raises(RemnaWaveAPIError):
        await api.split_external_squad_exists(UUID)
