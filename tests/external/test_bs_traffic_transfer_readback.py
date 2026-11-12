from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_bs_traffic_grace_readback import payload
from tests.external.test_remnawave_3_4_3_contract import _api


async def test_transfer_reads_lifetime_and_identity_in_one_bounded_request():
    api = _api()
    data = payload()
    data['userTraffic']['lifetimeUsedTrafficBytes'] = 100
    api._make_request = AsyncMock(return_value={'response': data})
    result = await api.get_split_transfer_snapshot(12)
    assert result.lifetime_bytes == 100 and result.used_bytes == 10
    api._make_request.assert_awaited_once_with(
        'GET', '/api/users/12', retry_transient=False, expected_status=200, max_response_bytes=1024**2
    )


@pytest.mark.parametrize('lifetime', [None, True, -1, 3, 2**53, 100.5, '100'])
async def test_invalid_or_missing_lifetime_never_defaults_to_zero(lifetime):
    api = _api()
    data = payload()
    if lifetime is not None:
        data['userTraffic']['lifetimeUsedTrafficBytes'] = lifetime
    api._make_request = AsyncMock(return_value={'response': data})
    with pytest.raises(RemnaWaveAPIError):
        await api.get_split_transfer_snapshot(12)
