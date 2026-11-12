from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_remnawave_3_4_3_contract import _api


def payload():
    return dict(
        id=12,
        createdAt='2026-01-01T00:00:00Z',
        activeInternalSquads=[],
        status='ACTIVE',
        expireAt='2030-01-01T00:00:00Z',
        trafficLimitBytes=100,
        trafficLimitStrategy='NO_RESET',
        userTraffic={'usedTrafficBytes': 10},
        hwidDeviceLimit=None,
        externalSquadUuid=None,
        lastTrafficResetAt=None,
    )


async def test_full_grace_evidence_preserves_explicit_nulls_and_uses_one_bounded_read():
    api = _api()
    api._make_request = AsyncMock(return_value={'response': payload()})
    result = await api.get_split_grace_snapshot(12)
    assert result.id == 12 and result.hwid_device_limit is None
    assert result.status == 'ACTIVE' and result.traffic_limit_bytes == 100
    assert result.used_traffic_bytes == 10 and result.external_squad_uuid is None
    assert result.expire_at == datetime(2030, 1, 1, tzinfo=UTC)
    api._make_request.assert_awaited_once_with(
        'GET', '/api/users/12', retry_transient=False, expected_status=200, max_response_bytes=1024**2
    )
    api.enrich_user_with_happ_link.assert_not_awaited()


@pytest.mark.parametrize(
    'field',
    [
        'status',
        'expireAt',
        'trafficLimitBytes',
        'trafficLimitStrategy',
        'userTraffic',
        'hwidDeviceLimit',
        'externalSquadUuid',
        'lastTrafficResetAt',
    ],
)
async def test_missing_full_field_cannot_authorize_restore(field):
    data = payload()
    del data[field]
    api = _api()
    api._make_request = AsyncMock(return_value={'response': data})
    with pytest.raises(RemnaWaveAPIError):
        await api.get_split_grace_snapshot(12)


@pytest.mark.parametrize(
    'change',
    [
        {'status': 'active'},
        {'status': 'UNKNOWN'},
        {'expireAt': '2030-01-01'},
        {'trafficLimitBytes': True},
        {'trafficLimitBytes': -1},
        {'trafficLimitBytes': '100'},
        {'trafficLimitBytes': 2**53},
        {'trafficLimitStrategy': 'UNKNOWN'},
        {'userTraffic': {}},
        {'userTraffic': {'usedTrafficBytes': -1}},
        {'userTraffic': {'usedTrafficBytes': True}},
        {'userTraffic': {'usedTrafficBytes': '10'}},
        {'hwidDeviceLimit': True},
        {'hwidDeviceLimit': -(2**53)},
        {'hwidDeviceLimit': '2'},
        {'externalSquadUuid': ''},
        {'externalSquadUuid': 'invalid'},
        {'lastTrafficResetAt': '2026-01-01'},
    ],
)
async def test_invalid_full_state_is_rejected_without_coercion(change):
    api = _api()
    api._make_request = AsyncMock(return_value={'response': payload() | change})
    with pytest.raises(RemnaWaveAPIError):
        await api.get_split_grace_snapshot(12)


@pytest.mark.parametrize('devices', [None, 0, 5, -1])
async def test_explicit_device_limit_and_external_squad_are_preserved(devices):
    external = '99999999-9999-4999-8999-999999999999'
    api = _api()
    api._make_request = AsyncMock(
        return_value={
            'response': payload()
            | {
                'hwidDeviceLimit': devices,
                'externalSquadUuid': external,
                'lastTrafficResetAt': '2026-01-01T01:00:00+01:00',
                'trafficLimitStrategy': 'MONTH',
            }
        }
    )
    result = await api.get_split_grace_snapshot(12)
    assert result.hwid_device_limit == devices
    assert result.external_squad_uuid == external
    assert result.last_traffic_reset_at == datetime(2026, 1, 1, tzinfo=UTC)
    assert result.traffic_limit_strategy == 'MONTH'
