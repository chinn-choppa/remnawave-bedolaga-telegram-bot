from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from tests.external.test_remnawave_3_4_3_contract import _api
from tests.services.test_bs_traffic_grace_confirmation import state


async def test_full_restore_is_one_patch_without_counter_or_reset():
    from app.services.panel_sync.writer import patch_split_grace_target

    api = _api()
    api._make_request = AsyncMock(return_value={'response': {'id': 12}})
    await patch_split_grace_target(api, target=state())
    api._make_request.assert_awaited_once_with(
        'PATCH',
        '/api/users',
        {
            'id': 12,
            'status': 'ACTIVE',
            'expireAt': state().expire_at.isoformat(),
            'trafficLimitBytes': 0,
            'trafficLimitStrategy': 'NO_RESET',
            'activeInternalSquads': [],
            'externalSquadUuid': None,
            'hwidDeviceLimit': 2,
        },
        retry_transient=False,
        expected_status=200,
        max_response_bytes=1024**2,
    )
    api.enrich_user_with_happ_link.assert_not_awaited()


@pytest.mark.parametrize(
    'change',
    [
        {'id': True},
        {'hwid_device_limit': -1},
        {'traffic_limit_bytes': 1},
        {'traffic_limit_strategy': 'MONTH'},
        {'status': 'LIMITED'},
        {'status': 'DISABLED', 'external_squad_uuid': '00000000-0000-4000-8000-000000000001'},
        {'active_internal_squads': [{'uuid': 'bad'}]},
    ],
)
async def test_invalid_target_never_reaches_network(change):
    from app.services.panel_sync.writer import patch_split_grace_target

    api = _api()
    api._make_request = AsyncMock()
    with pytest.raises(ValueError):
        await patch_split_grace_target(api, target=replace(state(), **change))
    api._make_request.assert_not_awaited()


async def test_timeout_is_not_retried():
    from app.services.panel_sync.writer import patch_split_grace_target

    api = _api()
    api._make_request = AsyncMock(side_effect=TimeoutError)
    with pytest.raises(TimeoutError):
        await patch_split_grace_target(api, target=state())
    api._make_request.assert_awaited_once()


async def test_disabled_empty_target_accepts_past_expiry_without_reset():
    from datetime import UTC, datetime, timedelta

    from app.services.panel_sync.writer import patch_split_restriction_target

    api = _api()
    api._make_request = AsyncMock(return_value={'response': {'id': 12}})
    target = replace(state(), status='DISABLED', expire_at=datetime.now(UTC) - timedelta(minutes=1))
    await patch_split_restriction_target(api, target=target)
    payload = api._make_request.call_args.args[2]
    assert payload['status'] == 'DISABLED'
    assert payload['activeInternalSquads'] == [] and payload['externalSquadUuid'] is None
    assert not {'expireAt', 'usedTrafficBytes', 'trafficLimitBytes', 'trafficLimitStrategy'} & payload.keys()
    assert api._make_request.call_args.kwargs['retry_transient'] is False
