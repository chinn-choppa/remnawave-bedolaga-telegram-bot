from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_remnawave_3_4_3_contract import _api, _user_payload


@pytest.mark.parametrize('create', [False, True])
async def test_confirmed_missing_squad_is_never_sent_to_panel(create):
    api = _api()
    api.get_external_squad_by_uuid = AsyncMock(return_value=None)
    api._make_request = AsyncMock(return_value={'response': _user_payload()})
    if create:
        await api.create_user('user42', datetime(2030, 1, 1, tzinfo=UTC), external_squad_uuid='missing')
    else:
        await api.update_user(42, external_squad_uuid='missing')
    assert 'externalSquadUuid' not in api._make_request.call_args.args[2]


@pytest.mark.parametrize('lookup', [SimpleNamespace(uuid='live'), TimeoutError(), RemnaWaveAPIError('Forbidden', 403)])
async def test_generic_write_error_does_not_authorize_dropping_squad(lookup):
    api = _api()
    api.get_external_squad_by_uuid = AsyncMock(
        **({'side_effect': lookup} if isinstance(lookup, Exception) else {'return_value': lookup})
    )
    api._make_request = AsyncMock(side_effect=RemnaWaveAPIError('Update user error', 500, {'errorCode': 'A039'}))
    with pytest.raises(RemnaWaveAPIError):
        await api.update_user(42, external_squad_uuid='live')
    assert api._make_request.await_count == 1


@pytest.mark.parametrize('code', [None, 'A025', 'A039'])
async def test_unrelated_404_is_not_proof_of_deleted_external_squad(code):
    api = _api()
    api._make_request = AsyncMock(side_effect=RemnaWaveAPIError('not found', 404, {'errorCode': code}))
    with pytest.raises(RemnaWaveAPIError):
        await api.get_external_squad_by_uuid('unknown')


async def test_explicit_a182_is_proof_of_missing_squad():
    api = _api()
    api._make_request = AsyncMock(side_effect=RemnaWaveAPIError('External squad not found', 404, {'errorCode': 'A182'}))
    assert await api.get_external_squad_by_uuid('missing') is None


async def test_null_detach_is_preserved_without_catalog_lookup():
    api = _api()
    api.get_external_squad_by_uuid = AsyncMock()
    api._make_request = AsyncMock(return_value={'response': _user_payload()})
    await api.update_user(42, external_squad_uuid=None)
    assert api._make_request.call_args.args[2]['externalSquadUuid'] is None
    api.get_external_squad_by_uuid.assert_not_awaited()


async def test_slow_preflight_preserves_assignment_and_allows_write(monkeypatch):
    import asyncio

    from app.external import remnawave_api as module

    monkeypatch.setattr(module, 'EXTERNAL_SQUAD_LOOKUP_TIMEOUT', 0.01, raising=False)

    async def hang(uuid):
        await asyncio.Event().wait()

    api = _api()
    api.get_external_squad_by_uuid = hang
    api._make_request = AsyncMock(return_value={'response': _user_payload()})
    await asyncio.wait_for(api.update_user(42, external_squad_uuid='live'), timeout=0.25)
    assert api._make_request.call_args.args[2]['externalSquadUuid'] == 'live'


async def test_external_cancellation_is_not_swallowed():
    import asyncio

    api = _api()
    api.get_external_squad_by_uuid = AsyncMock(side_effect=asyncio.CancelledError())
    api._make_request = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await api.update_user(42, external_squad_uuid='live')
    api._make_request.assert_not_awaited()
