"""Narrow split access transport never mutates quotas or external squads."""

from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from app.services.panel_sync.writer import patch_split_internal_squads
from tests.external.test_remnawave_3_4_3_contract import _api, _user_payload


SQUAD = '11111111-1111-4111-8111-111111111111'


@pytest.mark.parametrize('squads', [(), (SQUAD,)])
async def test_narrow_payload_preserves_other_fields_and_explicit_empty(squads):
    api = _api()
    api._make_request = AsyncMock(return_value={'response': _user_payload()})
    await patch_split_internal_squads(api, user_id=42, squads=squads)
    api._make_request.assert_awaited_once_with(
        'PATCH',
        '/api/users',
        {'id': 42, 'activeInternalSquads': list(squads)},
        retry_transient=False,
        expected_status=200,
    )
    api.enrich_user_with_happ_link.assert_not_awaited()


@pytest.mark.parametrize(
    'user_id,squads', [(True, ()), (0, ()), ('42', ()), (42, None), (42, ('bad',)), (42, (SQUAD, SQUAD))]
)
async def test_invalid_selection_fails_before_network(user_id, squads):
    api = _api()
    api._make_request = AsyncMock()
    with pytest.raises(ValueError):
        await patch_split_internal_squads(api, user_id=user_id, squads=squads)
    api._make_request.assert_not_awaited()


@pytest.mark.parametrize('error', [TimeoutError(), RemnaWaveAPIError('Update failed', 500, {'errorCode': 'A039'})])
async def test_failure_is_not_retried_or_narrowed(error):
    api = _api()
    api._make_request = AsyncMock(side_effect=error)
    with pytest.raises(type(error)):
        await patch_split_internal_squads(api, user_id=42, squads=(SQUAD,))
    assert api._make_request.await_count == 1


async def test_different_account_response_is_not_a_success():
    api = _api()
    api._make_request = AsyncMock(return_value={'response': _user_payload(id=43)})
    with pytest.raises(RemnaWaveAPIError):
        await patch_split_internal_squads(api, user_id=42, squads=(SQUAD,))


@pytest.mark.parametrize('status', [200, 302, 307, 429, 503])
async def test_real_http_does_not_follow_redirects_or_retry(status, monkeypatch):
    import aiohttp
    from aiohttp import web

    from app.external.remnawave_api import RemnaWaveAPI

    requests = []
    redirected = []

    async def handler(request):
        requests.append(await request.json())
        return web.json_response({'response': _user_payload()}, status=status, headers={'Location': '/redirected'})

    async def redirect(request):
        redirected.append(True)
        return web.json_response({'response': _user_payload()})

    app = web.Application()
    app.router.add_patch('/api/users', handler)
    app.router.add_route('*', '/redirected', redirect)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    monkeypatch.setattr(RemnaWaveAPI, '_throttled_until', 0.0)
    try:
        port = site._server.sockets[0].getsockname()[1]
        api = RemnaWaveAPI(f'http://127.0.0.1:{port}', 'synthetic')
        async with aiohttp.ClientSession() as session:
            api.session = session
            if status == 200:
                await patch_split_internal_squads(api, user_id=42, squads=())
            else:
                with pytest.raises(RemnaWaveAPIError):
                    await patch_split_internal_squads(api, user_id=42, squads=())
        assert requests == [{'id': 42, 'activeInternalSquads': []}]
        assert redirected == []
    finally:
        RemnaWaveAPI._throttled_until = 0.0
        await runner.cleanup()
