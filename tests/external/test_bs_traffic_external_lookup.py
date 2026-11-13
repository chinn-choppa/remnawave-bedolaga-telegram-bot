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


async def test_real_bounded_http_preserves_specific_absence_without_private_body():
    import aiohttp
    from aiohttp import web

    from app.external.remnawave_api import RemnaWaveAPI

    async def missing(request):
        return web.json_response({'errorCode': 'A182', 'message': 'synthetic-private-marker'}, status=404)

    app = web.Application()
    app.router.add_get('/api/external-squads/{uuid}', missing)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        api = RemnaWaveAPI(f'http://127.0.0.1:{port}', 'synthetic')
        async with aiohttp.ClientSession() as session:
            api.session = session
            assert await api.split_external_squad_exists(UUID) is False
            with pytest.raises(RemnaWaveAPIError) as caught:
                await api._make_request(
                    'GET',
                    f'/api/external-squads/{UUID}',
                    retry_transient=False,
                    expected_status=200,
                    max_response_bytes=1024**2,
                )
            assert caught.value.response_data == {'errorCode': 'A182'}
            assert 'synthetic-private-marker' not in str(caught.value)
    finally:
        await runner.cleanup()


@pytest.mark.parametrize(
    'body,encoding,content_type',
    [
        (b'{"errorCode":"A182","message":"' + b'x' * 4096 + b'"}', None, 'application/json'),
        (b'{"errorCode":"A182"}', 'gzip', 'application/json'),
        (b'{"errorCode":"A182"}', None, 'text/html'),
        (b'{"errorCode":"A182"', None, 'application/json'),
        (b'{"errorCode":"A182-private-marker"}', None, 'application/json'),
        (b'[]', None, 'application/json'),
        (b'{"errorCode":"A039","errorCode":"A182"}', None, 'application/json'),
        (b'{"errorCode":"A182","errorCode":"A182"}', None, 'application/json'),
    ],
)
async def test_ambiguous_bounded_not_found_never_becomes_absence(body, encoding, content_type):
    from tests.external.test_bs_traffic_bounded_usage import api_for

    api, _, response = api_for(body, status=404, encoding=encoding)
    response.headers['Content-Type'] = content_type
    with pytest.raises(RemnaWaveAPIError) as caught:
        await api.split_external_squad_exists(UUID)
    assert caught.value.status_code == 404 and caught.value.response_data is None
    assert 'private-marker' not in str(caught.value)
    if encoding or content_type != 'application/json':
        response.content.read.assert_not_awaited()
