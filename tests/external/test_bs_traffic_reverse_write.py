from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from app.external.remnawave_api import RemnaWaveAPI, RemnaWaveAPIError
from tests.services.test_bs_traffic_grace_confirmation import state


SQUAD = '77777777-7777-4777-8777-777777777777'


def target():
    return replace(
        state(),
        expire_at=datetime.now(UTC) + timedelta(hours=1),
        traffic_limit_bytes=1100,
        active_internal_squads=[{'uuid': SQUAD}],
    )


@pytest.mark.parametrize('status', [200, 307, 429, 503])
async def test_real_reverse_http_positive_quota_single_patch_no_counter_reset(status, monkeypatch):
    import aiohttp
    from aiohttp import web

    from app.services.panel_sync.writer import patch_split_reverse_target

    requests, redirects = [], []

    async def patch(request):
        requests.append(await request.json())
        return web.json_response({'response': {'id': 12}}, status=status, headers={'Location': '/elsewhere'})

    async def redirect(request):
        redirects.append(True)
        return web.json_response({'response': {'id': 12}})

    app = web.Application()
    app.router.add_patch('/api/users', patch)
    app.router.add_route('*', '/elsewhere', redirect)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    monkeypatch.setattr(RemnaWaveAPI, '_throttled_until', 0.0)
    try:
        api = RemnaWaveAPI(f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}', 'synthetic')
        async with aiohttp.ClientSession() as session:
            api.session = session
            if status == 200:
                await patch_split_reverse_target(api, target=target())
            else:
                with pytest.raises(RemnaWaveAPIError):
                    await patch_split_reverse_target(api, target=target())
        assert len(requests) == 1 and not redirects
        assert requests[0]['trafficLimitBytes'] == 1100 and requests[0]['status'] == 'ACTIVE'
        assert requests[0]['trafficLimitStrategy'] == 'NO_RESET'
        assert requests[0]['activeInternalSquads'] == [SQUAD]
        assert requests[0]['externalSquadUuid'] is None
        assert 'usedTrafficBytes' not in requests[0]
    finally:
        RemnaWaveAPI._throttled_until = 0.0
        await runner.cleanup()
