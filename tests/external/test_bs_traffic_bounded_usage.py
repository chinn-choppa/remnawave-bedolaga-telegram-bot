"""Daily usage transport has a deadline and a hard uncompressed body budget."""

from datetime import date
from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_remnawave_3_0_0 import _api_with_session, _FakeResponse


NODE = '11111111-1111-4111-8111-111111111111'
DAY = date(2026, 11, 4)


def api_for(body=b'{"response":{"nodes":[]}}', status=200, encoding=None):
    response = _FakeResponse(status)
    response.content = type('Stream', (), {'read': AsyncMock(side_effect=[body, b''])})()
    response.text = AsyncMock(side_effect=AssertionError('Unbounded read'))
    if encoding:
        response.headers['Content-Encoding'] = encoding
    api, session = _api_with_session(response)
    return api, session, response


async def test_request_is_one_utc_day_scoped_and_without_implicit_retries():
    api, session, _ = api_for()
    assert await api.get_daily_nodes_usage((NODE,), DAY) == {'nodes': []}
    assert session.last['params'] == {'start': '2026-11-04', 'end': '2026-11-04', 'minTotalBytes': 0}
    assert session.last['json'] == {'nodesUuids': [NODE]}
    assert session.last['allow_redirects'] is False
    assert session.last['auto_decompress'] is False
    assert session.last['headers']['Accept-Encoding'] == 'identity'


async def test_oversized_chunked_response_is_rejected_before_json_decode():
    api, session, response = api_for(b'x' * 33)
    with pytest.raises(RemnaWaveAPIError, match='budget'):
        await api.get_daily_nodes_usage((NODE,), DAY, max_response_bytes=32)
    assert response.content.read.await_count == 1
    assert len(session.calls) == 1


@pytest.mark.parametrize('body', [b'bad json', b'[]', b'{"response":[]}', b'', b'\xff'])
async def test_invalid_response_is_not_converted_to_empty_usage(body):
    api, _, _ = api_for(body)
    with pytest.raises(RemnaWaveAPIError):
        await api.get_daily_nodes_usage((NODE,), DAY)


async def test_compressed_response_is_rejected_without_decompressing_or_reading():
    api, _, response = api_for(encoding='gzip')
    with pytest.raises(RemnaWaveAPIError, match='encoding'):
        await api.get_daily_nodes_usage((NODE,), DAY)
    response.content.read.assert_not_awaited()


async def test_total_deadline_includes_waiting_for_shared_throttle():
    import asyncio

    api, session, _ = api_for()

    async def wait():
        await asyncio.sleep(10)

    api._wait_for_shared_throttle = wait
    with pytest.raises(TimeoutError):
        await api.get_daily_nodes_usage((NODE,), DAY, timeout_seconds=0.01)
    assert session.calls == []


@pytest.mark.parametrize('status', [302, 307, 429, 502, 503])
async def test_errors_and_redirects_are_not_replayed(monkeypatch, status):
    from app.external.remnawave_api import RemnaWaveAPI

    monkeypatch.setattr(RemnaWaveAPI, '_throttled_until', 0.0)
    api, session, _ = api_for(status=status)
    try:
        with pytest.raises(RemnaWaveAPIError):
            await api.get_daily_nodes_usage((NODE,), DAY)
        assert len(session.calls) == 1
    finally:
        RemnaWaveAPI._throttled_until = 0.0


@pytest.mark.parametrize(
    'nodes,day,budget,timeout',
    [
        ((), DAY, 100, 1),
        ((NODE,) * 101, DAY, 100, 1),
        (('not-uuid',), DAY, 100, 1),
        ((NODE,), '2026-11-04', 100, 1),
        ((NODE,), DAY, True, 1),
        ((NODE,), DAY, 0, 1),
        ((NODE,), DAY, 9 * 1024**2, 1),
        ((NODE,), DAY, 100, float('nan')),
        ((NODE,), DAY, 100, True),
    ],
)
async def test_invalid_query_never_reaches_network(nodes, day, budget, timeout):
    api, session, _ = api_for()
    with pytest.raises(ValueError):
        await api.get_daily_nodes_usage(nodes, day, max_response_bytes=budget, timeout_seconds=timeout)
    assert not session.calls


async def test_limit_is_cumulative_and_exact_size_is_allowed():
    body = b'{"response":{"nodes":[]}}'
    api, _, response = api_for(body)
    response.content.read.side_effect = [body[:10], body[10:], b'']
    assert await api.get_daily_nodes_usage((NODE,), DAY, max_response_bytes=len(body)) == {'nodes': []}
    api, _, response = api_for(body)
    response.content.read.side_effect = [body[:10], body[10:], b'']
    with pytest.raises(RemnaWaveAPIError, match='budget'):
        await api.get_daily_nodes_usage((NODE,), DAY, max_response_bytes=len(body) - 1)


async def test_real_http_chunked_body_is_bounded_and_auth_header_is_preserved():
    import aiohttp
    from aiohttp import web

    from app.external.remnawave_api import RemnaWaveAPI

    received = []

    async def handler(request):
        received.append(dict(request.headers))
        assert await request.json() == {'nodesUuids': [NODE]}
        stream = web.StreamResponse()
        stream.enable_chunked_encoding()
        await stream.prepare(request)
        await stream.write(b'{"response":{"nodes":[]}}' + b' ' * 1024)
        await stream.write_eof()
        return stream

    app = web.Application()
    app.router.add_post('/api/bandwidth-stats/nodes/usage', handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        api = RemnaWaveAPI(f'http://127.0.0.1:{port}', 'synthetic')
        async with aiohttp.ClientSession(headers={'Authorization': 'Bearer synthetic'}) as session:
            api.session = session
            with pytest.raises(RemnaWaveAPIError, match='budget'):
                await api.get_daily_nodes_usage((NODE,), DAY, max_response_bytes=64)
        assert len(received) == 1
        assert received[0]['Authorization'] == 'Bearer synthetic'
        assert received[0]['Accept-Encoding'] == 'identity'
    finally:
        await runner.cleanup()


async def test_retry_after_is_available_to_durable_scheduler(monkeypatch):
    from app.external.remnawave_api import RemnaWaveAPI

    monkeypatch.setattr(RemnaWaveAPI, '_throttled_until', 0.0)
    api, _, response = api_for(status=429)
    response.headers['Retry-After'] = '600'
    try:
        with pytest.raises(RemnaWaveAPIError) as caught:
            await api.get_daily_nodes_usage((NODE,), DAY)
        assert caught.value.retry_after_seconds == 600
    finally:
        RemnaWaveAPI._throttled_until = 0.0


@pytest.mark.parametrize('value', ['nan', 'inf', '-inf'])
def test_nonfinite_retry_after_cannot_poison_shared_throttle(value):
    from app.external.remnawave_api import _retry_after_seconds

    assert _retry_after_seconds({'Retry-After': value}) is None


@pytest.mark.parametrize('status', [429, 503])
async def test_html_errors_keep_status_and_retry_after_without_reading_body(monkeypatch, status):
    from app.external.remnawave_api import RemnaWaveAPI, RemnaWaveTransientError

    monkeypatch.setattr(RemnaWaveAPI, '_throttled_until', 0.0)
    api, _, response = api_for(b'<html>proxy unavailable</html>', status=status)
    response.headers['Retry-After'] = '600'
    try:
        with pytest.raises(RemnaWaveTransientError) as caught:
            await api.get_daily_nodes_usage((NODE,), DAY)
        assert caught.value.status_code == status
        assert caught.value.retry_after_seconds == 600
        response.content.read.assert_not_awaited()
    finally:
        RemnaWaveAPI._throttled_until = 0.0


async def test_error_message_never_reaches_bounded_logs_or_exception_data(monkeypatch):
    from unittest.mock import Mock

    from app.external import remnawave_api as mod

    logger = Mock()
    monkeypatch.setattr(mod, 'logger', logger)
    api, _, _ = api_for(b'{"message":"synthetic-private-marker"}', status=500)
    with pytest.raises(RemnaWaveAPIError) as caught:
        await api.get_daily_nodes_usage((NODE,), DAY)
    assert 'synthetic-private-marker' not in repr(logger.mock_calls)
    assert 'synthetic-private-marker' not in str(caught.value)
    assert caught.value.response_data is None


def test_http_date_retry_after_is_supported():
    from datetime import UTC, datetime, timedelta
    from email.utils import format_datetime

    from app.external.remnawave_api import _retry_after_seconds

    header = format_datetime(datetime.now(UTC) + timedelta(seconds=600), usegmt=True)
    delay = _retry_after_seconds({'Retry-After': header})
    assert delay is not None and 590 <= delay <= 600
