"""The destructive drop POST is scoped and never silently replayed."""

from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from app.external import remnawave_api as mod
from tests.external.test_remnawave_3_0_0 import _api_with_session, _FakeResponse


NODE = '00000000-0000-4000-8000-000000000001'


async def test_drop_uses_one_user_and_explicit_unique_nodes():
    api, session = _api_with_session(_FakeResponse(202))
    assert await api.request_user_connection_drop(42, [NODE, NODE]) is None
    assert session.last['json'] == {
        'dropBy': {'by': 'userIds', 'userIds': [42]},
        'targetNodes': {'target': 'specificNodes', 'nodeUuids': [NODE]},
    }
    assert session.last['url'] == 'http://panel.local/api/connections/drop'
    assert session.last['method'] == 'POST'
    assert session.last['allow_redirects'] is False


@pytest.mark.parametrize('user', [True, False, None, -1, 0, 1.5, '42', 2**53])
async def test_invalid_user_is_rejected_before_network(user):
    api, session = _api_with_session(_FakeResponse(202))
    with pytest.raises(ValueError):
        await api.request_user_connection_drop(user, [NODE])
    assert session.calls == []


@pytest.mark.parametrize('nodes', [None, [], NODE, ['allNodes'], [None], [1], [NODE] * 101])
async def test_invalid_or_unbounded_node_selection_never_broadens_target(nodes):
    api, session = _api_with_session(_FakeResponse(202))
    with pytest.raises(ValueError):
        await api.request_user_connection_drop(42, nodes)
    assert session.calls == []


@pytest.mark.parametrize('status', [200, 204, 301, 302, 307, 308, 429, 502, 503, 504])
async def test_unexpected_ack_or_error_is_not_retried(monkeypatch, status):
    monkeypatch.setattr(mod.RemnaWaveAPI, '_throttled_until', 0.0)
    monkeypatch.setattr(mod.asyncio, 'sleep', AsyncMock())
    api, session = _api_with_session(_FakeResponse(status))
    with pytest.raises(mod.RemnaWaveAPIError):
        await api.request_user_connection_drop(42, [NODE])
    assert len(session.calls) == 1
    monkeypatch.setattr(mod.RemnaWaveAPI, '_throttled_until', 0.0)


async def test_lost_response_is_not_replayed():
    api, _ = _api_with_session(_FakeResponse(202))
    api.session.request = Mock(side_effect=aiohttp.ClientConnectionError('response lost'))
    with pytest.raises(mod.RemnaWaveAPIError):
        await api.request_user_connection_drop(42, [NODE])
    assert api.session.request.call_count == 1
