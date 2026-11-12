from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.cabinet.dependencies import get_current_cabinet_user
from app.cabinet.routes.subscription_modules import split_traffic
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.purchase import PackageQuote


@pytest.fixture
def quote_app(monkeypatch):
    app = FastAPI()
    app.include_router(split_traffic.router)
    app.dependency_overrides[get_current_cabinet_user] = lambda: SimpleNamespace(id=7)
    app.dependency_overrides[split_traffic.get_quote_reader] = lambda: object()
    quote = AsyncMock(
        return_value=PackageQuote(42, 'bs', 10, 30, 2500, 2000, 20, 1000, 1000, datetime(2030, 1, 1, tzinfo=UTC))
    )
    monkeypatch.setattr(split_traffic, 'prepare_package_quote', quote)
    return app, quote


async def test_quote_uses_authenticated_user_and_explicit_subscription(quote_app):
    app, quote = quote_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.post('/traffic/split-quote?subscription_id=42', json={'group': 'bs', 'gb': 10})
    assert response.status_code == 200
    assert response.json()['missing_kopeks'] == 1000
    assert response.json()['price_kopeks'] == 2000
    assert quote.await_args.kwargs == {'user_id': 7, 'subscription_id': 42, 'group': 'bs', 'gb': 10}


@pytest.mark.parametrize(
    'body',
    [
        {'group': 'all', 'gb': 10},
        {'group': 'bs', 'gb': True},
        {'group': 'bs', 'gb': 0},
        {'group': 'bs', 'gb': 100001},
        {'group': 'bs', 'gb': 10, 'user_id': 8},
    ],
)
async def test_invalid_terms_cannot_reach_quote(quote_app, body):
    app, quote = quote_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.post('/traffic/split-quote?subscription_id=42', json=body)
    assert response.status_code == 422
    quote.assert_not_awaited()


@pytest.mark.parametrize('error,status', [(LedgerConflict('private details'), 409), (TimeoutError('private'), 503)])
async def test_failures_are_sanitized(quote_app, error, status):
    app, quote = quote_app
    quote.side_effect = error
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.post('/traffic/split-quote?subscription_id=42', json={'group': 'bs', 'gb': 10})
    assert response.status_code == status
    assert 'private' not in response.text


async def test_panel_configuration_error_is_sanitized():
    from contextlib import asynccontextmanager
    from unittest.mock import Mock, patch

    from fastapi import HTTPException

    from app.services.remnawave_service import RemnaWaveConfigurationError

    @asynccontextmanager
    async def unavailable():
        raise RemnaWaveConfigurationError('private configuration')
        yield  # pragma: no cover

    service = Mock()
    service.get_api_client = unavailable
    with patch.object(split_traffic, 'RemnaWaveService', return_value=service):
        generator = split_traffic.get_quote_reader(
            subscription_id=42, user=SimpleNamespace(id=7), db=SimpleNamespace(scalar=AsyncMock(return_value='scope'))
        )
        with pytest.raises(HTTPException) as error:
            await anext(generator)
        assert error.value.status_code == 503
        assert 'private' not in error.value.detail


async def test_missing_subscription_id_is_not_defaulted(quote_app):
    app, quote = quote_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.post('/traffic/split-quote', json={'group': 'bs', 'gb': 10})
    assert response.status_code == 422
    quote.assert_not_awaited()
