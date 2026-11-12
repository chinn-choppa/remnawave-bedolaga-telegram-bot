from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.cabinet.dependencies import get_current_cabinet_user
from app.cabinet.routes.subscription_modules import split_traffic
from app.services.bs_traffic.ledger import LedgerConflict


BODY = {'group': 'bs', 'gb': 10, 'expected_price_kopeks': 2000, 'operation_key': '00000000-0000-4000-8000-000000000001'}


@pytest.fixture
def purchase_app(monkeypatch):
    app = FastAPI()
    app.include_router(split_traffic.router)
    app.dependency_overrides[get_current_cabinet_user] = lambda: SimpleNamespace(id=7)
    app.dependency_overrides[split_traffic.get_quote_reader] = lambda: object()
    checkout = AsyncMock(
        return_value=SimpleNamespace(
            purchase=SimpleNamespace(price_kopeks=2000, replayed=False), delivery_status='pending'
        )
    )
    monkeypatch.setattr(split_traffic, 'checkout_package', checkout)
    return app, checkout


async def post(app, body=BODY):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        return await client.post('/traffic/split-purchase?subscription_id=42', json=body)


async def test_runtime_unavailable_never_debits(purchase_app):
    app, checkout = purchase_app
    assert (await post(app)).status_code == 503
    checkout.assert_not_awaited()


async def test_authenticated_id_exact_price_and_stable_operation_are_forwarded(purchase_app):
    app, checkout = purchase_app
    app.dependency_overrides[split_traffic.split_purchase_runtime_ready] = lambda: True
    response = await post(app)
    assert response.status_code == 200
    assert response.json() == {
        'subscription_id': 42,
        'price_kopeks': 2000,
        'delivery_status': 'pending',
        'replayed': False,
    }
    assert checkout.await_args.kwargs == {'user_id': 7, 'subscription_id': 42, **BODY}
    await post(app)
    assert checkout.await_args.kwargs['operation_key'] == BODY['operation_key']


@pytest.mark.parametrize(
    'extra',
    [
        {'user_id': 8},
        {'panel_scope': 'injected'},
        {'operation_key': 'bad'},
        {'expected_price_kopeks': True},
        {'expected_price_kopeks': -1},
    ],
)
async def test_invalid_purchase_rejected(purchase_app, extra):
    app, checkout = purchase_app
    app.dependency_overrides[split_traffic.split_purchase_runtime_ready] = lambda: True
    assert (await post(app, {**BODY, **extra})).status_code == 422
    checkout.assert_not_awaited()


@pytest.mark.parametrize('error,status', [(LedgerConflict('secret'), 409), (TimeoutError('secret'), 503)])
async def test_checkout_errors_sanitized(purchase_app, error, status):
    app, checkout = purchase_app
    app.dependency_overrides[split_traffic.split_purchase_runtime_ready] = lambda: True
    checkout.side_effect = error
    response = await post(app)
    assert response.status_code == status
    assert 'secret' not in response.text


async def test_readiness_uses_owned_persisted_scope(monkeypatch):
    from app.services.bs_traffic import runtime

    monkeypatch.setattr(runtime.split_traffic_runtime, 'enabled', True)
    scopes = []
    monkeypatch.setattr(runtime, 'runtime_ready', lambda scope: scopes.append(scope) or True)
    db = SimpleNamespace(scalar=AsyncMock(return_value='trusted-scope'))
    assert await split_traffic.split_purchase_runtime_ready(42, SimpleNamespace(id=7), db)
    assert scopes == ['trusted-scope']
    query = str(db.scalar.await_args.args[0])
    assert 'subscriptions.user_id' in query
    assert 'subscriptions.traffic_accounting_mode' in query
    db.scalar.return_value = None
    assert not await split_traffic.split_purchase_runtime_ready(43, SimpleNamespace(id=7), db)
    assert scopes == ['trusted-scope']
