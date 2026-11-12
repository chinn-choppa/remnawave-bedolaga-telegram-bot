from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.cabinet.routes.subscription_modules import split_traffic
from app.services.bs_traffic import customer_summary as service
from app.services.bs_traffic.ledger import LedgerConflict


@pytest.fixture
def summary_app(monkeypatch):
    app = FastAPI()
    app.include_router(split_traffic.router)
    app.dependency_overrides[get_current_cabinet_user] = lambda: SimpleNamespace(id=7)
    db = object()
    app.dependency_overrides[get_cabinet_db] = lambda: db
    group = {
        'mode': 'unlimited',
        'remaining_bytes': None,
        'pending_bytes': '0',
        'pending_operations': 0,
        'needs_review': False,
    }
    summary = AsyncMock(
        return_value={
            'subscription_id': 42,
            'calculated_at': datetime(2030, 1, 1, tzinfo=UTC),
            'grace_active': False,
            'groups': {'ordinary': group, 'bs': group},
        }
    )
    monkeypatch.setattr(service, 'customer_summary', summary)
    return app, summary, db


async def test_summary_uses_authenticated_owner_and_serializes_unlimited(summary_app):
    app, summary, db = summary_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
        response = await http.get('/traffic/split-summary?subscription_id=42')
    assert response.status_code == 200
    assert response.json()['groups']['ordinary']['remaining_bytes'] is None
    summary.assert_awaited_once_with(db, user_id=7, subscription_id=42)


@pytest.mark.parametrize('query', ['', '?subscription_id=0', '?subscription_id=-1', '?subscription_id=true'])
async def test_summary_requires_positive_explicit_selection(summary_app, query):
    app, summary, _ = summary_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
        response = await http.get('/traffic/split-summary' + query)
    assert response.status_code == 422
    summary.assert_not_awaited()


@pytest.mark.parametrize(
    'error,status', [(service.SplitSubscriptionNotFound('secret'), 404), (LedgerConflict('secret'), 409)]
)
async def test_summary_does_not_expose_internal_errors(summary_app, error, status):
    app, summary, _ = summary_app
    summary.side_effect = error
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
        response = await http.get('/traffic/split-summary?subscription_id=42')
    assert response.status_code == status
    assert 'secret' not in response.text
