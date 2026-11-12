from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.cabinet.routes.subscription_modules import split_traffic
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    'owner,mode,expected', [(1, 'split', 200), (2, 'split', 404), (1, 'legacy', 404), (1, 'split_pending', 404)]
)
async def test_quote_http_uses_owned_persisted_scope(postgres_database, monkeypatch, owner, mode, expected):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        from app.database.models import Subscription

        subscription = await db.get(Subscription, target['subscription_id'])
        subscription.traffic_accounting_mode = mode
        await db.commit()
        before = await totals(db)

        @asynccontextmanager
        async def api_client():
            yield reader._api

        service = Mock()
        service.get_api_client = api_client
        make_service = Mock(return_value=service)
        monkeypatch.setattr(split_traffic, 'RemnaWaveService', make_service)
        monkeypatch.setattr(split_traffic, 'AsyncSessionLocal', reader._factory)
        app = FastAPI()
        app.include_router(split_traffic.router)
        app.dependency_overrides[get_current_cabinet_user] = lambda: SimpleNamespace(id=owner)
        app.dependency_overrides[get_cabinet_db] = lambda: db
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            response = await http.post(
                '/traffic/split-quote',
                params={'subscription_id': target['subscription_id']},
                json={'group': 'bs', 'gb': 10},
            )
        assert response.status_code == expected, response.text
        if expected == 200:
            assert response.json()['price_kopeks'] == 2500
            assert response.json()['subscription_id'] == target['subscription_id']
        else:
            make_service.assert_not_called()
            reader._api.get_split_access_snapshot.assert_not_awaited()
        db.expire_all()
        assert await totals(db) == before
        reader._api.update_internal_squads_once.assert_not_awaited()
