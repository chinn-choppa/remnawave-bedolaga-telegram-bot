import httpx
import pytest
from fastapi import FastAPI

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.cabinet.routes.subscription_modules import traffic
from app.database.models import Subscription, User
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
@pytest.mark.parametrize(
    'method,path',
    [
        ('GET', '/traffic-packages'),
        ('POST', '/traffic'),
        ('POST', '/traffic/save-cart'),
        ('PUT', '/traffic'),
        ('POST', '/refresh-traffic'),
    ],
)
async def test_real_owned_split_request_cannot_enter_old_traffic_flow(
    postgres_database, monkeypatch, mode, method, path
):
    monkeypatch.setattr(type(traffic.settings), 'is_multi_tariff_enabled', lambda self: True)
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        sub.traffic_accounting_mode = mode
        await db.commit()
        before = await totals(db)
        user = await db.get(User, target['user_id'])
        app = FastAPI()
        app.include_router(traffic.router)
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        app.dependency_overrides[get_cabinet_db] = lambda: db
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            response = await http.request(method, path, params={'subscription_id': sub.id}, json={'gb': 10})
        assert response.status_code == 409, response.text
        assert await totals(db) == before
        reader._api.get_split_access_snapshot.assert_not_awaited()
        reader._api.update_internal_squads_once.assert_not_awaited()
