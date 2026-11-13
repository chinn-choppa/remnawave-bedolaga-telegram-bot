from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.cabinet.routes.subscription_modules import tariff_switch
from app.config import settings
from app.database.models import Transaction, User
from tests.database.test_bs_tariff_switch_postgres import setup_switch
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_http_switch_replay_and_foreign_owner(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, _ = await setup_switch(db, monkeypatch)
        sub_id, tariff_id = sub.id, new.id
        monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
        app = FastAPI()
        app.include_router(tariff_switch.router)
        user = await db.get(User, target['user_id'])
        user_id = user.id
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        app.dependency_overrides[get_cabinet_db] = lambda: db
        from app.cabinet.routes.subscription_modules import split_tariff_switch as adapter
        from app.services.bs_traffic import tariff_switch_checkout as checkout

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(adapter, '_reader', bound)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='verified'))
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        await db.commit()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            url = f'/tariff/switch?subscription_id={sub_id}'
            quote = await http.post(
                '/tariff/switch/preview',
                params={'subscription_id': sub_id},
                json={'tariff_id': tariff_id, 'period_days': 30},
            )
            assert quote.status_code == 200, quote.text
            terms = quote.json()
            assert terms['traffic_accounting_mode'] == 'split'
            assert terms.get('split_limits') == {
                'ordinary': {'mode': 'finite', 'limit_gb': 20},
                'bs': {'mode': 'finite', 'limit_gb': 10},
            }
            assert terms.get('device_limit') == 4
            request = dict(
                tariff_id=tariff_id,
                period_days=30,
                operation_id=str(uuid4()),
                expected_price_kopeks=terms['upgrade_cost_kopeks'],
                expected_terms_hash=terms['expected_terms_hash'],
            )
            response = await http.post(url, json=request)
            assert response.status_code == 200, response.text
            assert response.json()['charged_kopeks'] == terms['upgrade_cost_kopeks']
            # The immutable paid receipt remains readable after later mode,
            # availability and sales changes, without constructing a client.
            sub.traffic_accounting_mode = 'managed'
            sub.status = 'disabled'
            new.is_active = False
            await db.commit()
            monkeypatch.setattr(settings, 'SALES_MODE', 'classic')

            @asynccontextmanager
            async def offline(*args):
                raise AssertionError('Historical retry must not contact panel')
                yield

            monkeypatch.setattr(adapter, '_reader', offline)
            again = await http.post(url, json=request)
            assert again.status_code == 200, again.text
            assert again.json()['charged_kopeks'] == response.json()['charged_kopeks']
            foreign = await db.get(User, 2)
            app.dependency_overrides[get_current_cabinet_user] = lambda: foreign
            denied = await http.post(url, json=request)
            assert denied.status_code == 404, denied.text
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert (await db.get(User, user_id)).balance_kopeks == 10000 - terms['upgrade_cost_kopeks']


@pytest.mark.parametrize('confirmation', ['missing', 'price', 'terms'])
async def test_http_confirmation_rejection_has_no_charge(postgres_database, monkeypatch, confirmation):
    from app.cabinet.routes.subscription_modules import split_tariff_switch as adapter

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, _ = await setup_switch(db, monkeypatch)
        sub_id, tariff_id = sub.id, new.id
        app = FastAPI()
        app.include_router(tariff_switch.router)
        user = await db.get(User, target['user_id'])
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        app.dependency_overrides[get_cabinet_db] = lambda: db

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(adapter, '_reader', bound)
        await db.commit()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            response = await http.post(
                '/tariff/switch/preview',
                params={'subscription_id': sub_id},
                json={'tariff_id': tariff_id, 'period_days': 30},
            )
            assert response.status_code == 200, response.text
            quote = response.json()
            request = dict(
                tariff_id=tariff_id,
                period_days=30,
                operation_id=str(uuid4()),
                expected_price_kopeks=quote['upgrade_cost_kopeks'],
                expected_terms_hash=quote['expected_terms_hash'],
            )
            if confirmation == 'missing':
                request.pop('expected_terms_hash')
            elif confirmation == 'price':
                request['expected_price_kopeks'] += 1
            else:
                request['expected_terms_hash'] = 'f' * 64
            response = await http.post('/tariff/switch', params={'subscription_id': sub_id}, json=request)
            assert response.status_code == 409, response.text
            assert response.json()['detail']['code'] == (
                'split_switch_confirmation_required' if confirmation == 'missing' else 'split_switch_quote_changed'
            )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert (await db.get(User, target['user_id'], populate_existing=True)).balance_kopeks == 10000


@pytest.mark.parametrize('endpoint', ['/tariff/switch/preview', '/tariff/switch'])
@pytest.mark.parametrize('selection', ['foreign_body', 'mismatch'])
async def test_http_switch_honors_body_selection_and_rejects_mismatch(
    postgres_database, monkeypatch, endpoint, selection
):
    from app.cabinet.routes.subscription_modules import split_tariff_switch as adapter

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, _ = await setup_switch(db, monkeypatch)

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(adapter, '_reader', bound)
        app = FastAPI()
        app.include_router(tariff_switch.router)
        user = await db.get(User, target['user_id'])
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        app.dependency_overrides[get_cabinet_db] = lambda: db
        request = dict(tariff_id=new.id, period_days=30, subscription_id=999)
        params = {'subscription_id': sub.id} if selection == 'mismatch' else {}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            response = await http.post(endpoint, json=request, params=params)
            assert response.status_code == (400 if selection == 'mismatch' else 404), response.text
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_http_panel_timeout_is_retryable_without_charge(postgres_database, monkeypatch):
    from app.cabinet.routes.subscription_modules import split_tariff_switch as adapter

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, _ = await setup_switch(db, monkeypatch)
        app = FastAPI()
        app.include_router(tariff_switch.router)
        user = await db.get(User, target['user_id'])
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        app.dependency_overrides[get_cabinet_db] = lambda: db

        @asynccontextmanager
        async def unavailable(*args):
            raise TimeoutError
            yield

        monkeypatch.setattr(adapter, '_reader', unavailable)
        payload = dict(tariff_id=new.id, period_days=30, subscription_id=sub.id)
        await db.commit()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            response = await http.post('/tariff/switch/preview', json=payload)
            assert response.status_code == 503
            assert response.json()['detail']['code'] == 'split_switch_temporarily_unavailable'
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_unknown_split_confirmation_never_falls_back_to_managed_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, _ = await setup_switch(db, monkeypatch)
        sub.traffic_accounting_mode = 'managed'
        sub_id = sub.id
        request = dict(
            tariff_id=new.id,
            period_days=30,
            subscription_id=sub_id,
            operation_id=str(uuid4()),
            expected_price_kopeks=1000,
            expected_terms_hash='a' * 64,
        )
        await db.commit()
        app = FastAPI()
        app.include_router(tariff_switch.router)
        user = await db.get(User, target['user_id'])
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        app.dependency_overrides[get_cabinet_db] = lambda: db

        # The ordinary boundary may perform panel IO. A split retry is never
        # permission to enter it, irrespective of current accounting ownership.
        async def forbidden(*args, **kwargs):
            raise AssertionError('Split confirmation entered ordinary checkout')

        monkeypatch.setattr('app.services.traffic_purchase_service.prepare_user_tariff_switch', forbidden)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url='http://test'
        ) as http:
            response = await http.post('/tariff/switch', json=request)
            assert response.status_code == 409, response.text
            assert response.json()['detail']['code'] == 'split_switch_unavailable'
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert (await db.get(User, target['user_id'], populate_existing=True)).balance_kopeks == 10000
