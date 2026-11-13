"""One form save must not persist tariff fields before access groups succeed."""

from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event

from app.database.models import Base, Tariff
from tests.cabinet.test_admin_create_update_schema_parity import (
    _override_permission_dependencies,
    _tariff_payload,
    get_cabinet_db,
    route,
)
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@asynccontextmanager
async def _app(monkeypatch, postgres_database):
    async def _no_reload(_db):
        return None

    monkeypatch.setattr(route, 'load_period_prices_from_db', _no_reload)
    async with postgres_session(postgres_database, tuple(Base.metadata.sorted_tables)) as db:
        app = FastAPI()
        app.include_router(route.router, prefix='/cabinet')
        app.dependency_overrides[get_cabinet_db] = lambda: db
        _override_permission_dependencies(app)
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as http:
            yield http, db


async def test_failed_promo_group_read_rolls_back_the_whole_tariff_edit(monkeypatch, postgres_database):
    async with _app(monkeypatch, postgres_database) as (http, db):
        response = await http.post('/cabinet/admin/tariffs', json=_tariff_payload())
        assert response.status_code == 200
        tariff_id = response.json()['id']
        original_name = response.json()['name']
        engine = db.bind.sync_engine

        def fail_group_lookup(connection, cursor, statement, parameters, context, executemany):
            if 'FROM promo_groups' in statement and 'promo_groups.id IN' in statement:
                raise RuntimeError('Simulated group lookup failure')

        event.listen(engine, 'before_cursor_execute', fail_group_lookup)
        try:
            with pytest.raises(RuntimeError, match='Simulated group lookup failure'):
                await http.put(
                    f'/cabinet/admin/tariffs/{tariff_id}',
                    json={
                        'name': 'Must not persist',
                        'traffic_limit_gb': 123,
                        'promo_group_ids': [999],
                    },
                )
        finally:
            event.remove(engine, 'before_cursor_execute', fail_group_lookup)
        await db.rollback()
        db.expire_all()
        tariff = await db.get(Tariff, tariff_id)
        assert tariff.name == original_name
        assert tariff.traffic_limit_gb == response.json()['traffic_limit_gb']


async def test_one_save_updates_fields_and_groups_and_allows_clearing_groups(monkeypatch, postgres_database):
    from sqlalchemy import select

    from app.database.models import PromoGroup, tariff_promo_groups

    async with _app(monkeypatch, postgres_database) as (http, db):
        db.add(PromoGroup(id=1, name='Test access group'))
        await db.commit()
        response = await http.post('/cabinet/admin/tariffs', json=_tariff_payload())
        tariff_id = response.json()['id']
        response = await http.put(
            f'/cabinet/admin/tariffs/{tariff_id}',
            json={
                'name': 'Updated together',
                'promo_group_ids': [1],
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()['name'] == 'Updated together'
        group_ids = await db.scalars(
            select(tariff_promo_groups.c.promo_group_id).where(
                tariff_promo_groups.c.tariff_id == tariff_id,
            )
        )
        assert group_ids.all() == [1]
        response = await http.put(f'/cabinet/admin/tariffs/{tariff_id}', json={'promo_group_ids': []})
        assert response.status_code == 200, response.text
        assert not (
            await db.scalars(
                select(tariff_promo_groups.c.promo_group_id).where(
                    tariff_promo_groups.c.tariff_id == tariff_id,
                )
            )
        ).all()
