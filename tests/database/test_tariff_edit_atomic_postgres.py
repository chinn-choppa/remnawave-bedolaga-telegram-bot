"""Regression for a partially committed cabinet form on a real PostgreSQL DB."""

from types import SimpleNamespace

import pytest
from sqlalchemy import event

from app.cabinet.routes.admin_tariffs import update_existing_tariff
from app.cabinet.schemas.tariffs import TariffUpdateRequest
from app.database.models import Base, Tariff
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_promo_lookup_failure_cannot_leave_new_tariff_fields_committed(postgres_database):
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        db.add(Tariff(id=1, name='Original', period_prices={'30': 100}, traffic_limit_gb=10))
        await db.commit()
        engine = db.bind.sync_engine

        def fail_group_lookup(connection, cursor, statement, parameters, context, executemany):
            if 'FROM promo_groups' in statement and 'promo_groups.id IN' in statement:
                raise RuntimeError('Simulated group lookup failure')

        event.listen(engine, 'before_cursor_execute', fail_group_lookup)
        try:
            with pytest.raises(RuntimeError, match='Simulated group lookup failure'):
                await update_existing_tariff(
                    1,
                    TariffUpdateRequest(name='New', traffic_limit_gb=123, promo_group_ids=[999]),
                    SimpleNamespace(id=1),
                    db,
                )
        finally:
            event.remove(engine, 'before_cursor_execute', fail_group_lookup)
        await db.rollback()
        db.expire_all()
        tariff = await db.get(Tariff, 1)
        assert tariff.name == 'Original'
        assert tariff.traffic_limit_gb == 10


async def test_two_cabinet_saves_with_one_version_have_only_one_winner(postgres_database, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from fastapi import HTTPException
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.cabinet.routes import admin_tariffs
    from app.database.crud.tariff import get_tariff_by_id
    from app.services.tariff_edit_version import tariff_edit_version

    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        db.add(Tariff(id=1, name='Original', period_prices={'30': 100}))
        await db.commit()
        version = tariff_edit_version(await get_tariff_by_id(db, 1))
        await db.rollback()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        barrier = asyncio.Barrier(2)
        monkeypatch.setattr(admin_tariffs, 'load_period_prices_from_db', AsyncMock())
        monkeypatch.setattr(admin_tariffs, 'get_tariff', AsyncMock(return_value={}))

        async def edit(name):
            async with factory() as session:
                await barrier.wait()
                try:
                    await update_existing_tariff(
                        1,
                        TariffUpdateRequest(name=name, expected_version=version),
                        SimpleNamespace(id=1),
                        session,
                    )
                    return 200
                except HTTPException as exc:
                    await session.rollback()
                    return exc.status_code

        results = await asyncio.wait_for(asyncio.gather(edit('A'), edit('B')), timeout=10)
        assert sorted(results) == [200, 409]
        assert (await db.get(Tariff, 1, populate_existing=True)).name in {'A', 'B'}


async def test_telegram_and_cabinet_save_cannot_overwrite_each_other(postgres_database, monkeypatch):
    import asyncio
    import inspect
    from unittest.mock import AsyncMock

    from fastapi import HTTPException
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.cabinet.routes import admin_tariffs
    from app.handlers.admin import tariffs as handlers
    from tests.handlers.test_tariff_squad_form_state import A, B, callback, invoke, prepare, state

    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        form = (await fsm.get_data())['tariff_squad_form']
        await db.rollback()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        barrier = asyncio.Barrier(2)
        monkeypatch.setattr(admin_tariffs, 'load_period_prices_from_db', AsyncMock())
        monkeypatch.setattr(admin_tariffs, 'get_tariff', AsyncMock(return_value={}))

        async def cabinet():
            async with factory() as session:
                await barrier.wait()
                try:
                    await update_existing_tariff(
                        1,
                        TariffUpdateRequest(name='Cabinet', expected_version=form['version']),
                        SimpleNamespace(id=1),
                        session,
                    )
                    return True
                except HTTPException as exc:
                    assert exc.status_code == 409
                    await session.rollback()
                    return False

        async def telegram():
            async with factory() as session:
                await barrier.wait()
                await inspect.unwrap(handlers.save_tariff_squads)(
                    callback(f'admin_tariff_save_squads:1:{form["token"]}'),
                    SimpleNamespace(id=1, language='ru'),
                    session,
                    fsm,
                )
                return propagate.await_count == 1

        results = await asyncio.wait_for(asyncio.gather(cabinet(), telegram()), timeout=10)
        assert sorted(results) == [False, True]
        tariff = await db.get(Tariff, 1, populate_existing=True)
        if results[0]:
            assert tariff.name == 'Cabinet' and tariff.allowed_squads == [A]
        else:
            assert tariff.name == 'Test' and set(tariff.allowed_squads) == {A, B}
