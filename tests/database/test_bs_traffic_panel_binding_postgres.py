from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.services.bs_traffic.ledger import LedgerConflict
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
SCOPE = '11111111-1111-4111-8111-111111111111'
OTHER = '22222222-2222-4222-8222-222222222222'
URL = 'https://panel.example'


async def test_endpoint_is_bound_but_api_key_rotation_keeps_scope(postgres_database):
    from app.database.models import BsTrafficPanelBinding
    from app.services.bs_traffic.panel_binding import bind_reader, register_binding

    async with postgres_session(postgres_database, [BsTrafficPanelBinding.__table__]) as db:
        await register_binding(db, panel_scope=SCOPE, endpoint=URL)
        await db.commit()
        for token in ('synthetic-old', 'synthetic-new'):
            api = SimpleNamespace(
                base_url=URL, api_key=token, get_system_configuration=AsyncMock(return_value={'ok': True})
            )
            reader = await bind_reader(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
            assert await reader.get_system_configuration() == {'ok': True}
        wrong = SimpleNamespace(base_url='https://other.example', get_system_configuration=AsyncMock())
        with pytest.raises(LedgerConflict):
            await bind_reader(async_sessionmaker(db.bind), wrong, panel_scope=SCOPE)
        wrong.get_system_configuration.assert_not_awaited()


async def test_retired_scope_cannot_be_reused_or_read(postgres_database):
    from app.database.models import BsTrafficPanelBinding
    from app.services.bs_traffic.panel_binding import bind_reader, register_binding, retire_binding

    async with postgres_session(postgres_database, [BsTrafficPanelBinding.__table__]) as db:
        await register_binding(db, panel_scope=SCOPE, endpoint=URL)
        await db.commit()
        api = SimpleNamespace(base_url=URL, get_system_configuration=AsyncMock())
        reader = await bind_reader(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        await retire_binding(db, panel_scope=SCOPE)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await reader.get_system_configuration()
        api.get_system_configuration.assert_not_awaited()
        with pytest.raises(LedgerConflict):
            await register_binding(db, panel_scope=SCOPE, endpoint=URL)
        await db.rollback()
        await register_binding(db, panel_scope=OTHER, endpoint=URL)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await reader.get_system_configuration()


@pytest.mark.parametrize('method,args', [('get_system_configuration', ()), ('get_split_grace_snapshot', (12,))])
async def test_retirement_during_read_discards_response(postgres_database, method, args):
    from app.database.models import BsTrafficPanelBinding
    from app.services.bs_traffic.panel_binding import bind_reader, register_binding, retire_binding

    async with postgres_session(postgres_database, [BsTrafficPanelBinding.__table__]) as db:
        await register_binding(db, panel_scope=SCOPE, endpoint=URL)
        await db.commit()
        factory = async_sessionmaker(db.bind)

        async def read(*_):
            async with factory() as worker, worker.begin():
                await retire_binding(worker, panel_scope=SCOPE)
            return {'must_not_be_used': True}

        api = SimpleNamespace(base_url=URL, **{method: AsyncMock(side_effect=read)})
        reader = await bind_reader(factory, api, panel_scope=SCOPE)
        with pytest.raises(LedgerConflict):
            await getattr(reader, method)(*args)


async def test_client_endpoint_mutation_is_rejected_before_http(postgres_database):
    from app.database.models import BsTrafficPanelBinding
    from app.services.bs_traffic.panel_binding import bind_reader, register_binding

    async with postgres_session(postgres_database, [BsTrafficPanelBinding.__table__]) as db:
        await register_binding(db, panel_scope=SCOPE, endpoint=URL)
        await db.commit()
        api = SimpleNamespace(base_url=URL, get_system_configuration=AsyncMock())
        reader = await bind_reader(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        api.base_url = 'https://other.example'
        with pytest.raises(LedgerConflict):
            await reader.get_system_configuration()
        api.get_system_configuration.assert_not_awaited()


async def test_concurrent_registration_has_single_active_owner(postgres_database):
    import asyncio

    from sqlalchemy import func, select

    from app.database.models import BsTrafficPanelBinding
    from app.services.bs_traffic.panel_binding import register_binding

    async with postgres_session(postgres_database, [BsTrafficPanelBinding.__table__]) as db:
        factory = async_sessionmaker(db.bind)

        async def register(scope):
            try:
                async with factory() as worker, worker.begin():
                    await register_binding(worker, panel_scope=scope, endpoint=URL)
                return True
            except LedgerConflict:
                return False

        results = await asyncio.gather(register(SCOPE), register(OTHER))
        assert sorted(results) == [False, True]
        assert await db.scalar(select(func.count()).select_from(BsTrafficPanelBinding)) == 1


async def test_binding_migration_is_idempotent_and_preserves_retirement(postgres_database):
    import importlib.util

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import inspect, text

    from app.database.models import BsTrafficPanelBinding
    from app.services.bs_traffic.panel_binding import register_binding, retire_binding

    spec = importlib.util.spec_from_file_location(
        'binding_migration', 'migrations/alembic/versions/0146_bs_traffic_panel_binding.py'
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    async with postgres_session(postgres_database, [BsTrafficPanelBinding.__table__]) as db:
        await db.execute(text('DROP TABLE bs_traffic_panel_bindings'))
        connection = await db.connection()

        def upgrade(sync_connection):
            with Operations.context(MigrationContext.configure(sync_connection)):
                module.upgrade()
                module.upgrade()
            indexes = inspect(sync_connection).get_indexes('bs_traffic_panel_bindings')
            assert any(row['name'] == 'uq_bs_panel_active_endpoint' and row['unique'] for row in indexes)

        await connection.run_sync(upgrade)
        await register_binding(db, panel_scope=SCOPE, endpoint=URL)
        await retire_binding(db, panel_scope=SCOPE)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await register_binding(db, panel_scope=SCOPE, endpoint=URL)
