import importlib

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

from app.database.crud.tariff import create_tariff, get_tariff_by_id, update_tariff
from app.database.models import Base
from app.services.tariff_edit_version import tariff_edit_version
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
SQUAD = '11111111-1111-4111-8111-111111111111'
TERMS = {'squad_uuids': [SQUAD], 'limit_gb': 10, 'topup_enabled': True, 'topup_packages': {'5': 100}}


async def test_create_edit_omit_and_clear_bs_terms(postgres_database):
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        tariff = await create_tariff(db, name='Test', bs_traffic_settings=TERMS)
        tariff = await get_tariff_by_id(db, tariff.id)
        assert tariff.bs_traffic_settings['topup_packages'] == {'5': 100}
        await update_tariff(db, tariff, name='Renamed')
        assert tariff.bs_traffic_settings['limit_gb'] == 10
        initial = tariff_edit_version(tariff)
        await update_tariff(db, tariff, bs_traffic_settings=TERMS | {'limit_gb': 20})
        assert tariff_edit_version(tariff) != initial
        await update_tariff(db, tariff, bs_traffic_settings=None)
        assert tariff.bs_traffic_settings['enabled'] is False


async def test_overlap_rejected_before_any_edit(postgres_database):
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        tariff = await create_tariff(db, name='Test', bs_traffic_settings=TERMS)
        with pytest.raises(ValueError, match='overlap'):
            await update_tariff(db, tariff, name='Wrong', allowed_squads=[SQUAD])
        assert tariff.name == 'Test' and tariff.allowed_squads == []


async def test_migration_preserves_existing_tariffs_and_bs_terms(postgres_database):
    migration = importlib.import_module('migrations.alembic.versions.0152_bs_tariff_settings')
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        tariff = await create_tariff(db, name='Existing')
        connection = await db.connection()

        def run(conn, operation):
            with Operations.context(MigrationContext.configure(conn)):
                operation()

        await connection.run_sync(run, migration.downgrade)
        await connection.run_sync(run, migration.upgrade)
        await connection.run_sync(run, migration.upgrade)
        assert await db.scalar(text('SELECT bs_traffic_settings FROM tariffs WHERE id=:id'), {'id': tariff.id}) is None
        await db.execute(text("UPDATE tariffs SET bs_traffic_settings='{}' WHERE id=:id"), {'id': tariff.id})
        with pytest.raises(RuntimeError, match='preserved'):
            await connection.run_sync(run, migration.downgrade)


async def test_admin_routes_roundtrip_and_overlap_response(postgres_database, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from app.cabinet.routes import admin_tariffs
    from app.cabinet.schemas.tariffs import TariffCreateRequest, TariffUpdateRequest

    monkeypatch.setattr(admin_tariffs, 'load_period_prices_from_db', AsyncMock())
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        admin = SimpleNamespace(id=1)
        created = await admin_tariffs.create_new_tariff(
            TariffCreateRequest(name='Test', bs_traffic_settings=TERMS), admin, db
        )
        assert created.bs_traffic_settings.limit_gb == 10
        edited = await admin_tariffs.update_existing_tariff(
            created.id, TariffUpdateRequest(bs_traffic_settings=TERMS | {'limit_gb': 20}), admin, db
        )
        assert edited.bs_traffic_settings.limit_gb == 20
        with pytest.raises(HTTPException) as error:
            await admin_tariffs.update_existing_tariff(
                created.id, TariffUpdateRequest(name='Wrong', allowed_squads=[SQUAD]), admin, db
            )
        assert error.value.status_code == 422
        cleared = await admin_tariffs.update_existing_tariff(
            created.id, TariffUpdateRequest(bs_traffic_settings=None), admin, db
        )
        assert cleared.bs_traffic_settings.enabled is False


async def test_opt_in_configuration_does_not_mutate_existing_subscription(postgres_database):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import func, select

    from app.database.models import BsTrafficLedger, Subscription, Transaction, User

    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        tariff = await create_tariff(db, name='Ordinary', traffic_limit_gb=50)
        db.add(User(id=1, balance_kopeks=12345))
        await db.flush()
        sub = Subscription(
            id=1,
            user_id=1,
            tariff_id=tariff.id,
            traffic_accounting_mode='legacy',
            status='active',
            is_trial=False,
            traffic_limit_gb=50,
            traffic_used_gb=7,
            end_date=datetime.now(UTC) + timedelta(days=30),
        )
        db.add(sub)
        await db.commit()
        original_expiry = sub.end_date
        assert tariff.bs_traffic_settings is None
        # Form changes alone are not permission to rewrite paid subscriptions.
        for terms in (TERMS, None, TERMS, None):
            await update_tariff(db, tariff, bs_traffic_settings=terms)
            await db.refresh(sub)
            assert sub.traffic_accounting_mode == 'legacy'
            assert sub.traffic_limit_gb == 50
            assert sub.traffic_used_gb == 7
            assert sub.end_date == original_expiry
            assert await db.scalar(select(User.balance_kopeks).where(User.id == 1)) == 12345
            assert await db.scalar(select(func.count()).select_from(BsTrafficLedger)) == 0
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_disable_timestamp_is_server_owned_and_stable(postgres_database):
    from datetime import UTC, datetime

    from app.services.bs_traffic.tariff_settings import BsTrafficSettings

    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        tariff = await create_tariff(db, name='Toggle', bs_traffic_settings=TERMS)
        before = await db.scalar(text('SELECT clock_timestamp()'))
        await update_tariff(db, tariff, bs_traffic_settings=None)
        stamp = datetime.fromisoformat(tariff.bs_traffic_settings['disabled_at'])
        after = await db.scalar(text('SELECT clock_timestamp()'))
        assert before <= stamp <= after
        await update_tariff(
            db, tariff, bs_traffic_settings=TERMS | {'enabled': False, 'disabled_at': '2000-01-01T00:00:00Z'}
        )
        assert datetime.fromisoformat(tariff.bs_traffic_settings['disabled_at']) == stamp
        assert 'disabled_at' not in BsTrafficSettings.model_validate(tariff.bs_traffic_settings).model_dump()
        await update_tariff(db, tariff, bs_traffic_settings=TERMS | {'disabled_at': '2000-01-01T00:00:00Z'})
        assert 'disabled_at' not in tariff.bs_traffic_settings
        await update_tariff(db, tariff, bs_traffic_settings=None)
        assert datetime.fromisoformat(tariff.bs_traffic_settings['disabled_at']) > stamp
        assert stamp.tzinfo == UTC
