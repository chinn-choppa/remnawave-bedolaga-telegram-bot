"""Only an explicit operator save may create the first panel epoch."""

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficPanelBinding
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_first_registration_is_atomic_and_reuses_exact_endpoint(postgres_database):
    from app.services.bs_traffic.operator_binding import ensure_operator_binding

    async with postgres_session(postgres_database, TABLES) as db:
        scope = await ensure_operator_binding(db, endpoint='https://panel.example')
        await db.commit()
        assert await ensure_operator_binding(db, endpoint='https://panel.example/') == scope
        assert await db.scalar(select(func.count()).select_from(BsTrafficPanelBinding)) == 1
        with pytest.raises(LedgerConflict):
            await ensure_operator_binding(db, endpoint='https://another.example')


async def test_retired_epoch_is_never_silently_replaced(postgres_database):
    from app.services.bs_traffic.operator_binding import ensure_operator_binding
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        scope = await ensure_operator_binding(db, endpoint='https://panel.example')
        await retire_binding(db, panel_scope=scope)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await ensure_operator_binding(db, endpoint='https://panel.example')
        assert await db.scalar(select(func.count()).select_from(BsTrafficPanelBinding)) == 1


async def test_cabinet_save_rejects_unverified_panel_before_tariff_commit(postgres_database, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from app.cabinet.routes.admin_tariffs import update_existing_tariff
    from app.cabinet.schemas.tariffs import TariffUpdateRequest
    from app.database.models import Tariff
    from app.services.bs_traffic import operator_binding

    async with postgres_session(postgres_database, TABLES) as db:
        db.add(
            Tariff(
                id=1,
                name='Original',
                period_prices={'30': 100},
                allowed_squads=['00000000-0000-4000-8000-000000000001'],
            )
        )
        await db.commit()
        monkeypatch.setattr(
            operator_binding, 'prepare_operator_settings', AsyncMock(side_effect=ValueError('Invalid panel topology'))
        )
        with pytest.raises(HTTPException) as error:
            await update_existing_tariff(
                1,
                TariffUpdateRequest(
                    bs_traffic_settings={'squad_uuids': ['00000000-0000-4000-8000-000000000002'], 'limit_gb': 10}
                ),
                SimpleNamespace(id=1),
                db,
            )
        assert error.value.status_code == 422
        await db.rollback()
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None


async def test_concurrent_first_saves_share_one_epoch(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.operator_binding import ensure_operator_binding

    async with postgres_session(postgres_database, TABLES) as db:
        factory = async_sessionmaker(db.bind)

        async def save():
            async with factory() as other, other.begin():
                return await ensure_operator_binding(other, endpoint='https://panel.example')

        scopes = await asyncio.gather(save(), save())
        assert scopes[0] == scopes[1]
        assert await db.scalar(select(func.count()).select_from(BsTrafficPanelBinding)) == 1


async def test_existing_split_owner_without_binding_cannot_be_adopted(postgres_database):
    from app.database.models import Subscription, User
    from app.services.bs_traffic.operator_binding import ensure_operator_binding

    async with postgres_session(postgres_database, TABLES) as db:
        db.add(User(id=1))
        await db.flush()
        db.add(
            Subscription(
                user_id=1,
                traffic_accounting_mode='split_pending',
                end_date=await db.scalar(select(func.clock_timestamp())),
            )
        )
        await db.commit()
        with pytest.raises(LedgerConflict, match='bs_binding_mismatch'):
            await ensure_operator_binding(db, endpoint='https://panel.example')
        assert await db.scalar(select(func.count()).select_from(BsTrafficPanelBinding)) == 0


async def test_binding_reuse_is_compatible_with_financial_share_lock(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.operator_binding import ensure_operator_binding

    async with postgres_session(postgres_database, TABLES) as db:
        scope = await ensure_operator_binding(db, endpoint='https://panel.example')
        await db.commit()
        await db.execute(select(BsTrafficPanelBinding).with_for_update(read=True))
        factory = async_sessionmaker(db.bind)

        async def reuse():
            async with factory() as concurrent, concurrent.begin():
                return await ensure_operator_binding(concurrent, endpoint='https://panel.example')

        assert await asyncio.wait_for(reuse(), 1) == scope


@pytest.mark.parametrize('creating', [False, True])
@pytest.mark.parametrize(
    'code,status',
    [
        ('bs_topology_invalid', 422),
        ('bs_usage_config_invalid', 422),
        ('bs_panel_unavailable', 503),
        ('bs_binding_mismatch', 409),
        ('bs_uncertain_policy_required', 422),
    ],
)
async def test_tariff_routes_return_only_operator_codes(postgres_database, monkeypatch, creating, code, status):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from app.cabinet.routes.admin_tariffs import create_new_tariff, update_existing_tariff
    from app.cabinet.schemas.tariffs import TariffCreateRequest, TariffUpdateRequest
    from app.database.models import Tariff
    from app.services.bs_traffic import operator_binding

    async with postgres_session(postgres_database, TABLES) as db:
        db.add(
            Tariff(
                id=1,
                name='Original',
                period_prices={'30': 100},
                allowed_squads=['00000000-0000-4000-8000-000000000001'],
            )
        )
        await db.commit()
        monkeypatch.setattr(
            operator_binding,
            'prepare_operator_settings',
            AsyncMock(side_effect=operator_binding.OperatorSettingsError(code)),
        )
        config = {'squad_uuids': ['00000000-0000-4000-8000-000000000002'], 'limit_gb': 10}
        with pytest.raises(HTTPException) as error:
            if creating:
                await create_new_tariff(
                    TariffCreateRequest(name='New', bs_traffic_settings=config), SimpleNamespace(id=1), db
                )
            else:
                await update_existing_tariff(
                    1, TariffUpdateRequest(bs_traffic_settings=config), SimpleNamespace(id=1), db
                )
        assert error.value.status_code == status and error.value.detail == code
