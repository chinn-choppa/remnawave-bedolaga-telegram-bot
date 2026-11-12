from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficLedger,
    Subscription,
    Tariff,
    TrafficAccountingState,
    TrafficAllowance,
)
from app.external.remnawave_api import SplitTransferSnapshot
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_transfer_capture import capture_managed_transfer
from app.services.bs_traffic.panel_binding import bind_reader, retire_binding
from tests.database.test_bs_traffic_ledger_postgres import SCOPE
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepared(db):
    state = await setup(db)
    subscription = await db.get(Subscription, 1)
    tariff = await db.get(Tariff, subscription.tariff_id)
    tariff.bs_traffic_settings = {
        'enabled': True,
        'squad_uuids': ['33333333-3333-4333-8333-333333333333'],
        'limit_gb': 10,
    }
    panel = SplitTransferSnapshot(12, datetime(2026, 1, 1, tzinfo=UTC), [], 200, 1000, 1000, 'NO_RESET')
    state.panel_identity = f'{panel.id}@{panel.created_at.isoformat()}'
    generation = account_generation(panel_scope=SCOPE, account=panel)
    (await db.get(BsTrafficLedger, 1)).account_generation = generation
    await db.commit()
    api = SimpleNamespace(base_url='https://panel.example', get_split_transfer_snapshot=AsyncMock(return_value=panel))
    reader = await bind_reader(async_sessionmaker(db.bind, expire_on_commit=False), api, panel_scope=SCOPE)
    return reader, api, dict(user_id=1, subscription_id=1, account_generation=generation, destination_group='ordinary')


async def test_bound_observation_does_not_import_or_hold_locks_during_http(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await prepared(db)
        panel = api.get_split_transfer_snapshot.return_value

        async def read(_):
            async with reader._factory() as other, other.begin():
                assert (
                    await other.scalar(select(Subscription.id).where(Subscription.id == 1).with_for_update(nowait=True))
                    == 1
                )
            return panel

        api.get_split_transfer_snapshot.side_effect = read
        proof = await capture_managed_transfer(reader, **keys)
        assert proof.manifest.total_bytes == 800 and proof.valid_until > proof.observed_at
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
        assert (await db.get(TrafficAccountingState, 1)).revision == 3


@pytest.mark.parametrize(
    'change',
    [
        {'lifetime_bytes': 1001},
        {'used_bytes': 201},
        {'used_bytes': 0},
        {'created_at': datetime(2026, 2, 1, tzinfo=UTC)},
        {'traffic_limit_bytes': 999},
        {'traffic_limit_strategy': 'MONTH'},
    ],
)
async def test_counter_identity_or_projection_drift_refuses_observation(postgres_database, change):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await prepared(db)
        api.get_split_transfer_snapshot.return_value = replace(api.get_split_transfer_snapshot.return_value, **change)
        with pytest.raises(LedgerConflict):
            await capture_managed_transfer(reader, **keys)
        assert await db.scalar(select(BsTrafficAllowance.id)) is None


async def test_source_purchase_update_during_http_invalidates_manifest(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await prepared(db)
        panel = api.get_split_transfer_snapshot.return_value

        async def read(_):
            async with reader._factory() as other, other.begin():
                (await other.get(TrafficAllowance, (1, 'purchase:synthetic'))).remaining_bytes = 600
                state = await other.get(TrafficAccountingState, 1)
                state.revision = state.projected_revision = 4
            return panel

        api.get_split_transfer_snapshot.side_effect = read
        with pytest.raises(LedgerConflict, match='changed during'):
            await capture_managed_transfer(reader, **keys)


async def test_retired_panel_during_read_cannot_authorize_transfer(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await prepared(db)
        panel = api.get_split_transfer_snapshot.return_value

        async def read(_):
            async with reader._factory() as other, other.begin():
                await retire_binding(other, panel_scope=SCOPE)
            return panel

        api.get_split_transfer_snapshot.side_effect = read
        with pytest.raises(LedgerConflict):
            await capture_managed_transfer(reader, **keys)


@pytest.mark.parametrize('phase,zero_remaining,quota', [('ready', True, 1), ('unlimited', False, 0)])
async def test_zero_finite_sentinel_and_unlimited_projection(postgres_database, phase, zero_remaining, quota):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await prepared(db)
        state = await db.get(TrafficAccountingState, 1)
        state.phase = phase
        state.used_bytes = 0
        if zero_remaining:
            for grant in (await db.scalars(select(TrafficAllowance))).all():
                grant.remaining_bytes = 0
        await db.commit()
        api.get_split_transfer_snapshot.return_value = replace(
            api.get_split_transfer_snapshot.return_value, used_bytes=0, traffic_limit_bytes=quota
        )
        result = await capture_managed_transfer(reader, **keys)
        assert result.manifest.total_bytes == (0 if zero_remaining else 800)
        assert result.account.traffic_limit_bytes == quota
