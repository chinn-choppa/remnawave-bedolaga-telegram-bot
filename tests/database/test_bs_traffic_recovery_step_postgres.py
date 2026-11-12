import asyncio

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import (
    BsTrafficCollectionBudget,
    BsTrafficDelivery as Delivery,
    BsTrafficRecoverySchedule as Schedule,
    User,
)
from app.external.remnawave_api import RemnaWaveTransientError
from app.services.bs_traffic.recovery_schedule import register_recovery_scope, run_recovery_once
from tests.database.test_bs_traffic_ledger_postgres import SCOPE
from tests.database.test_bs_traffic_recovery_preflight_postgres import attempt, client
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('endpoint', ['topology', 'account'])
async def test_step_persists_provider_pause_from_both_network_stages(postgres_database, endpoint):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        api = client()
        _, operation = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        error = RemnaWaveTransientError('private provider body', status_code=429)
        error.retry_after_seconds = 120
        method = api.get_system_configuration if endpoint == 'topology' else api.get_split_access_snapshot
        method.side_effect = error
        before = await db.scalar(select(func.clock_timestamp()))
        await db.rollback()
        assert await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        row = await db.get(Schedule, SCOPE)
        assert (row.due_at - before).total_seconds() >= 120
        assert row.last_result == 'unconfirmed' and row.cursor == operation and row.lease_token is None
        assert (await db.get(Delivery, operation)).state == 'attempted'
        assert not await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        method.assert_awaited_once()
        api.update_internal_squads_once.assert_not_awaited()


async def test_step_commits_verified_result_without_holding_schedule_lock_during_http(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        api = client()
        _, operation = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        configuration = api.get_system_configuration.return_value

        async def probe():
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await other.execute(select(Schedule).where(Schedule.panel_scope == SCOPE).with_for_update(nowait=True))
            return configuration

        api.get_system_configuration.side_effect = probe
        assert await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        assert (await db.get(Delivery, operation)).state == 'verified'
        assert (await db.get(Schedule, SCOPE)).last_result == 'verified'
        api.update_internal_squads_once.assert_not_awaited()


async def test_cancelled_step_keeps_lease_for_recovery_and_does_not_advance_cursor(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        api = client()
        _, operation = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        entered = asyncio.Event()

        async def blocked():
            entered.set()
            await asyncio.Event().wait()

        api.get_system_configuration.side_effect = blocked
        task = asyncio.create_task(run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = await db.get(Schedule, SCOPE)
        assert row.cursor == 0 and row.lease_token is not None
        assert (await db.get(Delivery, operation)).state == 'attempted'


async def test_late_provider_failure_extends_pause_without_finishing_new_lease(postgres_database):
    from app.services.bs_traffic.recovery_schedule import claim_recovery
    from tests.database.test_bs_traffic_recovery_schedule_postgres import PAST

    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        api = client()
        _, operation = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        newer = None

        async def late():
            nonlocal newer
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await other.execute(update(Schedule).where(Schedule.panel_scope == SCOPE).values(lease_until=PAST))
                newer = await claim_recovery(other, panel_scope=SCOPE)
            error = RemnaWaveTransientError('private body', status_code=503)
            error.retry_after_seconds = 300
            raise error

        api.get_system_configuration.side_effect = late
        assert await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        row = await db.get(Schedule, SCOPE)
        now = await db.scalar(select(func.clock_timestamp()))
        assert row.cursor == 0 and row.lease_token == newer.token
        assert (row.due_at - now).total_seconds() > 290
        assert (await db.get(Delivery, operation)).state == 'attempted'
        api.update_internal_squads_once.assert_not_awaited()


async def test_unexpected_failure_leaves_claim_without_fake_completion(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        api = client()
        _, operation = await attempt(db, api)
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        api.get_system_configuration.side_effect = RuntimeError('synthetic failure')
        with pytest.raises(RuntimeError, match='synthetic failure'):
            await run_recovery_once(async_sessionmaker(db.bind), api, panel_scope=SCOPE)
        row = await db.get(Schedule, SCOPE)
        assert row.cursor == 0 and row.lease_token is not None and row.last_result is None
        assert (await db.get(Delivery, operation)).state == 'attempted'
