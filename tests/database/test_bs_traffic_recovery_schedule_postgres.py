from datetime import UTC, datetime

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import (
    BsTrafficCollectionBudget,
    BsTrafficDelivery as Delivery,
    BsTrafficRecoverySchedule as Schedule,
    User,
)
from app.services.bs_traffic.recovery_schedule import claim_recovery, finish_recovery, register_recovery_scope
from tests.database.test_bs_traffic_recovery_scan_postgres import OTHER, SCOPE, seed_rows
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
PAST = datetime(2000, 1, 1, tzinfo=UTC)


async def due(db, scope=SCOPE):
    await db.execute(update(Schedule).where(Schedule.panel_scope == scope).values(due_at=PAST))
    await db.commit()


async def test_registration_explicit_and_idempotent_without_resetting_active_work(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        assert await claim_recovery(db, panel_scope=SCOPE) is None
        await register_recovery_scope(db, panel_scope=SCOPE)
        first = await claim_recovery(db, panel_scope=SCOPE)
        assert first.candidate.operation_id == 1
        await db.commit()
        await register_recovery_scope(db, panel_scope=SCOPE)
        assert await claim_recovery(db, panel_scope=SCOPE) is None
        assert (await db.get(Schedule, SCOPE)).lease_token == first.token


async def test_competing_workers_skip_locked_schedule_and_committed_lease(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        factory = async_sessionmaker(db.bind)
        async with factory() as worker, worker.begin():
            first = await claim_recovery(worker, panel_scope=SCOPE)
            assert first is not None
            assert await claim_recovery(db, panel_scope=SCOPE) is None
            await db.rollback()
        assert await claim_recovery(db, panel_scope=SCOPE) is None


async def test_rollback_does_not_reserve_work(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        await db.commit()
        first = await claim_recovery(db, panel_scope=SCOPE)
        await db.rollback()
        second = await claim_recovery(db, panel_scope=SCOPE)
        assert first.candidate == second.candidate and first.token != second.token


async def test_expired_lease_reselects_without_resending_and_fences_old_completion(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention', 'attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        old = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        await db.execute(update(Schedule).where(Schedule.panel_scope == SCOPE).values(lease_until=PAST))
        await db.commit()
        assert not await finish_recovery(db, old, result='verified')
        newer = await claim_recovery(db, panel_scope=SCOPE)
        assert newer.candidate == old.candidate and newer.token != old.token
        await db.commit()
        assert not await finish_recovery(db, old, result='verified')
        assert await finish_recovery(db, newer, result='attention')
        await db.commit()
        assert (await db.get(Delivery, 1)).state == 'attention'
        assert (await db.get(Schedule, SCOPE)).cursor == 1
        assert await claim_recovery(db, panel_scope=SCOPE) is None
        await due(db)
        assert (await claim_recovery(db, panel_scope=SCOPE)).candidate.operation_id == 2


async def test_panel_scopes_have_independent_admission(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await seed_rows(db, ['attention'], first_id=2, scope=OTHER)
        for scope in (SCOPE, OTHER):
            await register_recovery_scope(db, panel_scope=scope)
        first = await claim_recovery(db, panel_scope=SCOPE)
        second = await claim_recovery(db, panel_scope=OTHER)
        assert first.candidate.operation_id == 1 and second.candidate.operation_id == 2


async def test_completed_pass_waits_before_revisiting_unresolved_operations(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        first = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        assert await finish_recovery(db, first, result='unconfirmed')
        await due(db)
        assert await claim_recovery(db, panel_scope=SCOPE) is None
        row = await db.get(Schedule, SCOPE)
        assert row.cursor == 0 and row.upper_id is None and row.eligible_before is None
        await db.commit()
        assert await claim_recovery(db, panel_scope=SCOPE) is None
        await due(db)
        assert (await claim_recovery(db, panel_scope=SCOPE)).candidate.operation_id == 1


async def test_arbitrary_error_text_cannot_be_persisted(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        claim = await claim_recovery(db, panel_scope=SCOPE)
        with pytest.raises(ValueError):
            await finish_recovery(db, claim, result='raw private response')
        assert (await db.get(Schedule, SCOPE)).last_result is None


async def test_migration_matches_model_and_preserves_active_lease(postgres_database):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0145_bs_traffic_recovery_schedule.py')
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        connection = await db.connection()

        def migrate(conn):
            Schedule.__table__.drop(conn)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
            inspector = sa.inspect(conn)
            assert {c['name'] for c in inspector.get_columns(Schedule.__tablename__)} == set(
                Schedule.__table__.c.keys()
            )
            assert {c['name'] for c in inspector.get_check_constraints(Schedule.__tablename__)} == {
                'ck_bs_recovery_cursor',
                'ck_bs_recovery_window',
                'ck_bs_recovery_lease',
            }

        await connection.run_sync(migrate)
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        claim = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        connection = await db.connection()

        def downgrade(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.downgrade()
                migration.upgrade()

        await connection.run_sync(downgrade)
        assert (await db.get(Schedule, SCOPE)).lease_token == claim.token


async def test_expired_owner_does_not_recover_an_already_verified_delivery(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention', 'attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        first = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        await db.execute(update(Delivery).where(Delivery.id == first.candidate.operation_id).values(state='verified'))
        await db.execute(update(Schedule).where(Schedule.panel_scope == SCOPE).values(lease_until=PAST))
        await db.commit()
        second = await claim_recovery(db, panel_scope=SCOPE)
        assert second.candidate.operation_id == 2
        assert not await finish_recovery(db, first, result='unconfirmed')


async def test_finish_rollback_preserves_lease_and_cursor(postgres_database):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        claim = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        assert await finish_recovery(db, claim, result='attention')
        await db.rollback()
        row = await db.get(Schedule, SCOPE)
        assert row.cursor == 0 and row.lease_token == claim.token
        assert await finish_recovery(db, claim, result='attention')


async def test_persisted_window_excludes_new_and_newly_eligible_work_until_next_pass(postgres_database):
    from datetime import timedelta

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention', 'attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        first = await claim_recovery(db, panel_scope=SCOPE)
        row = await db.get(Schedule, SCOPE)
        cutoff = row.eligible_before
        assert row.upper_id == 2
        await db.commit()
        await seed_rows(db, ['attention'], first_id=3)
        await db.execute(
            update(Delivery).where(Delivery.id == 2).values(attempted_at=cutoff + timedelta(microseconds=1))
        )
        await db.execute(update(Delivery).where(Delivery.id == 1).values(state='verified'))
        assert await finish_recovery(db, first, result='verified')
        await due(db)
        async with async_sessionmaker(db.bind)() as worker, worker.begin():
            stored = await worker.get(Schedule, SCOPE)
            assert stored.upper_id == 2 and stored.eligible_before == cutoff
            assert await claim_recovery(worker, panel_scope=SCOPE) is None
        await due(db)
        second = await claim_recovery(db, panel_scope=SCOPE)
        assert second.candidate.operation_id == 2
        assert await finish_recovery(db, second, result='attention')
        await due(db)
        third = await claim_recovery(db, panel_scope=SCOPE)
        assert third.candidate.operation_id == 3


async def test_provider_pause_survives_other_completion_and_late_old_response(postgres_database):
    from sqlalchemy import func, select

    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention', 'attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        old = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        await db.execute(update(Schedule).where(Schedule.panel_scope == SCOPE).values(lease_until=PAST))
        newer = await claim_recovery(db, panel_scope=SCOPE)
        await db.commit()
        before = await db.scalar(select(func.clock_timestamp()))
        assert not await finish_recovery(db, old, result='unconfirmed', retry_after_seconds=300)
        row = await db.get(Schedule, SCOPE)
        deadline = row.due_at
        assert (deadline - before).total_seconds() >= 300
        assert row.lease_token == newer.token and row.cursor == 0
        assert await finish_recovery(db, newer, result='attention', retry_after_seconds=1)
        assert row.due_at == deadline and row.cursor == 1
        await db.commit()
        assert await claim_recovery(db, panel_scope=SCOPE) is None


@pytest.mark.parametrize('delay', [-1, True, float('nan'), float('inf'), '5'])
async def test_invalid_pause_rejected_before_modifying_schedule(postgres_database, delay):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        claim = await claim_recovery(db, panel_scope=SCOPE)
        with pytest.raises(ValueError):
            await finish_recovery(db, claim, result='unconfirmed', retry_after_seconds=delay)
        assert (await db.get(Schedule, SCOPE)).lease_token == claim.token


@pytest.mark.parametrize('delay', [1e100, pytest.param(10**1000, id='huge-int')])
async def test_extreme_provider_pause_is_not_shortened_by_overflow(postgres_database, delay):
    async with postgres_session(
        postgres_database, [BsTrafficCollectionBudget.__table__, Schedule.__table__, User.__table__]
    ) as db:
        await seed_rows(db, ['attention'])
        await register_recovery_scope(db, panel_scope=SCOPE)
        claim = await claim_recovery(db, panel_scope=SCOPE)
        assert await finish_recovery(db, claim, result='unconfirmed', retry_after_seconds=delay)
        assert (await db.get(Schedule, SCOPE)).due_at == datetime.max.replace(tzinfo=UTC)
