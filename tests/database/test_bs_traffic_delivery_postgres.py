"""Durable attempts cannot repeat a possibly successful panel write."""

from datetime import UTC, datetime, timedelta

import pytest

from app.database.models import User
from app.services.bs_traffic.delivery_store import claim, enqueue, reconcile
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_ledger_postgres import TARGET, issue, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
SQUAD = '11111111-1111-4111-8111-111111111111'
GUARD = 'b' * 64


async def stage(db, **changes):
    args = dict(
        TARGET,
        operation_key='restriction-1',
        revision=0,
        guard_hash=GUARD,
        squads=(SQUAD,),
        valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )
    args.update(changes)
    return await enqueue(db, **args)


async def take(db, operation_id, **changes):
    args = dict(TARGET, operation_id=operation_id, current_guard_hash=GUARD)
    args.update(changes)
    return await claim(db, **args)


async def test_attempt_survives_commit_and_is_never_claimed_twice(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.commit()
        attempt = await take(db, op)
        assert attempt is not None and attempt.squads == (SQUAD,)
        await db.commit()
        assert await take(db, op) is None


async def test_purchase_invalidates_pending_attempt(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await issue(db)
        assert await take(db, op) is None


async def test_changed_grace_or_subscription_guard_invalidates_pending_attempt(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        assert await take(db, op, current_guard_hash='c' * 64) is None


async def test_readback_mismatch_requires_attention_and_blocks_new_writes(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        attempt = await take(db, op)
        result = await reconcile(
            db, **TARGET, operation_id=op, token=attempt.token, current_guard_hash=GUARD, observed_squads=()
        )
        assert result == 'attention'
        with pytest.raises(LedgerConflict):
            await stage(db, operation_key='other')
        assert await take(db, op) is None


async def test_readback_match_is_required_to_verify(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        attempt = await take(db, op)
        assert (
            await reconcile(
                db, **TARGET, operation_id=op, token=attempt.token, current_guard_hash=GUARD, observed_squads=(SQUAD,)
            )
            == 'verified'
        )


async def test_unknown_attempt_cannot_be_replaced_even_after_expiry(postgres_database):
    from sqlalchemy import update

    from app.database.models import BsTrafficDelivery

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await take(db, op)
        await db.execute(
            update(BsTrafficDelivery)
            .where(BsTrafficDelivery.id == op)
            .values(
                valid_until=datetime.now(UTC) - timedelta(minutes=1),
                attempted_at=datetime.now(UTC) - timedelta(minutes=2),
            )
        )
        with pytest.raises(LedgerConflict):
            await stage(db, operation_key='new')


async def test_replay_cannot_replace_request(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        assert await stage(db) == op
        with pytest.raises(LedgerConflict):
            await stage(db, squads=())


@pytest.mark.parametrize('field,value', [('status', 'disabled'), ('tariff_id', None), ('connected_squads', [SQUAD])])
async def test_local_subscription_change_invalidates_even_unchanged_caller_guard(postgres_database, field, value):
    from sqlalchemy import update

    from app.database.models import Subscription, Tariff

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        tariff = Tariff(name='Synthetic delivery')
        db.add(tariff)
        await db.flush()
        await db.execute(update(Subscription).where(Subscription.id == 1).values(tariff_id=tariff.id))
        op = await stage(db)
        await db.execute(update(Subscription).where(Subscription.id == 1).values(**{field: value}))
        assert await take(db, op) is None


async def test_parallel_claims_send_only_one_attempt(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)

        async def worker():
            async with maker() as conn, conn.begin():
                await barrier.wait()
                return await take(conn, op)

        results = await asyncio.wait_for(asyncio.gather(worker(), worker()), 10)
        assert sum(item is not None for item in results) == 1


async def test_recreated_account_cannot_receive_old_pending_selection(postgres_database):
    from sqlalchemy import update

    from app.database.models import BsTrafficLedger

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.execute(
            update(BsTrafficLedger).where(BsTrafficLedger.subscription_id == 1).values(account_generation='d' * 64)
        )
        assert await take(db, op, account_generation='d' * 64) is None


async def test_attempt_rollback_does_not_leave_a_false_attempt(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.commit()
        await take(db, op)
        await db.rollback()
        assert await take(db, op) is not None


async def test_migration_preserves_attempts_on_downgrade(postgres_database):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.database.models import BsTrafficDelivery, BsTrafficEnrollment
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0145_bs_traffic_delivery.py')
    drop_migration = _load('0164_bs_connection_drop.py')
    async with postgres_session(postgres_database, [User.__table__]) as db:
        connection = await db.connection()

        def migrate(conn):
            BsTrafficEnrollment.__table__.drop(conn)
            BsTrafficDelivery.__table__.drop(conn)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
                drop_migration.upgrade()
            BsTrafficEnrollment.__table__.create(conn)
            assert {c['name'] for c in sa.inspect(conn).get_columns('bs_traffic_deliveries')} == set(
                BsTrafficDelivery.__table__.c.keys()
            )
            indexes = {i['name']: i for i in sa.inspect(conn).get_indexes('bs_traffic_deliveries')}
            recovery = indexes['ix_bs_delivery_recovery_scan']
            assert recovery['column_names'] == ['id']
            predicate = str(recovery['dialect_options']['postgresql_where'])
            assert 'attempted' in predicate and 'attention' in predicate
            expiry = indexes['ix_bs_delivery_pending_expiry']
            assert expiry['column_names'] == ['valid_until', 'id']
            assert 'pending' in str(expiry['dialect_options']['postgresql_where'])

        await connection.run_sync(migrate)
        await seed(db)
        op = await stage(db)
        attempt = await take(db, op)
        row = await db.get(BsTrafficDelivery, op)
        row.state, row.not_sent_reason = 'not_sent', 'stale_evidence'
        await db.flush()
        next_op = await stage(db, operation_key='after-unsent')

        def rollback_version(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.downgrade()
                migration.upgrade()

        await connection.run_sync(rollback_version)
        assert (await db.get(BsTrafficDelivery, op)).token == attempt.token
        assert (await db.get(BsTrafficDelivery, op)).not_sent_reason == 'stale_evidence'
        assert (await db.get(BsTrafficDelivery, next_op)).state == 'pending'


async def test_expired_evidence_cancels_without_attempt(postgres_database):
    from sqlalchemy import update

    from app.database.models import BsTrafficDelivery

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.execute(
            update(BsTrafficDelivery)
            .where(BsTrafficDelivery.id == op)
            .values(valid_until=datetime.now(UTC) - timedelta(seconds=1))
        )
        assert await take(db, op) is None
        assert (await db.get(BsTrafficDelivery, op)).token is None


@pytest.mark.parametrize('change', [{'user_id': 2}, {'subscription_id': 2}, {'account_generation': 'f' * 64}])
async def test_other_identity_cannot_claim(postgres_database, change):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        with pytest.raises(LedgerConflict):
            await take(db, op, **change)


async def test_forged_token_cannot_confirm_delivery(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await take(db, op)
        with pytest.raises(LedgerConflict):
            await reconcile(
                db, **TARGET, operation_id=op, token='forged', current_guard_hash=GUARD, observed_squads=(SQUAD,)
            )


async def test_purchase_after_attempt_cannot_be_confirmed_by_old_readback(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        attempt = await take(db, op)
        await issue(db)
        assert (
            await reconcile(
                db, **TARGET, operation_id=op, token=attempt.token, current_guard_hash=GUARD, observed_squads=(SQUAD,)
            )
            == 'attention'
        )


async def test_new_grace_overlay_invalidates_existing_pending_write(postgres_database):
    from app.database.models import GraceAccessSessionModel

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        now = datetime.now(UTC)
        db.add(
            GraceAccessSessionModel(
                id='22222222-2222-4222-8222-222222222222',
                subscription_id=1,
                remnawave_id=12,
                reason='limited',
                incident_key='synthetic',
                state='pending',
                billing_before={},
                panel_before={},
                overlay={},
                started_at=now,
                grace_until=now + timedelta(hours=1),
                updated_at=now,
            )
        )
        await db.flush()
        assert await take(db, op) is None


async def test_ownership_transfer_invalidates_old_pending_operation(postgres_database):
    from sqlalchemy import update

    from app.database.models import Subscription

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.execute(update(Subscription).where(Subscription.id == 1).values(user_id=2))
        assert await take(db, op, user_id=2) is None


@pytest.mark.parametrize(
    'values',
    [
        {'state': 'not_sent'},
        {'state': 'not_sent', 'not_sent_reason': 'unknown'},
        {'state': 'attention', 'not_sent_reason': 'stale_evidence'},
        {'state': 'not_sent', 'not_sent_reason': 'stale_evidence', 'token': None},
    ],
)
async def test_database_rejects_incomplete_or_mislabelled_unsent_audit(postgres_database, values):
    from sqlalchemy import update
    from sqlalchemy.exc import IntegrityError

    from app.database.models import BsTrafficDelivery

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await take(db, op)
        with pytest.raises(IntegrityError):
            async with db.begin_nested():
                await db.execute(update(BsTrafficDelivery).where(BsTrafficDelivery.id == op).values(**values))
