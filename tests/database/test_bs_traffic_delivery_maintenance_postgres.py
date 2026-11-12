from datetime import UTC, datetime

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficDelivery as Delivery, User
from app.services.bs_traffic.delivery_maintenance import expire_pending
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_delivery_postgres import stage, take
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
PAST = datetime(2000, 1, 1, tzinfo=UTC)
OTHER = '99999999-9999-4999-8999-999999999999'


async def expire(db, operation_id):
    await db.execute(update(Delivery).where(Delivery.id == operation_id).values(valid_until=PAST))
    await db.commit()


async def test_expired_unsent_operation_releases_slot_without_rewriting_old_identity(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await expire(db, op)
        with pytest.raises(LedgerConflict, match='reconciliation'):
            await stage(db, operation_key='replacement')
        await db.rollback()
        assert await expire_pending(db, panel_scope=SCOPE) == (op,)
        await db.commit()
        row = await db.get(Delivery, op)
        assert row.state == 'cancelled' and row.token is None and row.attempted_at is None
        assert row.valid_until == PAST
        assert await stage(db) == op
        assert await take(db, op) is None
        assert await stage(db, operation_key='replacement') != op


@pytest.mark.parametrize('state', ['attempted', 'attention', 'verified', 'not_sent', 'cancelled'])
async def test_expiry_never_frees_potential_remote_effects_or_rewrites_terminal_rows(postgres_database, state):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        if state != 'cancelled':
            await take(db, op)
        await db.execute(
            update(Delivery)
            .where(Delivery.id == op)
            .values(
                state=state,
                not_sent_reason='stale_evidence' if state == 'not_sent' else None,
            )
        )
        await expire(db, op)
        assert await expire_pending(db, panel_scope=SCOPE) == ()
        row = await db.get(Delivery, op)
        assert row.state == state
        if state in ('attempted', 'attention'):
            with pytest.raises(LedgerConflict):
                await stage(db, operation_key='must-not-repeat')


async def test_fresh_or_other_scope_pending_is_untouched(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        assert await expire_pending(db, panel_scope=SCOPE) == ()
        await expire(db, op)
        assert await expire_pending(db, panel_scope=OTHER) == ()
        assert (await db.get(Delivery, op)).state == 'pending'


async def test_locked_pending_is_skipped_and_rollback_restores_cleanup(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await expire(db, op)
        factory = async_sessionmaker(db.bind)
        async with factory() as locked, locked.begin():
            await locked.execute(select(Delivery).where(Delivery.id == op).with_for_update())
            assert await expire_pending(db, panel_scope=SCOPE) == ()
            await db.rollback()
        assert await expire_pending(db, panel_scope=SCOPE) == (op,)
        await db.rollback()
        assert (await db.get(Delivery, op)).state == 'pending'
        assert await expire_pending(db, panel_scope=SCOPE) == (op,)


async def test_concurrent_claim_cannot_be_reclassified_as_never_sent(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        op = await stage(db)
        await db.commit()
        factory = async_sessionmaker(db.bind)
        async with factory() as sender, sender.begin():
            assert await take(sender, op) is not None
            await sender.execute(update(Delivery).where(Delivery.id == op).values(valid_until=PAST))
            assert await expire_pending(db, panel_scope=SCOPE) == ()
            await db.rollback()
        assert await expire_pending(db, panel_scope=SCOPE) == ()
        assert (await db.get(Delivery, op)).state == 'attempted'


@pytest.mark.parametrize('limit', [0, -1, True, 1.5, 501])
async def test_invalid_limit(postgres_database, limit):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        with pytest.raises(ValueError):
            await expire_pending(db, panel_scope=SCOPE, limit=limit)


async def test_two_workers_take_disjoint_bounded_batches_oldest_first(postgres_database):
    from datetime import timedelta

    from app.services.bs_traffic.ledger import initialize
    from tests.database.test_bs_traffic_ledger_postgres import GENERATION

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        first = await stage(db)
        await initialize(
            db, user_id=1, subscription_id=2, account_generation=GENERATION, panel_scope=SCOPE, panel_user_id=13
        )
        second = await stage(db, subscription_id=2)
        await db.execute(update(Delivery).where(Delivery.id == first).values(valid_until=PAST))
        await db.execute(update(Delivery).where(Delivery.id == second).values(valid_until=PAST - timedelta(seconds=1)))
        await db.commit()
        factory = async_sessionmaker(db.bind)
        async with factory() as worker, worker.begin():
            assert await expire_pending(worker, panel_scope=SCOPE, limit=1) == (second,)
            assert await expire_pending(db, panel_scope=SCOPE, limit=1) == (first,)
            await db.commit()
        assert await expire_pending(db, panel_scope=SCOPE) == ()


@pytest.mark.parametrize('scope', [None, 1, '', 'invalid'])
async def test_invalid_scope(postgres_database, scope):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        with pytest.raises(ValueError):
            await expire_pending(db, panel_scope=scope)
