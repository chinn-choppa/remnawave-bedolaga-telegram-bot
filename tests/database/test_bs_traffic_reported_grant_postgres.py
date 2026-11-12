"""A purchase settles known usage before increasing its group's balance."""

from datetime import timedelta

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficAllowance, BsTrafficUsage
from app.services.bs_traffic.intake import grant_with_reported_settlement
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_intake_postgres import TABLES
from tests.database.test_bs_traffic_ledger_postgres import END, NOW, TARGET, remaining
from tests.database.test_bs_traffic_reported_postgres import sample, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
PURCHASE = dict(TARGET, group='bs', operation_key='purchase', amount_bytes=100, starts_at=NOW, expires_at=END)


async def test_known_overflow_is_not_taken_from_purchase(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        grant_id = await grant_with_reported_settlement(db, **PURCHASE)
        assert await remaining(db) == 100
        assert (await db.get(BsTrafficAllowance, grant_id)).remaining_bytes == 100
        usage = await db.scalar(select(BsTrafficUsage))
        assert usage.charged_bytes == 100 and usage.unallocated_bytes == 50


async def test_payment_replay_does_not_process_new_traffic(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        first = await grant_with_reported_settlement(db, **PURCHASE)
        await sample(db, 580)
        assert await grant_with_reported_settlement(db, **PURCHASE) == first
        assert await remaining(db) == 140
        assert len((await db.scalars(select(BsTrafficUsage))).all()) == 1


async def test_failed_purchase_rolls_back_settlement(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        await db.commit()
        await grant_with_reported_settlement(db, **PURCHASE)
        await db.rollback()
        assert await remaining(db) == 100
        assert not (await db.scalars(select(BsTrafficUsage))).all()


async def test_bad_terms_do_not_consume_known_usage(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        with pytest.raises(LedgerConflict):
            await grant_with_reported_settlement(db, **(PURCHASE | {'expires_at': NOW - timedelta(days=1)}))
        assert await remaining(db) == 100
        assert not (await db.scalars(select(BsTrafficUsage))).all()


async def test_group_isolation(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        await grant_with_reported_settlement(db, **(PURCHASE | {'group': 'ordinary'}))
        assert await remaining(db, 'ordinary') == 100
        assert await remaining(db) == 100
        assert not (await db.scalars(select(BsTrafficUsage))).all()


@pytest.mark.parametrize('change', [{'user_id': 2}, {'account_generation': 'b' * 64}])
async def test_wrong_owner_or_generation_cannot_settle_or_grant(postgres_database, change):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        with pytest.raises(LedgerConflict):
            await grant_with_reported_settlement(db, **(PURCHASE | change))
        assert await remaining(db) == 100
        assert not (await db.scalars(select(BsTrafficUsage))).all()


async def test_conflicting_replay_preserves_balances(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await grant_with_reported_settlement(db, **PURCHASE)
        await sample(db, 650)
        with pytest.raises(LedgerConflict):
            await grant_with_reported_settlement(db, **(PURCHASE | {'amount_bytes': 999}))
        assert await remaining(db) == 200


async def test_payment_and_collector_race_preserves_new_package(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.intake import settle_reported_observation
    from tests.database.test_bs_traffic_intake_postgres import KEY

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        await db.commit()
        maker = async_sessionmaker(db.bind)

        async def purchase():
            async with maker() as conn, conn.begin():
                return await grant_with_reported_settlement(conn, **PURCHASE)

        async def collect():
            async with maker() as conn, conn.begin():
                return await settle_reported_observation(conn, **KEY)

        first, second, _ = await asyncio.wait_for(asyncio.gather(purchase(), purchase(), collect()), 10)
        assert first == second
        assert await remaining(db) == 100
        assert len((await db.scalars(select(BsTrafficUsage))).all()) == 1


async def test_multi_node_failure_rolls_back_earlier_settlement(postgres_database):
    from app.database.models import BsTrafficObservation, BsTrafficUsageCursor
    from tests.database.test_bs_traffic_intake_postgres import NODE
    from tests.database.test_bs_traffic_ledger_postgres import GENERATION, SCOPE

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        source = await db.scalar(select(BsTrafficObservation))
        second = '33333333-3333-4333-8333-333333333333'
        db.add(
            BsTrafficObservation(
                panel_scope=SCOPE,
                panel_user_id=12,
                account_generation=GENERATION,
                node_uuid=second,
                day=NOW.date(),
                high_water_bytes=60,
                latest_bytes=50,
                latest_poll_id=source.latest_poll_id,
                needs_review=True,
            )
        )
        db.add(
            BsTrafficUsageCursor(
                subscription_id=1,
                node_uuid=second,
                day=NOW.date(),
                account_generation=GENERATION,
                traffic_group='bs',
                baseline_bytes=0,
                processed_bytes=0,
                needs_review=False,
            )
        )
        await db.flush()
        with pytest.raises(LedgerConflict):
            await grant_with_reported_settlement(db, **PURCHASE)
        assert await remaining(db) == 100
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE, NOW.date()))
        assert cursor.processed_bytes == 500
        assert not (await db.scalars(select(BsTrafficUsage))).all()
