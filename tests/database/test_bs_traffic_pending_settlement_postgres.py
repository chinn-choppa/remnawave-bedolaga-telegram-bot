"""No new entitlements or classified charges before cutover confirmation."""

from datetime import timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficAllowance, BsTrafficUsage, Subscription, User
from app.services.bs_traffic.ledger import LedgerConflict, open_usage, summary
from tests.database.test_bs_traffic_ledger_postgres import END, NOW, TARGET, issue, resolve, seed, usage
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def pending(db):
    (await db.get(Subscription, 1)).traffic_accounting_mode = 'split_pending'
    await db.commit()


@pytest.mark.parametrize('operation', ['grant', 'classified', 'charge'])
async def test_pending_owner_blocks_new_financial_mutations(postgres_database, operation):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        grant_id = await issue(db)
        usage_id = await usage(db)
        await pending(db)
        with pytest.raises(LedgerConflict, match='pending panel confirmation'):
            if operation == 'grant':
                await issue(db, 'new')
            elif operation == 'classified':
                await usage(db, 'new')
            else:
                await resolve(db, usage_id)
        # Catching the error and committing must not persist any partial debit.
        await db.commit()
        assert (await db.get(BsTrafficAllowance, grant_id)).remaining_bytes == 100
        assert (await db.get(BsTrafficUsage, usage_id)).state == 'pending'
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsage)) == 1


async def test_pending_owner_retains_raw_evidence_without_grant_eligibility(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        grant_id = await issue(db)
        await pending(db)
        event = await open_usage(
            db,
            **TARGET,
            group='bs',
            source_key='late-raw',
            amount_bytes=200,
            interval_start=NOW,
            interval_end=NOW + timedelta(seconds=10),
            evidence_kind='raw_pending',
        )
        await db.commit()
        row = await db.get(BsTrafficUsage, event)
        assert row.eligible_grants == [] and row.state == 'pending'
        with pytest.raises(LedgerConflict, match='not a classified'):
            await resolve(db, event)
        await resolve(db, event, decision='write_off', allow_write_off=True)
        await db.commit()
        assert (await db.get(BsTrafficAllowance, grant_id)).remaining_bytes == 100
        assert row.charged_bytes == 0 and row.unallocated_bytes == 200


async def test_pending_owner_allows_historical_replays_and_balance_reads(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        grant_id = await issue(db)
        usage_id = await usage(db, amount=40)
        await resolve(db, usage_id)
        await pending(db)
        assert await issue(db) == grant_id
        assert await usage(db, amount=40) == usage_id
        await resolve(db, usage_id)
        result = await summary(db, **TARGET, now=NOW + timedelta(seconds=20))
        assert result['bs']['remaining_bytes'] == 60
        await db.commit()


async def test_persisted_pending_mode_overrides_stale_orm_instance(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        cached = await db.get(Subscription, 1)
        cached.traffic_accounting_mode = 'managed'
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        async with factory() as other:
            await pending(other)
        assert cached.traffic_accounting_mode == 'managed'
        with pytest.raises(LedgerConflict, match='pending panel confirmation'):
            await issue(db)
        await db.commit()
        assert await db.scalar(select(BsTrafficAllowance.id)) is None


async def test_real_transfer_freezes_imported_remainders(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import grant
    from app.services.bs_traffic.managed_transfer_commit import commit_managed_transfer
    from tests.database.test_bs_traffic_managed_commit_postgres import ready
    from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES

    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        await commit_managed_transfer(db, **keys)
        await db.commit()
        target = {name: keys[name] for name in ('user_id', 'subscription_id', 'account_generation')}
        with pytest.raises(LedgerConflict, match='pending panel confirmation'):
            await grant(
                db,
                **target,
                group='ordinary',
                operation_key='purchase-during-cutover',
                amount_bytes=100,
                starts_at=NOW,
                expires_at=END,
            )
        await db.commit()
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == 2


async def test_pending_policy_is_frozen_and_never_projects_exhaustion_or_grace(postgres_database):
    from tests.database.test_bs_traffic_policy_postgres import configure, project, seed as policy_seed

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await policy_seed(db)
        evidence = await configure(db)
        await pending(db)
        # Even confirmed zero balances cannot start limited grace mid-transfer.
        projection = await project(db, evidence)
        assert projection.ordinary.state == projection.bs.state == 'transfer_pending'
        assert projection.ordinary.squads is None and projection.bs.squads is None
        assert projection.grace_candidate is None
        await configure(db)  # Exact existing policy is an idempotent read.
        with pytest.raises(LedgerConflict, match='pending panel confirmation'):
            await configure(db, 'unlimited', 'finite')
        await db.commit()
        projection = await project(db, evidence)
        assert projection.ordinary.mode == 'finite'
