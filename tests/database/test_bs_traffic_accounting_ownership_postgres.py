from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update

from app.database.models import BsTrafficGraceIntent, Subscription, TrafficAccountingState, User
from app.services.bs_traffic.grace_intent import cancel_pending_restore
from app.services.bs_traffic.grace_sender import send_restore
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.traffic_accounting import AccountingConflict
from app.services.traffic_accounting_service import _assert_no_split_delivery, ensure_managed_account
from tests.database.test_bs_traffic_delivery_postgres import seed, stage
from tests.database.test_bs_traffic_grace_sender_postgres import TABLES, prepared
from tests.database.test_bs_traffic_policy_postgres import evidence
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['managed', 'migrating'])
async def test_managed_mode_cannot_stage_split_write(postgres_database, mode):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await db.execute(update(Subscription).where(Subscription.id == 1).values(traffic_accounting_mode=mode))
        with pytest.raises(LedgerConflict, match='ownership transfer'):
            await stage(db)


@pytest.mark.parametrize('mode', ['managed', 'migrating'])
async def test_mode_change_before_grace_send_blocks_patch(postgres_database, mode):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        await db.execute(update(Subscription).where(Subscription.id == 1).values(traffic_accounting_mode=mode))
        await db.commit()
        assert await send_restore(reader, **keys, evidence=evidence()) == 'obsolete'
        api.restore_split_user_once.assert_not_awaited()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'pending'


async def test_persisted_accountant_blocks_split_even_if_mode_was_reset(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        db.add(
            TrafficAccountingState(
                subscription_id=1,
                panel_identity='synthetic',
                lifetime_bytes=100,
                used_bytes=20,
                business_strategy='NO_RESET',
                calendar_timezone='UTC',
                calendar_anchor=datetime.now(UTC),
            )
        )
        await db.flush()
        with pytest.raises(LedgerConflict, match='ownership transfer'):
            await stage(db)
        assert (await db.get(TrafficAccountingState, 1)).used_bytes == 20


async def test_split_operation_blocks_managed_bootstrap_before_any_panel_read(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        panel = AsyncMock()
        with pytest.raises(AccountingConflict, match='Split traffic delivery'):
            await ensure_managed_account(reader._factory, panel, 1)
        assert panel.mock_calls == []
        assert await db.get(TrafficAccountingState, 1) is None


async def test_only_cancelled_grace_attempt_does_not_claim_quota_ownership(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await cancel_pending_restore(db, **keys)
        await _assert_no_split_delivery(db, 1)


async def test_dry_ledger_does_not_activate_split_accounting(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await _assert_no_split_delivery(db, 1)
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'legacy'


async def test_normal_split_operation_also_blocks_managed_migration(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await stage(db)
        with pytest.raises(AccountingConflict, match='Split traffic delivery'):
            await _assert_no_split_delivery(db, 1)


async def test_managed_migration_and_split_staging_cannot_both_take_ownership(postgres_database):
    import asyncio

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import BsTrafficDelivery
    from app.services.traffic_accounting_service import lock_accounting_subscription

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def managed():
            try:
                async with factory() as worker, worker.begin():
                    subscription, _ = await lock_accounting_subscription(worker, 1)
                    await _assert_no_split_delivery(worker, 1)
                    subscription.traffic_accounting_mode = 'migrating'
                return True
            except AccountingConflict:
                return False

        async def split():
            try:
                async with factory() as worker, worker.begin():
                    await stage(worker)
                return True
            except LedgerConflict:
                return False

        won_managed, won_split = await asyncio.gather(managed(), split())
        assert won_managed != won_split
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == ('migrating' if won_managed else 'legacy')
        operation = await db.scalar(select(BsTrafficDelivery.id))
        assert (operation is not None) == won_split


async def test_attempted_restore_becomes_obsolete_after_mode_change(postgres_database):
    from app.services.bs_traffic.grace_intent import claim_restore
    from app.services.bs_traffic.grace_recovery import recover_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await claim_restore(db, **keys) is not None
        await db.execute(update(Subscription).where(Subscription.id == 1).values(traffic_accounting_mode='managed'))
        await db.commit()
        assert await recover_restore(reader, **keys) == 'obsolete'
        api.get_split_grace_snapshot.assert_not_awaited()
        api.restore_split_user_once.assert_not_awaited()
