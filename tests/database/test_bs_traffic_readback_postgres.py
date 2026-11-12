"""Recovery reads the exact panel account and never repeats a write."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficDelivery, BsTrafficLedger, Subscription, User
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.readback import recover_delivery
from tests.database.test_bs_traffic_delivery_postgres import GUARD, SQUAD, stage, take
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, TARGET, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def panel(**changes):
    values = dict(id=12, created_at=datetime(2026, 1, 1, tzinfo=UTC), active_internal_squads=[{'uuid': SQUAD}])
    values.update(changes)
    return SimpleNamespace(**values)


async def prepare(db):
    await seed(db)
    generation = account_generation(panel_scope=SCOPE, account=panel())
    await db.execute(
        update(BsTrafficLedger).where(BsTrafficLedger.subscription_id == 1).values(account_generation=generation)
    )
    op = await stage(db, account_generation=generation)
    attempt = await take(db, op, account_generation=generation)
    await db.execute(
        update(BsTrafficDelivery)
        .where(BsTrafficDelivery.id == op)
        .values(attempted_at=datetime.now(UTC) - timedelta(seconds=16))
    )
    await db.commit()
    return op, attempt, dict(TARGET, account_generation=generation)


async def run(db, api, op, target):
    return await recover_delivery(
        async_sessionmaker(db.bind, expire_on_commit=True), api, **target, operation_id=op, current_guard_hash=GUARD
    )


async def test_recovery_verifies_readback_without_repeating_patch(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(return_value=panel()), update_user=AsyncMock())
        assert await run(db, api, op, target) == 'verified'
        api.get_split_access_snapshot.assert_awaited_once_with(12)
        api.update_user.assert_not_awaited()
        assert await run(db, api, op, target) == 'already_verified'
        assert api.get_split_access_snapshot.await_count == 1


@pytest.mark.parametrize(
    'account',
    [
        None,
        panel(id=13),
        panel(created_at=datetime(2026, 2, 1, tzinfo=UTC)),
        panel(active_internal_squads=None),
        panel(active_internal_squads=[{'uuid': SQUAD}, {'uuid': SQUAD}]),
    ],
)
async def test_missing_changed_or_malformed_account_cannot_confirm(postgres_database, account):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(return_value=account))
        assert await run(db, api, op, target) == 'unconfirmed'
        assert (await db.get(BsTrafficDelivery, op)).state == 'attempted'


async def test_timeout_never_repeats_and_retains_attempt(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=TimeoutError))
        assert await run(db, api, op, target) == 'unconfirmed'
        assert api.get_split_access_snapshot.await_count == 1
        assert (await db.get(BsTrafficDelivery, op)).state == 'attempted'


async def test_status_changed_during_network_read_cannot_verify_old_access(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)

        async def read(panel_id):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await other.execute(update(Subscription).where(Subscription.id == 1).values(status='disabled'))
            return panel()

        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=read))
        assert await run(db, api, op, target) == 'attention'


async def test_recovery_obeys_existing_grace_advisory_lock(postgres_database):
    import asyncio

    from sqlalchemy import text

    from app.services.grace_access_runtime import _POSTGRES_GLOBAL_PANEL_LOCK_ID, _POSTGRES_LOCK_NAMESPACE

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        called = asyncio.Event()

        async def read(panel_id):
            called.set()
            return panel()

        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=read))
        maker = async_sessionmaker(db.bind)
        async with maker() as holder:
            await holder.execute(
                text('SELECT pg_advisory_xact_lock(:namespace, :lock_id)'),
                {'namespace': _POSTGRES_LOCK_NAMESPACE, 'lock_id': _POSTGRES_GLOBAL_PANEL_LOCK_ID},
            )
            task = asyncio.create_task(run(db, api, op, target))
            try:
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(called.wait(), 0.15)
            finally:
                await holder.rollback()
                result = await asyncio.wait_for(task, 5)
        assert result == 'verified'


async def test_mismatched_squads_leave_attention_without_writes(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(return_value=panel(active_internal_squads=[])))
        assert await run(db, api, op, target) == 'attention'


async def test_cancellation_preserves_attempt_and_propagates(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=asyncio.CancelledError))
        with pytest.raises(asyncio.CancelledError):
            await run(db, api, op, target)
        assert (await db.get(BsTrafficDelivery, op)).state == 'attempted'


async def test_read_deadline_does_not_hold_sql_locks(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic import readback

    monkeypatch.setattr(readback, 'READ_TIMEOUT', 0.05)
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)

        async def hang(panel_id):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await other.execute(update(Subscription).where(Subscription.id == 1).values(status='disabled'))
            await asyncio.Event().wait()

        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=hang))
        assert await run(db, api, op, target) == 'unconfirmed'
        assert (await db.get(BsTrafficDelivery, op)).state == 'attempted'


async def test_concurrent_verification_is_reported_as_history_not_fresh_success(postgres_database):
    from app.services.bs_traffic.delivery_store import reconcile

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, attempt, target = await prepare(db)

        async def read(panel_id):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                await reconcile(
                    other,
                    **target,
                    operation_id=op,
                    token=attempt.token,
                    current_guard_hash=GUARD,
                    observed_squads=(SQUAD,),
                )
                await other.execute(update(Subscription).where(Subscription.id == 1).values(status='disabled'))
            return panel()

        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=read))
        assert await run(db, api, op, target) == 'already_verified'


async def test_recovery_waits_for_live_sender_window(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        await db.execute(
            update(BsTrafficDelivery).where(BsTrafficDelivery.id == op).values(attempted_at=datetime.now(UTC))
        )
        await db.commit()
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock())
        assert await run(db, api, op, target) == 'in_progress'
        api.get_split_access_snapshot.assert_not_awaited()


@pytest.mark.parametrize('expires_during_read', [False, True])
async def test_recovery_rechecks_explicit_evidence_before_and_after_http(postgres_database, expires_during_read):
    import asyncio
    from dataclasses import replace

    from tests.database.test_bs_traffic_policy_postgres import evidence

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        op, _, target = await prepare(db)
        value = replace(
            evidence(), valid_until=datetime.now(UTC) + timedelta(seconds=0.5 if expires_during_read else -1)
        )

        async def snapshot(panel_id):
            await asyncio.sleep(0.6)
            return panel()

        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(side_effect=snapshot))
        assert (
            await recover_delivery(
                async_sessionmaker(db.bind),
                api,
                **target,
                operation_id=op,
                current_guard_hash=GUARD,
                topology_evidence=value,
            )
            == 'unconfirmed'
        )
        assert api.get_split_access_snapshot.await_count == int(expires_during_read)
        assert (await db.get(BsTrafficDelivery, op)).state == 'attempted'
