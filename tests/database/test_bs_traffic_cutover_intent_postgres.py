import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficCutoverIntent, Subscription
from app.services.bs_traffic.cutover_intent import cancel_pending_cutover, claim_cutover, stage_cutover
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_cutover import prepare_managed_cutover
from tests.database.test_bs_traffic_managed_cutover_postgres import setup
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def staged(db, monkeypatch):
    reader, api, keys = await setup(db, monkeypatch)
    plan = await prepare_managed_cutover(reader, **keys)
    op = await stage_cutover(db, reader, **keys, operation_key='cutover-1', plan=plan)
    await db.commit()
    return reader, keys, plan, op


async def test_durable_attempt_is_never_reclaimed_or_cancelled(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await staged(db, monkeypatch)
        claim = await claim_cutover(db, reader, **keys, operation_id=op)
        await db.commit()
        assert claim.operation_id == op and claim.target == plan.target
        assert claim.send_deadline <= plan.valid_until
        assert await claim_cutover(db, reader, **keys, operation_id=op) is None
        assert not await cancel_pending_cutover(db, **keys, operation_id=op)
        assert await stage_cutover(db, reader, **keys, operation_key='cutover-1', plan=plan) == op
        with pytest.raises(LedgerConflict, match='requires reconciliation'):
            await stage_cutover(db, reader, **keys, operation_key='cutover-2', plan=plan)
        await db.commit()
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
        row = await db.get(BsTrafficCutoverIntent, op)
        row.send_deadline = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        assert await claim_cutover(db, reader, **keys, operation_id=op) is None
        assert row.state == 'attempted'


async def test_concurrent_claimers_get_one_token(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await staged(db, monkeypatch)

        async def claim():
            async with reader._factory() as worker, worker.begin():
                return await claim_cutover(worker, reader, **keys, operation_id=op)

        results = await asyncio.gather(claim(), claim())
        assert sum(item is not None for item in results) == 1


async def test_uncommitted_claim_rolls_back_and_pending_can_be_cancelled(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await staged(db, monkeypatch)
        await claim_cutover(db, reader, **keys, operation_id=op)
        await db.rollback()  # No HTTP may be performed before commit.
        assert await cancel_pending_cutover(db, **keys, operation_id=op)
        replacement = await stage_cutover(db, reader, **keys, operation_key='cutover-2', plan=plan)
        await db.commit()
        assert replacement != op
        assert await db.scalar(select(func.count()).select_from(BsTrafficCutoverIntent)) == 2


@pytest.mark.parametrize('change', ['target', 'guard', 'deadline', 'owner'])
async def test_staging_rejects_modified_or_foreign_plan(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        plan = await prepare_managed_cutover(reader, **keys)
        if change == 'target':
            plan = replace(plan, target=replace(plan.target, hwid_device_limit=50))
        elif change == 'guard':
            plan = replace(plan, guard_hash='b' * 64)
        elif change == 'deadline':
            plan = replace(plan, valid_until=plan.valid_until + timedelta(minutes=5))
        else:
            keys = dict(keys, user_id=2)
        with pytest.raises(LedgerConflict):
            await stage_cutover(db, reader, **keys, operation_key='bad', plan=plan)
        await db.commit()
        assert await db.scalar(select(BsTrafficCutoverIntent.id)) is None


async def test_changed_rights_prevent_claim_without_consuming_attempt(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await staged(db, monkeypatch)
        (await db.get(Subscription, 1)).device_limit += 1
        await db.commit()
        with pytest.raises(LedgerConflict, match='rights changed'):
            await claim_cutover(db, reader, **keys, operation_id=op)
        await db.commit()
        row = await db.get(BsTrafficCutoverIntent, op)
        assert row.state == 'pending' and row.attempt_token is None


async def test_expired_pending_is_cancelled_not_sent(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await staged(db, monkeypatch)
        (await db.get(BsTrafficCutoverIntent, op)).valid_until = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        assert await claim_cutover(db, reader, **keys, operation_id=op) is None
        await db.commit()
        assert (await db.get(BsTrafficCutoverIntent, op)).state == 'cancelled'


async def test_migration_repeated_upgrade_and_downgrade_keep_attempt(postgres_database, monkeypatch):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0149_bs_cutover_intent.py')
    async with postgres_session(postgres_database, TABLES) as db:

        def install(conn):
            table = BsTrafficCutoverIntent.__table__
            table.drop(conn)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
            inspector = sa.inspect(conn)
            assert {c['name'] for c in inspector.get_columns(table.name)} == set(table.c.keys())
            assert {c['name'] for c in inspector.get_check_constraints(table.name)} == {
                c.name for c in table.constraints if isinstance(c, sa.CheckConstraint)
            }

        await (await db.connection()).run_sync(install)
        reader, keys, _, op = await staged(db, monkeypatch)
        attempt = await claim_cutover(db, reader, **keys, operation_id=op)
        await db.commit()

        def preserve(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.downgrade()
                migration.upgrade()

        await (await db.connection()).run_sync(preserve)
        assert (await db.get(BsTrafficCutoverIntent, op)).attempt_token == attempt.token
        assert await claim_cutover(db, reader, **keys, operation_id=op) is None


async def test_replay_cannot_extend_deadline_and_corrupt_target_is_not_claimed(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await staged(db, monkeypatch)
        with pytest.raises(LedgerConflict, match='original terms'):
            await stage_cutover(
                db,
                reader,
                **keys,
                operation_key='cutover-1',
                plan=replace(plan, valid_until=plan.valid_until + timedelta(seconds=1)),
            )
        row = await db.get(BsTrafficCutoverIntent, op)
        row.target = dict(row.target, hwid_device_limit=999)
        await db.commit()
        with pytest.raises(LedgerConflict, match='Stored restore target is invalid'):
            await claim_cutover(db, reader, **keys, operation_id=op)
        await db.commit()
        assert row.state == 'pending' and row.attempt_token is None


@pytest.mark.parametrize('changed', [False, True])
async def test_recovery_requires_full_fresh_readback_without_activating(postgres_database, monkeypatch, changed):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.cutover_recovery import recover_cutover

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await staged(db, monkeypatch)
        await claim_cutover(db, reader, **keys, operation_id=op)
        await db.commit()
        reader._api.get_split_grace_snapshot = AsyncMock(
            return_value=(replace(plan.target, hwid_device_limit=99) if changed else plan.target)
        )
        result = await recover_cutover(reader, **keys, operation_id=op)
        assert result == ('unconfirmed' if changed else 'verified')
        async with reader._factory() as check:
            row = await check.get(BsTrafficCutoverIntent, op)
            assert row.state == ('attempted' if changed else 'verified')
            assert (await check.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
        assert reader._api.get_split_grace_snapshot.await_count == 1
        if not changed:
            assert await recover_cutover(reader, **keys, operation_id=op) == 'verified'
            assert reader._api.get_split_grace_snapshot.await_count == 1  # Historical receipt only.


async def test_recovery_rights_change_during_http_does_not_verify(postgres_database, monkeypatch):
    from app.services.bs_traffic.cutover_recovery import recover_cutover

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await staged(db, monkeypatch)
        await claim_cutover(db, reader, **keys, operation_id=op)
        await db.commit()

        async def read(_):
            async with reader._factory() as other, other.begin():
                sub = await other.scalar(select(Subscription).where(Subscription.id == 1).with_for_update(nowait=True))
                sub.device_limit += 1
            return plan.target

        reader._api.get_split_grace_snapshot.side_effect = read
        assert await recover_cutover(reader, **keys, operation_id=op) == 'obsolete'
        async with reader._factory() as check:
            assert (await check.get(BsTrafficCutoverIntent, op)).state == 'attempted'
