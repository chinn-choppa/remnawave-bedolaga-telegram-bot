import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficCutoverIntent, BsTrafficUsageCursor, Subscription
from app.services.bs_traffic import cutover_activation, cutover_coordinator
from app.services.bs_traffic.cutover_sender import send_cutover
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_cutover_sender_postgres import TABLES, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepared(db, monkeypatch):
    reader, keys, plan, op = await setup(db, monkeypatch, baseline=False)
    await observe(db, reader, keys)
    await db.commit()
    monkeypatch.setattr(cutover_activation, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
    return reader, keys, plan, op


async def test_one_step_enrolls_sends_and_activates_without_recredit(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await prepared(db, monkeypatch)
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'activated'
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'activated'
        reader._api.restore_split_user_once.assert_awaited_once()
        async with reader._factory() as check:
            assert (await check.get(Subscription, 1)).traffic_accounting_mode == 'split'
            assert await check.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800


async def test_missing_data_does_not_consume_attempt(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch, baseline=False)
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'accounting_pending'
        reader._api.restore_split_user_once.assert_not_called()
        async with reader._factory() as check:
            assert (await check.get(BsTrafficCutoverIntent, op)).state == 'pending'
            assert await check.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0


async def test_resume_after_verified_write_does_not_patch_again(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch)
        monkeypatch.setattr(cutover_activation, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
        assert await send_cutover(reader, **keys, operation_id=op) == 'verified'
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'activated'
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_concurrent_coordinators_never_duplicate_write(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await prepared(db, monkeypatch)
        results = await asyncio.gather(
            *(cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) for _ in range(2))
        )
        assert 'activated' in results
        assert all(value in ('activated', 'unconfirmed', 'obsolete') for value in results)
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_unknown_write_outcome_never_restarts_enrollment(postgres_database, monkeypatch):
    from sqlalchemy import delete

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await prepared(db, monkeypatch)
        reader._api.restore_split_user_once.side_effect = TimeoutError('synthetic timeout')
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'unconfirmed'
        await db.execute(delete(BsTrafficUsageCursor))
        await db.commit()
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'unconfirmed'
        reader._api.restore_split_user_once.assert_awaited_once()
        async with reader._factory() as check:
            assert await check.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0


async def test_cancellation_preserves_attempt_for_read_only_recovery(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await prepared(db, monkeypatch)

        async def applied_then_cancelled(target):
            reader._api.get_split_grace_snapshot.return_value = target
            raise asyncio.CancelledError

        reader._api.restore_split_user_once.side_effect = applied_then_cancelled
        with pytest.raises(asyncio.CancelledError):
            await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op)
        async with reader._factory() as check:
            assert (await check.get(BsTrafficCutoverIntent, op)).state == 'attempted'
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'activated'
        reader._api.restore_split_user_once.assert_awaited_once_with(plan.target)


async def test_wrong_owner_cannot_advance_or_touch_panel(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await prepared(db, monkeypatch)
        assert (
            await cutover_coordinator.advance_cutover(reader, **{**keys, 'user_id': 2}, operation_id=op) == 'obsolete'
        )
        reader._api.restore_split_user_once.assert_not_called()
        reader._api.get_split_grace_snapshot.assert_not_called()


async def test_delta_between_enrollment_and_send_does_not_consume_paid_package(postgres_database, monkeypatch):
    from app.services.bs_traffic import cutover_sender

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await prepared(db, monkeypatch)

        async def topology(*args, **kwargs):
            async with reader._factory() as check, check.begin():
                await observe(check, reader, keys, amount=200)
            return plan.evidence

        monkeypatch.setattr(cutover_sender, 'collect_topology_evidence', topology)
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'accounting_pending'
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'accounting_pending'
        reader._api.restore_split_user_once.assert_not_called()
        async with reader._factory() as check:
            assert await check.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800
            cursor = await check.scalar(select(BsTrafficUsageCursor))
            assert cursor.baseline_bytes == cursor.processed_bytes == 100


async def test_expired_pending_without_observations_is_cancelled(postgres_database, monkeypatch):
    from datetime import timedelta

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch, baseline=False)
        row = await db.get(BsTrafficCutoverIntent, op)
        row.valid_until = await db.scalar(select(func.clock_timestamp())) - timedelta(seconds=1)
        await db.commit()
        assert await cutover_coordinator.advance_cutover(reader, **keys, operation_id=op) == 'cancelled'
        reader._api.restore_split_user_once.assert_not_called()
        async with reader._factory() as check:
            assert (await check.get(BsTrafficCutoverIntent, op)).state == 'cancelled'
            assert await check.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0
