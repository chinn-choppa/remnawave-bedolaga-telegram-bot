from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficPoll, BsTrafficUsageCursor
from app.services.bs_traffic import cutover_baseline
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_cutover_baseline_postgres import TABLES, observe
from tests.database.test_bs_traffic_managed_cutover_postgres import setup
from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_enrollment_preserves_explicit_counter_and_paid_grants(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        _, day = await observe(db, reader, keys, amount=123)
        result = await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)
        assert result.node_count == 1
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE_A, day))
        assert (cursor.baseline_bytes, cursor.processed_bytes) == (123, 123)
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800
        assert (await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)).fingerprint == result.fingerprint


async def test_partial_coverage_rolls_back_all_new_cursors(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch, bs='finite')
        await observe(db, reader, keys)
        with pytest.raises(LedgerConflict):
            await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0


async def test_replay_does_not_skip_new_usage(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        _, day = await observe(db, reader, keys, amount=100)
        await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)
        await observe(db, reader, keys, amount=150)
        with pytest.raises(LedgerConflict):
            await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)
        await db.commit()
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE_A, day))
        assert (cursor.baseline_bytes, cursor.processed_bytes) == (100, 100)


async def test_stale_evidence_is_not_enrolled(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        poll, _ = await observe(db, reader, keys)
        poll.created_at -= timedelta(minutes=1)
        await db.flush()
        with pytest.raises(LedgerConflict):
            await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0


async def test_two_finite_groups_are_enrolled_together(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch, bs='finite')
        await observe(db, reader, keys, amount=123)
        await observe(db, reader, keys, node=NODE_B, amount=0)
        assert (await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)).node_count == 2
        assert await db.scalar(select(func.count()).select_from(BsTrafficPoll)) == 2


async def test_missing_baseline_after_verified_write_cannot_hide_usage(postgres_database, monkeypatch):
    from sqlalchemy import delete

    from app.services.bs_traffic.cutover_sender import send_cutover
    from tests.database.test_bs_traffic_cutover_sender_postgres import setup as staged_setup

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await staged_setup(db, monkeypatch)
        assert await send_cutover(reader, **keys, operation_id=op) == 'verified'
        await db.execute(delete(BsTrafficUsageCursor))
        await db.commit()
        with pytest.raises(LedgerConflict, match='possible panel write'):
            await cutover_baseline.enroll_cutover_baseline(db, reader, **keys)
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0


async def test_concurrent_enrollment_preserves_single_baseline(postgres_database, monkeypatch):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        await observe(db, reader, keys)
        await db.commit()

        async def enroll():
            async with reader._factory() as session, session.begin():
                return await cutover_baseline.enroll_cutover_baseline(session, reader, **keys)

        left, right = await asyncio.gather(enroll(), enroll())
        assert left.fingerprint == right.fingerprint
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 1


async def test_wrong_owner_cannot_enroll(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        await observe(db, reader, keys)
        with pytest.raises(LedgerConflict):
            await cutover_baseline.enroll_cutover_baseline(db, reader, **{**keys, 'user_id': 2})
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsageCursor)) == 0
