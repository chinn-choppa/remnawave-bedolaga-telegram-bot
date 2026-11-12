from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficCollectionJob, BsTrafficObservation
from app.services.bs_traffic import cutover_activation, cutover_collection
from app.services.bs_traffic.cutover_coordinator import advance_cutover
from app.services.bs_traffic.cutover_scan import start_cutover_scan
from app.services.bs_traffic.queue import run_once
from tests.database.test_bs_traffic_cutover_scan_postgres import clone_candidate
from tests.database.test_bs_traffic_cutover_sender_postgres import TABLES as BASE_TABLES, setup
from tests.database.test_bs_traffic_policy_postgres import NODE_A
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [*BASE_TABLES, BsTrafficCollectionJob.__table__]


async def test_two_accounts_share_one_idempotent_collection_job(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await setup(db, monkeypatch, baseline=False)
        await clone_candidate(db, 10)
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        first = await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        assert first.accounts == 2 and len(first.job_ids) == 1
        assert first == await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        row = await db.get(BsTrafficCollectionJob, first.job_ids[0])
        assert row.node_uuids == [NODE_A]
        assert row.identities == {'12': keys['account_generation'], '110': keys['account_generation']}
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 1


async def test_queue_response_drives_enrollment_and_cutover_without_seeded_baseline(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch, baseline=False)
        monkeypatch.setattr(cutover_activation, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
        reader._api.get_daily_nodes_usage = AsyncMock(
            return_value={'nodes': [{'uuid': NODE_A, 'users': [{'id': 12, 'totalBytes': 123}]}]}
        )
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        result = await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        assert len(result.job_ids) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficObservation)) == 0
        await db.commit()
        assert await run_once(reader._factory, reader, reader.panel_scope)
        assert await advance_cutover(reader, **keys, operation_id=op) == 'activated'
        reader._api.get_daily_nodes_usage.assert_awaited_once()
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_retired_binding_cannot_enqueue(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await setup(db, monkeypatch, baseline=False)
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        await retire_binding(db, panel_scope=reader.panel_scope)
        with pytest.raises(LedgerConflict):
            await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 0


async def test_corrupt_policy_does_not_block_other_accounts(postgres_database, monkeypatch):
    from app.database.models import BsTrafficLedger

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await setup(db, monkeypatch, baseline=False)
        await clone_candidate(db, 10)
        (await db.get(BsTrafficLedger, 10)).access_policy = {'invalid': True}
        await db.flush()
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        result = await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        assert result.accounts == 1 and result.rejected == 1
        row = await db.get(BsTrafficCollectionJob, result.job_ids[0])
        assert row.identities == {'12': keys['account_generation']}


async def test_one_node_per_job_does_not_repeat_unrelated_identity_maps(postgres_database, monkeypatch):
    from app.database.models import BsTrafficLedger
    from tests.database.test_bs_traffic_policy_postgres import NODE_B

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await setup(db, monkeypatch, baseline=False)
        await clone_candidate(db, 10)
        state = await db.get(BsTrafficLedger, 10)
        state.access_policy = {**state.access_policy, 'ordinary_mode': 'unlimited', 'bs_mode': 'finite'}
        await db.flush()
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        result = await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        assert len(result.job_ids) == 2
        jobs = (await db.scalars(select(BsTrafficCollectionJob).order_by(BsTrafficCollectionJob.id))).all()
        actual = {tuple(job.node_uuids): job.identities for job in jobs}
        assert actual == {
            (NODE_A,): {'12': keys['account_generation']},
            (NODE_B,): {'110': keys['account_generation']},
        }


async def test_missing_panel_rows_are_not_zero_baselines(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch, baseline=False)
        reader._api.get_daily_nodes_usage = AsyncMock(return_value={'nodes': []})
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        await db.commit()
        assert await run_once(reader._factory, reader, reader.panel_scope)
        assert await advance_cutover(reader, **keys, operation_id=op) == 'accounting_pending'
        reader._api.restore_split_user_once.assert_not_called()
        assert await db.scalar(select(func.count()).select_from(BsTrafficObservation)) == 0


async def test_replay_cannot_rewrite_completed_identity_map(postgres_database, monkeypatch):
    from app.database.models import BsTrafficLedger

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await setup(db, monkeypatch, baseline=False)
        await clone_candidate(db, 10)
        state = await db.get(BsTrafficLedger, 10)
        state.access_policy = {**state.access_policy, 'ordinary_mode': 'unlimited'}
        await db.flush()
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        first = await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(return_value={'nodes': []})
        assert await run_once(reader._factory, reader, reader.panel_scope)
        state.access_policy = {**state.access_policy, 'ordinary_mode': 'finite'}
        await db.flush()
        with pytest.raises(ValueError, match='different request'):
            await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 1
        job = await db.get(BsTrafficCollectionJob, first.job_ids[0], populate_existing=True)
        assert job.state == 'complete' and job.identities == {}


async def test_job_budget_rejects_page_atomically(postgres_database, monkeypatch):
    from app.database.models import BsTrafficLedger
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await setup(db, monkeypatch, baseline=False)
        state = await db.get(BsTrafficLedger, 1)
        state.access_policy = {**state.access_policy, 'bs_mode': 'finite'}
        await db.flush()
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        monkeypatch.setattr(cutover_collection, 'MAX_PAGE_JOBS', 1)
        with pytest.raises(LedgerConflict, match='job budget'):
            await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 0
        result = await cutover_collection.enqueue_cutover_collection(
            db, reader, window, round_key='round-1', nodes_per_job=2
        )
        assert len(result.job_ids) == 1


async def test_concurrent_planners_share_durable_jobs(postgres_database, monkeypatch):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await setup(db, monkeypatch, baseline=False)
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        await db.commit()

        async def plan():
            async with reader._factory() as worker, worker.begin():
                return await cutover_collection.enqueue_cutover_collection(worker, reader, window, round_key='round-1')

        left, right = await asyncio.gather(plan(), plan())
        assert left == right
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 1


async def test_current_panel_id_cannot_belong_to_two_subscriptions(postgres_database, monkeypatch):
    from sqlalchemy import update
    from sqlalchemy.exc import IntegrityError

    from app.database.models import Subscription

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db, monkeypatch, baseline=False)
        with pytest.raises(IntegrityError, match='uq_subscriptions_remnawave_id'):
            async with db.begin_nested():
                await db.execute(update(Subscription).where(Subscription.id == 2).values(remnawave_id=12))


async def test_fifty_accounts_use_one_job_and_fixed_number_of_selects(postgres_database, monkeypatch):
    from sqlalchemy import event

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await setup(db, monkeypatch, baseline=False)
        for value in range(10, 59):
            await clone_candidate(db, value)
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
        queries = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith('SELECT '):
                queries.append(statement)

        event.listen(db.bind.sync_engine, 'before_cursor_execute', capture)
        try:
            result = await cutover_collection.enqueue_cutover_collection(db, reader, window, round_key='round-1')
        finally:
            event.remove(db.bind.sync_engine, 'before_cursor_execute', capture)
        assert result.accounts == 50 and len(result.job_ids) == 1
        assert len(queries) == 5
        row = await db.get(BsTrafficCollectionJob, result.job_ids[0])
        assert len(row.identities) == 50
