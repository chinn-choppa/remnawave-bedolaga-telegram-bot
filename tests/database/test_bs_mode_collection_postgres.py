"""Targeted checkout collection retains the queue's shared safety gates."""

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficCollectionJob
from app.services.bs_traffic.queue import claim_job, run_once
from tests.database.test_bs_traffic_queue_postgres import JOB_TABLES, NODE, SCOPE, enqueue
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_targeted_claim_does_not_steal_earlier_job(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        earlier = await enqueue(db, 'earlier')
        target = await enqueue(db, 'checkout')
        claim = await claim_job(db, SCOPE, job_id=target)
        assert claim.job_id == target
        assert (await db.get(BsTrafficCollectionJob, earlier)).state == 'pending'


@pytest.mark.parametrize('gate', ['scope', 'cooldown', 'slot', 'leased', 'future', 'complete'])
async def test_targeted_claim_preserves_shared_gates(postgres_database, gate):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        identifier = await enqueue(db)
        now = await db.scalar(select(func.clock_timestamp()))
        if gate in ('cooldown', 'slot'):
            db.add(
                BsTrafficCollectionBudget(
                    panel_scope=SCOPE,
                    **{
                        'cooldown_until' if gate == 'cooldown' else 'slot_until': now + timedelta(minutes=1),
                        **({'slot_token': str(uuid4())} if gate == 'slot' else {}),
                    },
                )
            )
        if gate == 'leased':
            await claim_job(db, SCOPE)
        job = await db.get(BsTrafficCollectionJob, identifier)
        if gate == 'future':
            job.due_at = now + timedelta(minutes=1)
        if gate == 'complete':
            job.state = 'complete'
        await db.flush()
        assert await claim_job(db, NODE if gate == 'scope' else SCOPE, job_id=identifier) is None


@pytest.mark.parametrize('gate', ['pause', 'admission'])
async def test_targeted_run_cannot_bypass_admission(postgres_database, gate):
    from app.services.bs_traffic.panel_admission import PANEL_SLOTS, _slot_key
    from app.services.bs_traffic.panel_pause import record_pause

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        identifier = await enqueue(db)
        if gate == 'pause':
            await record_pause(db, panel_scope=SCOPE, seconds=60)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        api = AsyncMock()
        async with factory() as holder, holder.begin():
            if gate == 'admission':
                for slot in range(PANEL_SLOTS):
                    await holder.execute(select(func.pg_advisory_xact_lock(_slot_key(SCOPE, slot))))
            assert await run_once(factory, api, SCOPE, job_id=identifier) is False
        api.get_daily_nodes_usage.assert_not_awaited()
        await db.refresh(await db.get(BsTrafficCollectionJob, identifier))
        job = await db.get(BsTrafficCollectionJob, identifier)
        assert job.state == 'pending' and job.attempts == 0


@pytest.mark.parametrize('fault', [None, 'identity', 'generation', 'partial', 'stale', 'scope'])
async def test_completed_membership_proof_is_exact_after_identity_cleanup(postgres_database, fault):
    from importlib import import_module, util

    from app.database.models import BsTrafficPoll
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.queue import enqueue_job, finish_job

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        request_time = await db.scalar(select(func.clock_timestamp()))
        identifier = await enqueue_job(
            db,
            operation_key='proof',
            panel_scope=SCOPE,
            day=request_time.date(),
            node_uuids=(NODE,),
            identities={7: 'generation-a', **({8: 'generation-b'} if fault == 'partial' else {})},
        )
        claim = await claim_job(db, SCOPE, job_id=identifier)
        await finish_job(db, claim, {'nodes': []})
        poll = await db.get(BsTrafficPoll, claim.poll_id)
        now = await db.scalar(select(func.clock_timestamp()))
        if fault == 'stale':
            poll.created_at = now - timedelta(minutes=1)
        await db.flush()
        assert util.find_spec('app.services.bs_traffic.mode_collection') is not None
        proof = import_module('app.services.bs_traffic.mode_collection').completed_mode_collection
        args = dict(
            job_ids=(identifier,),
            scope=NODE if fault == 'scope' else SCOPE,
            panel_id=8 if fault == 'identity' else 7,
            generation='different' if fault == 'generation' else 'generation-a',
            nodes=(NODE,),
            day=now.date(),
            now=now,
        )
        if fault:
            with pytest.raises(LedgerConflict):
                await proof(db, **args)
        else:
            result = await proof(db, **args)
            assert result[NODE].id == poll.id
