"""Sparse provenance survives idle days; it never makes the whole day free."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from app.config import settings
from app.database.models import (
    BsTrafficCollectionJob as Job,
    BsTrafficFleetMember as Member,
    BsTrafficFleetNode as Node,
    BsTrafficFleetRound as Round,
    BsTrafficLedger as Ledger,
    BsTrafficObservation as Raw,
    BsTrafficUsage as Usage,
    BsTrafficUsageCursor as Cursor,
)
from app.services.bs_traffic.intake import settle_reported_observation
from app.services.bs_traffic.observations import begin_poll, record_poll
from app.services.bs_traffic.omitted_baseline import complete_omission, service_omitted_node
from app.services.bs_traffic.queue import enqueue_job
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import NODE_B
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase, purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def sparse(db, reader, keys, moment):
    day = moment.date()
    old = await db.get(Round, reader.panel_scope)
    if old is not None:
        await db.delete(old)
        await db.flush()
    round_id = str(uuid4())
    db.add(
        Round(
            panel_scope=reader.panel_scope,
            round_id=round_id,
            day=day,
            upper_id=1,
            cursor=1,
            phase='collecting',
            started_at=moment,
            error_count=0,
        )
    )
    await db.flush()
    poll = await begin_poll(
        db, panel_scope=reader.panel_scope, day=day, node_uuids=(NODE_B,), identities={12: keys['account_generation']}
    )
    await record_poll(db, poll.id, {'nodes': [{'uuid': NODE_B, 'users': []}]})
    poll.created_at = moment
    job_id = await enqueue_job(
        db,
        operation_key='sparse:' + round_id,
        panel_scope=reader.panel_scope,
        day=day,
        node_uuids=(NODE_B,),
        identities={12: keys['account_generation']},
    )
    job = await db.get(Job, job_id)
    job.state, job.poll_id, job.identities = 'complete', poll.id, {}
    db.add(
        Member(
            round_id=round_id,
            subscription_id=1,
            user_id=1,
            panel_user_id=12,
            account_generation=keys['account_generation'],
            nodes=[NODE_B],
        )
    )
    db.add(Node(round_id=round_id, node_uuid=NODE_B, job_id=job_id))
    await db.flush()
    return poll


async def test_sparse_idle_days_first_delta_only_then_reported_charge(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await purchased(db, monkeypatch)
        today = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
        await db.execute(delete(Raw).where(Raw.node_uuid == NODE_B))
        poll = await sparse(db, reader, keys, today)
        state = await db.get(Ledger, 1)
        cursor = await db.get(Cursor, (1, NODE_B, today.date()))
        if cursor is None:
            cursor = Cursor(
                subscription_id=1,
                node_uuid=NODE_B,
                day=today.date(),
                account_generation=keys['account_generation'],
                ownership_cycle=state.ownership_cycle,
                traffic_group='bs',
                needs_review=False,
            )
            db.add(cursor)
        cursor.baseline_bytes = cursor.processed_bytes = 0
        cursor.last_poll_started_at = None
        cursor.bootstrap_evidence = dict(
            version=1,
            kind='omitted',
            scope=reader.panel_scope,
            panel_id=12,
            generation=keys['account_generation'],
            cycle=state.ownership_cycle,
            node=NODE_B,
            day=today.date().isoformat(),
            poll_id=poll.id,
            response_hash=poll.response_hash,
            source_cutover_id=1,
            source_hash='a' * 64,
            first_observed_day=None,
            first_observed_poll_id=None,
        )
        await db.flush()
        # The next idle day has complete sparse coverage and no invented raw.
        tomorrow = today + timedelta(days=3)
        await sparse(db, reader, keys, tomorrow)
        assert await service_omitted_node(
            db, keys=keys, state=state, node=NODE_B, group='bs', day=tomorrow.date(), now=tomorrow, since=tomorrow
        )
        assert await db.get(Raw, (reader.panel_scope, 12, keys['account_generation'], NODE_B, tomorrow.date())) is None
        for amount, expected_kind, expected_charge in ((100, 'raw_pending', 0), (140, 'reported', 40)):
            sample, day = await observe(db, reader, keys, node=NODE_B, day=tomorrow.date(), amount=amount)
            sample.created_at = tomorrow + timedelta(seconds=amount)
            await db.flush()
            event_id = await settle_reported_observation(db, **keys, node_uuid=NODE_B, day=day)
            event = await db.get(Usage, event_id)
            assert event.evidence_kind == expected_kind and event.charged_bytes == expected_charge
        # Empty first poll on another day does not restart whole-day amnesty.
        later = tomorrow + timedelta(days=1)
        await sparse(db, reader, keys, later)
        assert await service_omitted_node(
            db, keys=keys, state=state, node=NODE_B, group='bs', day=later.date(), now=later, since=later
        )
        for amount, expected_kind, expected_charge in ((80, 'raw_pending', 0), (110, 'reported', 30)):
            sample, day = await observe(db, reader, keys, node=NODE_B, day=later.date(), amount=amount)
            sample.created_at = later + timedelta(seconds=amount)
            await db.flush()
            event_id = await settle_reported_observation(db, **keys, node_uuid=NODE_B, day=day)
            event = await db.get(Usage, event_id)
            assert event.evidence_kind == expected_kind and event.charged_bytes == expected_charge


@pytest.mark.parametrize(
    'fault',
    ['missing_member', 'wrong_generation', 'unfinished', 'stale', 'before_boundary', 'raw_review', 'unproven_payload'],
)
async def test_omission_never_replaces_unproven_or_reviewed_evidence(postgres_database, monkeypatch, fault):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, keys, _ = await prepared_purchase(db, monkeypatch)
        now = datetime.now(UTC)
        await db.execute(delete(Raw).where(Raw.node_uuid == NODE_B))
        poll = await sparse(db, reader, keys, now)
        if fault == 'missing_member':
            await db.execute(delete(Member))
        elif fault == 'wrong_generation':
            (await db.scalar(select(Member))).account_generation = 'f' * 64
        elif fault == 'unfinished':
            (await db.scalar(select(Job).where(Job.poll_id == poll.id))).state = 'pending'
        elif fault == 'unproven_payload':
            poll.response_nodes = None
        elif fault == 'stale':
            poll.created_at = now - timedelta(minutes=1)
        elif fault == 'raw_review':
            await observe(db, reader, keys, node=NODE_B, amount=200)
            (await db.scalar(select(Raw).where(Raw.node_uuid == NODE_B))).needs_review = True
        await db.flush()
        assert (
            await complete_omission(
                db,
                scope=reader.panel_scope,
                subscription_id=1,
                panel_id=12,
                generation=keys['account_generation'],
                node=NODE_B,
                day=now.date(),
                now=now,
                since=now + timedelta(seconds=1) if fault == 'before_boundary' else None,
            )
            is None
        )


async def test_omitted_provenance_migration_refuses_evidence_loss(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0164_bs_omitted_baseline.py')
    async with postgres_session(postgres_database, TABLES) as db:

        def install(connection):
            with Operations.context(MigrationContext.configure(connection)):
                migration.downgrade()
                migration.upgrade()

        await (await db.connection()).run_sync(install)
        await prepared_purchase(db, monkeypatch)
        cursor = await db.scalar(select(Cursor))
        cursor.bootstrap_evidence = {'version': 1, 'kind': 'omitted'}
        await db.commit()

        def refuse(connection):
            with Operations.context(MigrationContext.configure(connection)):
                with pytest.raises(RuntimeError, match='provenance'):
                    migration.downgrade()

        await (await db.connection()).run_sync(refuse)
