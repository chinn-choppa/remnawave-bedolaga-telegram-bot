"""Real PostgreSQL queue rotation; simulated account cost, not a throughput benchmark."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import (
    BsTrafficCollectionJob as Job,
    BsTrafficFleetNode as Node,
    BsTrafficFleetRound as Round,
    BsTrafficFleetWork as Work,
    BsTrafficLedger as Ledger,
    BsTrafficPoll as Poll,
)
from app.services.bs_traffic import fleet_round, fleet_settlement
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.queue import run_once
from tests.database.test_bs_fleet_round_postgres import bulk_fleet
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('poison', [None, 1])
@pytest.mark.parametrize('whole_empty', [False, True])
async def test_repeated_bounded_rounds_give_tail_a_fresh_opportunity(
    postgres_database, monkeypatch, poison, whole_empty
):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(Ledger, 1)
        ledger.access_policy = dict(ledger.access_policy, bs_mode='disabled')
        await db.flush()
        await bulk_fleet(db, 4)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [] if whole_empty else [{'uuid': node, 'users': []} for node in nodes]
            }
        )
        successful, attempts, stale_attempts = set(), [], []
        fresh_count = 0

        async def account_cost(factory, bound, cache, identifier, token):
            nonlocal fresh_count
            # Simulate a two-account fresh budget without sleeping. The real
            # scheduler, SQL claims, leases, error/backoff and rounds are used.
            async with factory() as current, current.begin():
                poll = await current.scalar(
                    select(Poll)
                    .join(Job, Job.poll_id == Poll.id)
                    .join(Node, Node.job_id == Job.id)
                    .join(Round, Round.round_id == Node.round_id)
                    .where(Round.panel_scope == bound.panel_scope)
                )
                now = await current.scalar(select(func.clock_timestamp()))
                attempts.append(identifier)
                if now >= poll.created_at + timedelta(seconds=30):
                    stale_attempts.append(identifier)
                    raise LedgerConflict('Simulated account reached the unchanged 30-second freshness boundary')
                fresh_count += 1
                if fresh_count == 2:
                    poll.created_at = now - timedelta(seconds=31)
            if identifier == poison:
                raise LedgerConflict('Permanent synthetic account failure')
            successful.add(identifier)
            return True

        monkeypatch.setattr(fleet_settlement, '_settle', account_cost)
        for _ in range(3):
            fresh_count = 0
            page = await fleet_round.plan_page(db, reader, limit=2)
            await db.commit()
            assert page.accounts == 2 and page.more
            page = await fleet_round.plan_page(db, reader, limit=2)
            await db.commit()
            assert page.accounts == 2 and not page.more
            assert await fleet_round.enqueue_node(db, reader)
            await db.commit()
            assert await run_once(reader._factory, reader, reader.panel_scope)
            assert await fleet_round.enqueue_node(db, reader)
            await db.commit()
            for _ in range(4):
                assert await fleet_settlement.run_local_once(reader._factory, reader, object())
            assert await fleet_round.finish_round(db, reader.panel_scope)
            row = await db.get(Round, reader.panel_scope)
            assert row.error_count > 0 and row.last_success_at is None
            # Start another round immediately, retaining real work timestamps.
            row.started_at -= timedelta(seconds=31)
            await db.commit()
        assert len(attempts) == 12 and stale_attempts
        assert successful == {1, 10, 11, 12} - {poison}
        db.expire_all()
        rows = list(await db.scalars(select(Work)))
        assert all(row.claim_token is None and row.lease_until is None for row in rows)


@pytest.mark.parametrize(
    'age,empty,expected', [(0, False, True), (31, False, False), (-5, False, False), (0, True, True)]
)
async def test_fresh_opportunity_stamp_is_not_success_or_a_freshness_override(
    postgres_database, monkeypatch, age, empty, expected
):
    from app.database.models import BsTrafficFleetMember as Member

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(Ledger, 1)
        ledger.access_policy = dict(ledger.access_policy, bs_mode='disabled')
        await db.commit()
        await fleet_round.plan_page(db, reader)
        await fleet_round.enqueue_node(db, reader)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {'nodes': [{'uuid': node, 'users': []} for node in nodes]}
        )
        assert await run_once(reader._factory, reader, reader.panel_scope)
        await fleet_round.enqueue_node(db, reader)
        now = await db.scalar(select(func.clock_timestamp()))
        poll = await db.scalar(select(Poll).join(Job, Job.poll_id == Poll.id))
        poll.created_at = now - timedelta(seconds=age)
        if empty:
            member = await db.scalar(select(Member))
            member.nodes = []
        await db.commit()
        settle = AsyncMock(return_value=False)
        monkeypatch.setattr(fleet_settlement, '_settle', settle)
        assert await fleet_settlement.run_local_once(reader._factory, reader, object(), interval_seconds=120)
        work = await db.get(Work, 1)
        assert (work.last_fresh_attempt_at is not None) is expected
        assert work.error == 'settlement_deferred'
        # Stale fallback is still attempted; the fairness hint cannot silence
        # expiry/restriction/delivery or relax an accounting validator.
        settle.assert_awaited_once()
        assert work.due_at > now


async def test_fairness_column_upgrade_preserves_work_and_lease(postgres_database, monkeypatch):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        db.add(
            Work(
                subscription_id=1,
                panel_scope=reader.panel_scope,
                account_generation='a' * 64,
                target_round='saved',
                due_at=now,
                error='settlement_deferred',
                claim_token='saved-lease',
                lease_until=now + timedelta(seconds=60),
                access_dirty=True,
            )
        )
        await db.commit()
        migration = _load('0166_bs_fleet_fresh_opportunity.py')

        def upgrade(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.downgrade()
                migration.upgrade()
            row = conn.execute(
                sa.text('SELECT target_round,error,claim_token,last_fresh_attempt_at FROM bs_traffic_fleet_work')
            ).one()
            assert tuple(row) == ('saved', 'settlement_deferred', 'saved-lease', None)
            assert 'ix_bs_fleet_fair' in {
                index['name'] for index in sa.inspect(conn).get_indexes('bs_traffic_fleet_work')
            }

        await db.run_sync(lambda sync: upgrade(sync.connection()))


async def test_50000_due_rows_use_fair_index_and_one_bounded_hint(postgres_database, monkeypatch):
    from sqlalchemy import case, delete, insert, literal, or_, text
    from sqlalchemy.dialects import postgresql

    from app.database.models import BsTrafficFleetMember as Member

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        await fleet_round.plan_page(db, reader)
        original = await db.scalar(select(Member))
        await bulk_fleet(db, 50000)
        db.add(
            Member(
                round_id=original.round_id,
                subscription_id=50008,
                user_id=50008,
                panel_user_id=50108,
                account_generation=original.account_generation,
                nodes=original.nodes,
            )
        )
        assert await fleet_round.enqueue_node(db, reader)
        await db.flush()
        assert await fleet_round.enqueue_node(db, reader)
        await db.flush()
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {'nodes': [{'uuid': node, 'users': []} for node in nodes]}
        )
        assert await run_once(reader._factory, reader, reader.panel_scope)
        assert await run_once(reader._factory, reader, reader.panel_scope)
        assert await fleet_round.enqueue_node(db, reader)
        await db.execute(delete(Work))
        now = await db.scalar(select(func.clock_timestamp()))
        await db.execute(
            insert(Work).from_select(
                [
                    'subscription_id',
                    'panel_scope',
                    'account_generation',
                    'target_round',
                    'due_at',
                    'access_dirty',
                    'last_fresh_attempt_at',
                ],
                select(
                    Ledger.subscription_id,
                    Ledger.panel_scope,
                    Ledger.account_generation,
                    literal(original.round_id),
                    literal(now),
                    literal(False),
                    case((Ledger.subscription_id == 50008, None), else_=now),
                ),
            )
        )
        await db.commit()
        await db.execute(text('ANALYZE bs_traffic_fleet_work'))
        query = (
            select(Work, fleet_settlement._fresh_opportunity_at(now))
            .where(
                Work.panel_scope == reader.panel_scope,
                Work.due_at <= now,
                or_(Work.lease_until.is_(None), Work.lease_until <= now),
            )
            .order_by(Work.last_fresh_attempt_at.asc().nulls_first(), Work.due_at, Work.subscription_id)
            .limit(1)
            .with_for_update(of=Work, skip_locked=True)
        )
        sql = str(query.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))
        explanation = (await db.execute(text('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + sql))).scalar_one()
        plan = explanation[0]['Plan']

        def walk(node):
            yield node
            for child in node.get('Plans', []):
                yield from walk(child)

        nodes = list(walk(plan))
        assert any(node.get('Index Name') == 'ix_bs_fleet_fair' for node in nodes)
        assert not any(node['Node Type'] == 'Sort' for node in nodes)
        assert all(node['Actual Loops'] <= 1 for node in nodes if 'Subplan Name' in node)
        selected = (await db.execute(query)).first()
        assert selected[0].subscription_id == 50008 and selected[1] is not None
        print(f'50000 queue rows: {explanation[0]["Execution Time"]}ms; fair index, no sort, <=1 hint execution')


async def test_fast_batch_also_yields_its_next_fresh_opportunity(postgres_database, monkeypatch):
    from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
    from app.services.bs_traffic.fleet_idle import settle_idle_batch
    from app.services.bs_traffic.intake import attach_cursor
    from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_B
    from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
    from tests.database.test_bs_traffic_runtime_postgres import runtime_for

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id)
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 100}]} for node in nodes]
            }
        )
        await runtime_for(reader).run_once()
        work = await db.get(Work, 1)
        assert work.error is None
        now = await db.scalar(select(func.clock_timestamp()))
        work.due_at, work.completed_round, work.last_fresh_attempt_at = now, None, None
        await db.commit()
        assert (
            await settle_idle_batch(
                reader._factory, reader, TopologyEvidenceCache(reader, panel_identity=reader.panel_scope)
            )
            == 1
        )
        await db.refresh(work)
        assert work.last_fresh_attempt_at >= now


@pytest.mark.parametrize('invalid', ['missing_node', 'wrong_day', 'generation'])
async def test_incomplete_collection_never_spends_fresh_opportunity(postgres_database, monkeypatch, invalid):
    from app.database.models import BsTrafficFleetMember as Member

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        await fleet_round.plan_page(db, reader)
        assert await fleet_round.enqueue_node(db, reader)
        await db.flush()
        assert await fleet_round.enqueue_node(db, reader)
        await db.flush()
        jobs = list(await db.scalars(select(Job).order_by(Job.id)))
        assert len(jobs) == 2
        if invalid == 'missing_node':
            jobs[0].state = 'attention'
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {'nodes': [{'uuid': node, 'users': []} for node in nodes]}
        )
        assert await run_once(reader._factory, reader, reader.panel_scope)
        if invalid != 'missing_node':
            assert await run_once(reader._factory, reader, reader.panel_scope)
        assert await fleet_round.enqueue_node(db, reader)
        if invalid == 'wrong_day':
            poll = await db.scalar(select(Poll).join(Job, Job.poll_id == Poll.id).order_by(Poll.id))
            poll.day -= timedelta(days=1)
        if invalid == 'generation':
            member = await db.scalar(select(Member))
            member.account_generation = 'b' * 64
        await db.commit()
        monkeypatch.setattr(fleet_settlement, '_settle', AsyncMock(return_value=False))
        assert await fleet_settlement.run_local_once(reader._factory, reader, object())
        assert (await db.get(Work, 1)).last_fresh_attempt_at is None
