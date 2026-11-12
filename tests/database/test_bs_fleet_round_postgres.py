"""Fleet SQL pages share node reads; mocked HTTP is not a live panel benchmark."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, insert, literal, select

from app.database.models import BsTrafficCollectionJob, BsTrafficLedger, TrafficAccountingState
from app.services.bs_traffic import fleet_round
from app.services.bs_traffic.queue import run_once
from tests.database.test_bs_traffic_cutover_scan_postgres import clone_candidate
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_500_accounts_two_pages_one_node_http_after_restart(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, 1)
        policy = dict(ledger.access_policy)
        policy['bs_mode'] = 'disabled'
        ledger.access_policy = policy
        for identifier in range(10, 509):
            await clone_candidate(db, identifier)
            table = TrafficAccountingState.__table__
            await db.execute(
                insert(table).from_select(
                    [c.name for c in table.c],
                    select(
                        *[literal(identifier, type_=c.type) if c.name == 'subscription_id' else c for c in table.c]
                    ).where(table.c.subscription_id == 1),
                )
            )
        await db.commit()
        first = await fleet_round.plan_page(db, reader, limit=250)
        await db.commit()
        assert first.more and first.accounts == 250
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 0
        # No in-memory cursor is handed to the next invocation.
        second = await fleet_round.plan_page(db, reader, limit=250)
        await db.commit()
        assert not second.more
        await fleet_round.enqueue_node(db, reader)
        await db.commit()
        jobs = list(await db.scalars(select(BsTrafficCollectionJob)))
        assert len(jobs) == 1 and len(jobs[0].identities) == 500
        reader._api.get_daily_nodes_usage = AsyncMock(
            return_value={
                'nodes': [
                    {
                        'uuid': jobs[0].node_uuids[0],
                        'users': [{'id': int(k), 'totalBytes': 100} for k in jobs[0].identities],
                    }
                ]
            }
        )
        await db.rollback()
        assert await run_once(reader._factory, reader, reader.panel_scope)
        reader._api.get_daily_nodes_usage.assert_awaited_once()


async def bulk_fleet(db, count):
    """Synthetic enrollment rows; never a claim about real panel identities."""
    from sqlalchemy import String, cast, true

    from app.database.models import BsTrafficCutoverIntent, BsTrafficManagedTransfer, Subscription, User

    series = func.generate_series(10, count + 8).table_valued('id').render_derived()
    await db.execute(insert(User).from_select(['id'], select(series.c.id)))
    for model, key in [
        (Subscription, 'id'),
        (BsTrafficLedger, 'subscription_id'),
        (BsTrafficManagedTransfer, 'subscription_id'),
        (BsTrafficCutoverIntent, 'subscription_id'),
        (TrafficAccountingState, 'subscription_id'),
    ]:
        table = model.__table__
        source = select(table).where(table.c[key] == 1).subquery()
        changes = {key: series.c.id}
        if model is Subscription:
            changes.update(
                user_id=series.c.id,
                remnawave_id=series.c.id + 100,
                remnawave_short_id=literal('synthetic-') + cast(series.c.id, String),
                remnawave_uuid=literal('00000000-0000-4000-8000-') + func.lpad(cast(series.c.id, String), 12, '0'),
            )
        if model is BsTrafficLedger:
            changes['panel_user_id'] = series.c.id + 100
        if model is BsTrafficCutoverIntent:
            changes['id'] = series.c.id
        await db.execute(
            insert(table).from_select(
                [c.name for c in table.c],
                select(*[changes.get(c.name, source.c[c.name]) for c in table.c]).select_from(
                    source.join(series, true())
                ),
            )
        )


@pytest.mark.parametrize('accounts', [10000, 50000])
async def test_shared_fleet_scale_uses_one_mock_http(postgres_database, monkeypatch, accounts):
    import time

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, 1)
        policy = dict(ledger.access_policy)
        policy['bs_mode'] = 'disabled'
        ledger.access_policy = policy
        await db.flush()
        await bulk_fleet(db, accounts)
        await db.commit()
        started = time.monotonic()
        pages = 0
        while True:
            page = await fleet_round.plan_page(db, reader, limit=500)
            await db.commit()
            pages += 1
            if not page.more:
                break
        await fleet_round.enqueue_node(db, reader)
        await db.commit()
        jobs = list(await db.scalars(select(BsTrafficCollectionJob)))
        assert len(jobs) == 1 and len(jobs[0].identities) == accounts
        assert pages == accounts // 500
        reader._api.get_daily_nodes_usage = AsyncMock(
            return_value={
                'nodes': [
                    {
                        'uuid': jobs[0].node_uuids[0],
                        'users': [{'id': int(k), 'totalBytes': 100} for k in jobs[0].identities],
                    }
                ]
            }
        )
        await db.rollback()
        assert await run_once(reader._factory, reader, reader.panel_scope)
        reader._api.get_daily_nodes_usage.assert_awaited_once()
        print(
            f'{accounts} activated synthetic accounts; {pages} SQL pages, 1 mocked node HTTP; '
            f'{time.monotonic() - started:.3f}s planning+raw persistence; excludes settlement/live panel SLO'
        )


async def test_midnight_closes_previous_day_before_current_round(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficFleetMember, BsTrafficFleetNode, BsTrafficFleetRound

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        await fleet_round.plan_page(db, reader)
        row = await db.get(BsTrafficFleetRound, reader.panel_scope)
        now = datetime.now(UTC)
        row.day = now.date() - timedelta(days=1)
        row.phase = 'complete'
        row.started_at = now - timedelta(days=1, seconds=60)
        row.completed_at = now - timedelta(days=1)
        old = row.round_id
        await db.commit()
        await fleet_round.plan_page(db, reader)
        assert row.day == now.date() - timedelta(days=1)
        assert row.round_id != old
        row.phase = 'complete'
        row.started_at = now - timedelta(seconds=60)
        row.completed_at = now
        await db.commit()
        await fleet_round.plan_page(db, reader)
        assert row.day == now.date()
        assert await db.scalar(select(func.count()).select_from(BsTrafficFleetMember)) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficFleetNode)) == 2


async def test_invalid_policy_does_not_pin_later_members(postgres_database, monkeypatch):
    from app.database.models import BsTrafficFleetMember, BsTrafficFleetRound

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        await bulk_fleet(db, 2)
        original = await db.get(BsTrafficLedger, 1)
        original.access_policy = {'bad': 'synthetic'}
        await db.commit()
        first = await fleet_round.plan_page(db, reader, limit=1)
        await db.commit()
        assert first.more
        second = await fleet_round.plan_page(db, reader, limit=1)
        await db.commit()
        assert not second.more
        row = await db.get(BsTrafficFleetRound, reader.panel_scope)
        assert row.error_count == 1 and row.phase == 'collecting'
        assert await db.scalar(select(func.count()).select_from(BsTrafficFleetMember)) == 1


async def test_local_poison_account_does_not_starve_next_due_work(postgres_database, monkeypatch):
    from datetime import UTC, datetime

    from app.database.models import BsTrafficFleetWork
    from app.services.bs_traffic import fleet_settlement

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        await bulk_fleet(db, 2)
        for identifier in (1, 10):
            db.add(
                BsTrafficFleetWork(
                    subscription_id=identifier,
                    panel_scope=reader.panel_scope,
                    account_generation='a' * 64,
                    target_round='synthetic',
                    due_at=datetime.now(UTC),
                    access_dirty=False,
                )
            )
        await db.commit()
        calls = []

        async def settle(factory, bound, cache, identifier, token):
            calls.append(identifier)
            if identifier == 1:
                raise ValueError('synthetic poison')
            return True

        monkeypatch.setattr(fleet_settlement, '_settle', settle)
        assert await fleet_settlement.run_local_once(reader._factory, reader, object())
        assert await fleet_settlement.run_local_once(reader._factory, reader, object())
        assert calls == [1, 10]
        db.expire_all()
        assert (await db.get(BsTrafficFleetWork, 1)).error is not None
        assert (await db.get(BsTrafficFleetWork, 10)).error is None


async def test_new_round_during_local_lease_cannot_be_falsely_completed(postgres_database, monkeypatch):
    from datetime import UTC, datetime

    from sqlalchemy import update

    from app.database.models import BsTrafficFleetWork
    from app.services.bs_traffic import fleet_settlement

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        db.add(
            BsTrafficFleetWork(
                subscription_id=1,
                panel_scope=reader.panel_scope,
                account_generation='a' * 64,
                target_round='old',
                due_at=datetime.now(UTC),
                access_dirty=False,
            )
        )
        await db.commit()

        async def publish(factory, bound, cache, identifier, token):
            async with factory() as other, other.begin():
                await other.execute(
                    update(BsTrafficFleetWork)
                    .where(BsTrafficFleetWork.subscription_id == identifier)
                    .values(target_round='new')
                )
            return True

        monkeypatch.setattr(fleet_settlement, '_settle', publish)
        await fleet_settlement.run_local_once(reader._factory, reader, object())
        work = await db.get(BsTrafficFleetWork, 1)
        assert work.target_round == 'new' and work.completed_round == 'old'
        assert work.due_at <= datetime.now(UTC)


async def test_maintenance_cursor_is_leased_and_stale_writer_is_fenced(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficFleetLane
    from app.services.bs_traffic.runtime import SplitTrafficRuntime

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        first = SplitTrafficRuntime(session_factory=reader._factory)
        second = SplitTrafficRuntime(session_factory=reader._factory)
        assert await first._load_lane(
            reader.panel_scope, 'drain', BsTrafficLedger, BsTrafficLedger.subscription_id
        ) == (0, 1)
        assert (
            await second._load_lane(reader.panel_scope, 'drain', BsTrafficLedger, BsTrafficLedger.subscription_id)
            is None
        )
        lane = await db.get(BsTrafficFleetLane, (reader.panel_scope, 'drain'))
        lane.lease_until = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        assert await second._load_lane(
            reader.panel_scope, 'drain', BsTrafficLedger, BsTrafficLedger.subscription_id
        ) == (0, 1)
        await first._save_lane(reader.panel_scope, 'drain', 1, complete=True, release=True)
        await db.refresh(lane)
        assert lane.upper_id == 1 and lane.completed_at is None


async def test_fleet_migration_creates_schema_and_refuses_dropping_work(postgres_database):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.database.models import BsTrafficFleetRound
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0157_bs_fleet_rounds.py')
    async with postgres_session(postgres_database, TABLES) as db:

        def install(conn):
            for name in (
                'bs_traffic_fleet_lanes',
                'bs_traffic_fleet_work',
                'bs_traffic_fleet_nodes',
                'bs_traffic_fleet_members',
                'bs_traffic_fleet_rounds',
            ):
                conn.execute(sa.text(f'DROP TABLE {name}'))
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
            assert 'lease_token' in {c['name'] for c in sa.inspect(conn).get_columns('bs_traffic_fleet_lanes')}
            assert 'ix_bs_fleet_due' in {i['name'] for i in sa.inspect(conn).get_indexes('bs_traffic_fleet_work')}

        await db.run_sync(lambda sync: install(sync.connection()))
        now = await db.scalar(select(func.clock_timestamp()))
        db.add(
            BsTrafficFleetRound(
                panel_scope='00000000-0000-4000-8000-000000000001',
                round_id='synthetic',
                day=now.date(),
                upper_id=0,
                cursor=0,
                phase='building',
                started_at=now,
                error_count=0,
            )
        )
        await db.flush()

        def refuse(conn):
            with Operations.context(MigrationContext.configure(conn)), pytest.raises(RuntimeError):
                migration.downgrade()

        await db.run_sync(lambda sync: refuse(sync.connection()))


@pytest.mark.parametrize('accounts,delta', [(1000, 1), (10000, 1), (50000, 1), (1000, 0), (10000, 0), (50000, 0)])
async def test_full_fleet_settlement_scale(postgres_database, monkeypatch, accounts, delta):
    """Real SQL accounting, mocked panel; warmed calendar/access metadata only."""
    import asyncio
    import time
    from collections import Counter
    from dataclasses import replace
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import event, true, update
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.database.models import BsTrafficAllowance, BsTrafficFleetRound, BsTrafficFleetWork, BsTrafficUsageCursor
    from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
    from app.services.bs_traffic.fleet_settlement import run_local_once
    from app.services.bs_traffic.intake import attach_cursor
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, 1)
        policy = dict(ledger.access_policy)
        policy['bs_mode'] = 'disabled'
        ledger.access_policy = policy
        node = policy['topology']['ordinary_nodes'][0]
        _, day = await observe(db, reader, target, node, 100)
        await attach_cursor(db, **target, group='ordinary', node_uuid=node, day=day)
        await db.flush()
        await bulk_fleet(db, accounts)
        series = func.generate_series(10, accounts + 8).table_valued('id').render_derived()
        for model in (BsTrafficAllowance, BsTrafficUsageCursor):
            table = model.__table__
            columns = [c for c in table.c if c.name != 'id']
            source = select(table).where(table.c.subscription_id == 1).subquery()
            await db.execute(
                insert(table).from_select(
                    [c.name for c in columns],
                    select(
                        *[series.c.id if c.name == 'subscription_id' else source.c[c.name] for c in columns]
                    ).select_from(source.join(series, true())),
                )
            )
        await db.commit()
        engine = create_async_engine(postgres_database, pool_size=12, max_overflow=0)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        reader = replace(reader, _factory=factory)
        statements = [0]
        sql_profile = Counter()

        @event.listens_for(engine.sync_engine, 'before_cursor_execute')
        def count_statements(*args):
            statements[0] += 1
            sql_profile[' '.join(args[2].split())[:160]] += 1

        started = time.monotonic()
        while True:
            page = await fleet_round.plan_page(db, reader, limit=500)
            await db.commit()
            if not page.more:
                break
        await fleet_round.enqueue_node(db, reader)
        await db.commit()
        job = await db.scalar(select(BsTrafficCollectionJob))
        reader._api.get_daily_nodes_usage = AsyncMock(
            return_value={
                'nodes': [
                    {'uuid': node, 'users': [{'id': int(key), 'totalBytes': 100 + delta} for key in job.identities]}
                ],
            }
        )
        await db.rollback()
        assert await run_once(reader._factory, reader, reader.panel_scope)
        await fleet_round.enqueue_node(db, reader)
        await db.execute(
            update(BsTrafficFleetWork).values(
                applied_squads=[ORDINARY],
                settled_revision=select(BsTrafficLedger.revision)
                .where(BsTrafficLedger.subscription_id == BsTrafficFleetWork.subscription_id)
                .scalar_subquery(),
                settled_at=datetime.now(UTC),
                access_dirty=False,
                calendar_receipt_id=0,
                calendar_due_at=datetime.now(UTC) + timedelta(days=1),
            )
        )
        await db.commit()
        collected = time.monotonic()
        reader._api.get_split_access_snapshot.reset_mock()
        cache = TopologyEvidenceCache(reader, panel_identity=reader.panel_scope)
        from app.services.bs_traffic.fleet_idle import settle_idle_batch

        completed = 0
        while completed < accounts:
            batch = await settle_idle_batch(reader._factory, reader, cache, interval_seconds=3600, settle_deltas=True)
            assert batch, f'Batch stopped at {completed}'
            completed += batch
        # Normal worker retries transient stale topology evidence. Include tail
        # retries in the end-to-end measurement instead of hiding them.
        async with asyncio.timeout(30):
            while await db.scalar(
                select(func.count()).select_from(BsTrafficFleetWork).where(BsTrafficFleetWork.error.is_not(None))
            ):
                await db.rollback()
                results = await asyncio.gather(
                    *(run_local_once(reader._factory, reader, cache, interval_seconds=3600) for _ in range(4))
                )
                if not any(results):
                    await asyncio.sleep(0.1)
        await db.rollback()
        await fleet_round.finish_round(db, reader.panel_scope)
        await db.commit()
        failures = await db.scalar(
            select(func.count()).select_from(BsTrafficFleetWork).where(BsTrafficFleetWork.error.is_not(None))
        )
        row = await db.get(BsTrafficFleetRound, reader.panel_scope)
        from tests.database.test_bs_traffic_runtime_postgres import runtime_for

        healthy = await runtime_for(reader)._health(reader)
        elapsed = time.monotonic() - started
        print(
            f'FULL SQL {accounts} accounts delta={delta}: raw {collected - started:.3f}s, '
            f'settlement {time.monotonic() - collected:.3f}s, total {elapsed:.3f}s, '
            f'errors={failures}, phase={row.phase}, healthy={healthy is not None}, within90s={elapsed < 90}, pooledSQL={statements[0]}',
            flush=True,
        )
        if delta:
            print(sql_profile.most_common(16), flush=True)
        await engine.dispose()
        assert failures == 0
        assert row.phase == 'complete' and row.last_success_at is not None
        assert (healthy is not None) == (elapsed < 90)
        assert (
            await db.scalar(
                select(func.count())
                .select_from(BsTrafficUsageCursor)
                .where(BsTrafficUsageCursor.processed_bytes == 100 + delta)
            )
            == accounts
        )
        reader._api.get_split_access_snapshot.assert_not_awaited()
