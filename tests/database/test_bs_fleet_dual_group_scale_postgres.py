"""One warmed two-finite-group round through all runtime lanes at default30s.

Real SQL and cloned funded rights; mocked shared node HTTP, not real VPN clients.
Calendar/access metadata is warmed; maintenance is executed with no paid backlog.
"""

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import event, func, insert, select, true, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database.models import (
    BsTrafficAllowance as Grant,
    BsTrafficCollectionJob as Job,
    BsTrafficFleetRound as Round,
    BsTrafficFleetWork as Work,
    BsTrafficLedger as Ledger,
    BsTrafficUsage as Usage,
    BsTrafficUsageCursor as Cursor,
    Subscription,
    Tariff,
    Transaction,
    User,
)
from app.services.bs_traffic import fleet_idle, fleet_round, fleet_settlement
from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
from app.services.bs_traffic.intake import attach_cursor, settle_reported_observation
from app.services.bs_traffic.purchase import purchase_package
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from app.services.bs_traffic.queue import run_once
from tests.database.test_bs_fleet_round_postgres import bulk_fleet
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('accounts', [100, 20000])
async def test_two_finite_groups_use_default_runtime_and_exact_group_accounting(
    postgres_database, monkeypatch, accounts
):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, bs_purchase, _ = await purchased(db, monkeypatch)
        assert await advance_purchase_delivery(reader, **target, operation_id=bs_purchase.delivery_id) == 'verified'
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.traffic_topup_enabled = True
        tariff.traffic_topup_packages = {'10': 1000}
        await db.commit()
        proof = await TopologyEvidenceCache(reader, panel_identity=reader.panel_scope).get(
            reader, panel_identity=reader.panel_scope, ordinary_squads=[ORDINARY], bs_squads=[BS]
        )
        ordinary_purchase = await purchase_package(
            db,
            **target,
            group='ordinary',
            gb=10,
            expected_price_kopeks=1000,
            operation_key='benchmark-ordinary-paid',
            evidence=proof,
            current_squads=(ORDINARY, BS),
        )
        await db.commit()
        assert (
            await advance_purchase_delivery(reader, **target, operation_id=ordinary_purchase.delivery_id) == 'verified'
        )
        ledger = await db.get(Ledger, 1, populate_existing=True)
        policy = ledger.access_policy
        assert policy['ordinary_mode'] == policy['bs_mode'] == 'finite'
        nodes = {group: policy['topology'][f'{group}_nodes'][0] for group in ('ordinary', 'bs')}
        assert nodes['ordinary'] != nodes['bs']
        for group, node in nodes.items():
            _, day = await observe(db, reader, target, node, 100)
            await attach_cursor(db, **target, group=group, node_uuid=node, day=day)
            # Existing cursors retain their first baseline. Consume a genuine
            # equal-counter observation after both paid purchases, so this
            # steady-state round does not cross an unresolved purchase boundary.
            assert await settle_reported_observation(db, **target, node_uuid=node, day=day) is None
        await db.flush()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
        funded_balance = (await db.get(User, 1)).balance_kopeks
        await bulk_fleet(db, accounts)
        series = func.generate_series(10, accounts + 8).table_valued('id').render_derived()
        for model in (Grant, Cursor):
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
        initial = dict(
            (
                await db.execute(
                    select(Grant.traffic_group, func.sum(Grant.remaining_bytes)).group_by(Grant.traffic_group)
                )
            ).all()
        )
        assert set(initial) == {'ordinary', 'bs'}
        assert await db.scalar(select(func.count()).select_from(Cursor)) == 2 * accounts
        identities = list(await db.scalars(select(Ledger.panel_user_id)))
        await db.commit()
        engine = create_async_engine(postgres_database, pool_size=12, max_overflow=0)
        factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        reader = replace(reader, _factory=factory)
        sql_count = [0]

        @event.listens_for(engine.sync_engine, 'before_cursor_execute')
        def count_sql(*args):
            sql_count[0] += 1

        deltas = {'ordinary': 1024**2, 'bs': 2 * 1024**2}
        node_deltas = {nodes[group]: delta for group, delta in deltas.items()}
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda selected, requested_day, **kw: {
                'nodes': [
                    {'uuid': node, 'users': [{'id': key, 'totalBytes': 100 + node_deltas[node]} for key in identities]}
                    for node in selected
                ],
            }
        )
        started = time.monotonic()
        try:
            while True:
                page = await fleet_round.plan_page(db, reader, limit=500)
                await db.commit()
                if not page.more:
                    break
            while await fleet_round.enqueue_node(db, reader):
                await db.commit()
            await db.commit()
            assert await db.scalar(select(func.count()).select_from(Job).where(Job.state == 'pending')) == 2
            await db.rollback()
            assert await run_once(factory, reader, reader.panel_scope)
            assert await run_once(factory, reader, reader.panel_scope)
            await fleet_round.enqueue_node(db, reader)
            # Steady-state fixture: both paid squads already applied; next
            # calendar refresh genuinely lies in the future. No runtime lane
            # or due interval is replaced, and no evidence timestamp is moved.
            await db.execute(
                update(Work).values(
                    applied_squads=[ORDINARY, BS],
                    settled_revision=select(Ledger.revision)
                    .where(Ledger.subscription_id == Work.subscription_id)
                    .scalar_subquery(),
                    settled_at=datetime.now(UTC),
                    access_dirty=False,
                    calendar_receipt_id=0,
                    calendar_due_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
            await db.commit()
            collected = time.monotonic()
            runtime = runtime_for(reader)
            assert runtime.interval == 30  # Valid constructor default, unchanged.
            assert runtime.factory.kw['autoflush'] is False
            reader._api.get_split_access_snapshot.reset_mock()
            reader._api.update_internal_squads_once.reset_mock()
            fast = fleet_idle.settle_idle_batch
            slow = fleet_settlement.run_local_once
            metrics = {
                'passes': 0,
                'fast_calls': 0,
                'fast_account_updates': 0,
                'zero_batches': 0,
                'fallback_calls': 0,
                'fallback_attempts': 0,
            }

            async def measured_fast(*args, **kwargs):
                result = await fast(*args, **kwargs)
                metrics['fast_calls'] += 1
                metrics['fast_account_updates'] += result
                metrics['zero_batches'] += result == 0
                return result

            async def measured_slow(*args, **kwargs):
                metrics['fallback_calls'] += 1
                result = await slow(*args, **kwargs)
                metrics['fallback_attempts'] += bool(result)
                return result

            with (
                patch.object(fleet_idle, 'settle_idle_batch', measured_fast),
                patch.object(fleet_settlement, 'run_local_once', measured_slow),
            ):
                async with asyncio.timeout(90 - (time.monotonic() - started)):
                    while True:
                        metrics['passes'] += 1
                        healthy = await runtime._process_scope(reader)
                        await db.rollback()
                        row = await db.get(Round, reader.panel_scope, populate_existing=True)
                        successful = await db.scalar(
                            select(func.count())
                            .select_from(Work)
                            .where(
                                Work.completed_round == Work.target_round,
                                Work.error.is_(None),
                                Work.access_dirty.is_(False),
                                Work.lease_until.is_(None),
                            )
                        )
                        if successful == accounts and row.phase == 'complete':
                            break
                        await asyncio.sleep(0.05)
            elapsed = time.monotonic() - started
            remaining = dict(
                (
                    await db.execute(
                        select(Grant.traffic_group, func.sum(Grant.remaining_bytes)).group_by(Grant.traffic_group)
                    )
                ).all()
            )
            charged = dict(
                (
                    await db.execute(
                        select(Usage.traffic_group, func.sum(Usage.charged_bytes)).group_by(Usage.traffic_group)
                    )
                ).all()
            )
            print(
                f'DUAL SQL {accounts} accounts interval={runtime.interval}: raw={collected - started:.3f}s settlement={time.monotonic() - collected:.3f}s total={elapsed:.3f}s SQL={sql_count[0]} successful={successful} healthy={healthy is not None} metrics={metrics}',
                flush=True,
            )
            assert elapsed < 90 and healthy is not None
            assert runtime._scope_failures == set() and row.error_count == 0
            assert metrics['fast_account_updates'] >= accounts * 0.9
            assert await db.scalar(select(func.count()).select_from(Work).where(Work.error.is_not(None))) == 0
            for group, delta in deltas.items():
                assert remaining[group] == initial[group] - accounts * delta
                assert charged[group] == accounts * delta
                assert (
                    await db.scalar(
                        select(func.count())
                        .select_from(Cursor)
                        .where(Cursor.traffic_group == group, Cursor.processed_bytes == 100 + delta)
                    )
                    == accounts
                )
                assert (
                    await db.scalar(
                        select(func.count())
                        .select_from(Usage)
                        .where(Usage.traffic_group == group, Usage.state == 'charged')
                    )
                    == accounts
                )
                assert (
                    await db.scalar(
                        select(func.count(func.distinct(Usage.subscription_id))).where(Usage.traffic_group == group)
                    )
                    == accounts
                )
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
            assert (await db.get(User, 1, populate_existing=True)).balance_kopeks == funded_balance
            reader._api.get_split_access_snapshot.assert_not_awaited()
            reader._api.update_internal_squads_once.assert_not_awaited()
        except TimeoutError:
            print(
                f'DUAL deadline {accounts} accounts: total={time.monotonic() - started:.3f}s '
                f'SQL={sql_count[0]} metrics={locals().get("metrics")}',
                flush=True,
            )
            raise
        finally:
            await engine.dispose()
