"""Real PostgreSQL regression for cache-hit evidence expiring inside a batch."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import BsTrafficFleetWork, BsTrafficObservation, BsTrafficUsage, BsTrafficUsageCursor
from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
from app.services.bs_traffic.fleet_idle import settle_idle_batch
from app.services.bs_traffic.intake import attach_cursor
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import BS, NODE_B, ORDINARY
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_topology_cache_hit_expiring_inside_batch_defers_then_retry_settles(postgres_database, monkeypatch):
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
        db.expire_all()
        work = await db.get(BsTrafficFleetWork, 1)
        assert work.error is None
        work.due_at = datetime.now(UTC) - timedelta(seconds=1)
        work.completed_round = None
        await db.execute(update(BsTrafficObservation).values(high_water_bytes=101))
        await db.commit()
        before_usage = await db.scalar(select(func.count()).select_from(BsTrafficUsage))
        before_cursors = list(
            (await db.execute(select(BsTrafficUsageCursor.node_uuid, BsTrafficUsageCursor.processed_bytes))).all()
        )
        await db.rollback()

        # Only clocks are controlled: all work, cursor, grant, and usage SQL is real PG.
        clock = [datetime.now(UTC)]
        cache = TopologyEvidenceCache(reader, panel_identity=reader.panel_scope, clock=lambda: clock[0])
        arguments = dict(panel_identity=reader.panel_scope, ordinary_squads=[ORDINARY], bs_squads=[BS])
        proof = await cache.get(reader, **arguments)
        assert proof.valid_until - proof.observed_at == timedelta(seconds=30)
        clock[0] = proof.valid_until - timedelta(microseconds=1)
        assert await cache.get(reader, **arguments) is proof
        assert proof.is_fresh(clock[0], panel_identity=reader.panel_scope)
        sql_now = proof.valid_until
        original_scalar = AsyncSession.scalar

        async def scalar(session, statement, *args, **kwargs):
            if str(statement).startswith('SELECT clock_timestamp()'):
                return sql_now
            return await original_scalar(session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, 'scalar', scalar)
        assert not proof.is_fresh(sql_now, panel_identity=reader.panel_scope)
        assert await settle_idle_batch(reader._factory, reader, cache, settle_deltas=True) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsage)) == before_usage
        assert (
            list((await db.execute(select(BsTrafficUsageCursor.node_uuid, BsTrafficUsageCursor.processed_bytes))).all())
            == before_cursors
        )
        work = await db.get(BsTrafficFleetWork, 1, populate_existing=True)
        assert work.completed_round is None and work.error is None
        await db.rollback()

        # The next ordinary cache lookup renews evidence; no new usage sample is needed.
        clock[0] = sql_now
        refreshed = await cache.get(reader, **arguments)
        assert refreshed is not proof and refreshed.is_fresh(sql_now, panel_identity=reader.panel_scope)
        assert await settle_idle_batch(reader._factory, reader, cache, settle_deltas=True) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsage)) > before_usage

        # The benchmark must follow the runtime lane when the first fast page
        # loses freshness during SQL. Its fallback refreshes the same cache;
        # an attempted/deferred work row must never count as completed progress.
        from tests.database.test_bs_fleet_round_postgres import settle_runtime_fleet

        await db.execute(
            update(BsTrafficFleetWork).values(completed_round=None, due_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await db.execute(update(BsTrafficObservation).values(high_water_bytes=102))
        await db.commit()
        clock[0] = refreshed.valid_until - timedelta(microseconds=1)
        sql_now = refreshed.valid_until
        get = cache.get

        async def expire_after_cache_hit(*args, **kwargs):
            evidence = await get(*args, **kwargs)
            clock[0] = sql_now
            return evidence

        monkeypatch.setattr(cache, 'get', expire_after_cache_hit)
        result = await settle_runtime_fleet(runtime_for(reader), reader, cache, accounts=1, timeout_seconds=10)
        assert result['completed'] == 1
        assert result['passes'] >= 1
        assert result['zero_batches'] >= 1
        assert result['fallback_calls'] >= 4
        assert result['fallback_attempts'] >= 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsage)) == before_usage + 4
        work = await db.get(BsTrafficFleetWork, 1, populate_existing=True)
        assert work.error is None and work.completed_round == work.target_round
        assert work.lease_until is None and work.claim_token is None
        assert (
            await db.scalar(
                select(BsTrafficUsageCursor.processed_bytes).where(BsTrafficUsageCursor.node_uuid == NODE_B)
            )
            == 102
        )

        # Completed_round alone is not success: the normal worker also stores
        # it on deferred attempts. An errored row must hit the bounded deadline.
        work.error = 'settlement_deferred'
        work.due_at = datetime.now(UTC) + timedelta(hours=1)
        await db.commit()
        with pytest.raises(pytest.fail.Exception, match=r"deadline exceeded.*'completed': 0"):
            await settle_runtime_fleet(runtime_for(reader), reader, cache, accounts=1, timeout_seconds=0.1)
