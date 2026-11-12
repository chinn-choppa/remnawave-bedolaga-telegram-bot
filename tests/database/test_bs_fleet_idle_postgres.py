"""Idle fast path never replaces accounting for new bytes or paid changes."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from app.database.models import BsTrafficFleetWork, BsTrafficLedger, BsTrafficObservation
from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('changed', ['none', 'payment', 'raw', 'concurrent_payment'])
@pytest.mark.parametrize('active', [False, True])
async def test_idle_batch_rechecks_paid_revision_and_raw(postgres_database, monkeypatch, changed, active):
    from sqlalchemy import select

    from app.services.bs_traffic.fleet_idle import settle_idle_batch

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        from app.services.bs_traffic.intake import attach_cursor
        from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
        from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
        from tests.database.test_bs_traffic_policy_postgres import NODE_B

        await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id)
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 100}]} for node in nodes],
            }
        )
        await runtime_for(reader).run_once()
        db.expire_all()
        work = await db.get(BsTrafficFleetWork, 1)
        assert work.error is None
        work.due_at = datetime.now(UTC) - timedelta(seconds=1)
        work.completed_round = None
        if active and changed != 'raw':
            from sqlalchemy import update

            await db.execute(update(BsTrafficObservation).values(high_water_bytes=101))
        if changed == 'payment':
            ledger = await db.get(BsTrafficLedger, 1)
            ledger.revision += 1
        elif changed == 'raw':
            raw = await db.scalar(select(BsTrafficObservation))
            raw.high_water_bytes += 1
        await db.commit()
        if changed == 'concurrent_payment':
            from app.services.bs_traffic.purchase import purchase_package
            from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

            ledger = await db.get(BsTrafficLedger, 1)
            evidence = await TopologyEvidenceCache(reader, panel_identity=reader.panel_scope).get(
                reader, panel_identity=reader.panel_scope, ordinary_squads=[ORDINARY], bs_squads=[BS]
            )
            import asyncio

            from sqlalchemy import event

            from app.database.models import User

            await db.execute(select(User).where(User.id == 1).with_for_update())
            waiting = asyncio.Event()

            def observed(conn, cursor, statement, parameters, context, many):
                if 'FOR UPDATE OF users' in statement:
                    waiting.set()

            event.listen(db.bind.sync_engine, 'before_cursor_execute', observed)
            task = asyncio.create_task(
                settle_idle_batch(
                    reader._factory,
                    reader,
                    TopologyEvidenceCache(reader, panel_identity=reader.panel_scope),
                    settle_deltas=active,
                )
            )
            try:
                await asyncio.wait_for(waiting.wait(), 5)
                await purchase_package(
                    db,
                    **target,
                    group='bs',
                    gb=10,
                    expected_price_kopeks=2500,
                    operation_key='concurrent-topup',
                    evidence=evidence,
                    current_squads=(ORDINARY, BS),
                )
                await db.commit()
                result = await asyncio.wait_for(task, 5)
            finally:
                event.remove(db.bind.sync_engine, 'before_cursor_execute', observed)
        else:
            result = await settle_idle_batch(
                reader._factory,
                reader,
                TopologyEvidenceCache(reader, panel_identity=reader.panel_scope),
                settle_deltas=active,
            )
        assert result == (1 if changed == 'none' or (changed == 'raw' and active) else 0)


@pytest.mark.parametrize('confirmed', [True, False])
async def test_verified_grace_is_serviced_without_changing_overlay(postgres_database, monkeypatch, confirmed):
    from app.database.models import GraceAccessSessionModel
    from app.services.bs_traffic.grace_lifecycle import start_split_grace
    from app.services.grace_access_service import GraceReason
    from tests.database.test_bs_grace_lifecycle_postgres import ready_lifecycle

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, policy, _ = await ready_lifecycle(db, monkeypatch)
        result = await start_split_grace(reader, **target, reason=GraceReason.EXPIRED, policy=policy)
        if not confirmed:
            grace = await db.get(GraceAccessSessionModel, result.session.id)
            grace.split_overlay_attempt = dict(grace.split_overlay_attempt, state='attempted')
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 100}]} for node in nodes],
            }
        )
        reader._api.update_internal_squads_once.reset_mock()
        runtime = runtime_for(reader)
        await runtime.run_once()
        db.expire_all()
        work = await db.get(BsTrafficFleetWork, 1)
        assert (work.error is None) == confirmed
        assert (reader.panel_scope in runtime._healthy) == confirmed
        reader._api.update_internal_squads_once.assert_not_awaited()


@pytest.mark.parametrize(
    'case',
    ['basic', 'fifo', 'extension', 'retired', 'exhaustion', 'purchase_boundary', 'grace_overlap', 'time_boundary'],
)
async def test_active_batch_matches_slow_ledger_exactly(postgres_database, monkeypatch, case):
    from sqlalchemy import select, update

    from app.database.models import BsTrafficAllowance, BsTrafficUsage, BsTrafficUsageCursor
    from app.services.bs_traffic.fleet_idle import settle_idle_batch
    from app.services.bs_traffic.fleet_settlement import run_local_once
    from app.services.bs_traffic.intake import attach_cursor
    from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_B

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id)
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 100}]} for node in nodes],
            }
        )
        await runtime_for(reader).run_once()
        from app.database.models import BsTrafficBaseExtension

        grant = await db.scalar(select(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'ordinary'))
        now = datetime.now(UTC)
        if case in ('fifo', 'retired'):
            grant.remaining_bytes = 1
            if case == 'retired':
                grant.retired_at = now - timedelta(hours=1)
            db.add(
                BsTrafficAllowance(
                    subscription_id=1,
                    traffic_group='ordinary',
                    operation_key='synthetic-second',
                    granted_bytes=100,
                    remaining_bytes=100,
                    starts_at=now - timedelta(days=2),
                    expires_at=now + timedelta(days=90),
                )
            )
        elif case == 'extension':
            grant.starts_at, grant.expires_at = now - timedelta(days=2), now - timedelta(hours=3)
            db.add(
                BsTrafficBaseExtension(
                    subscription_id=1,
                    allowance_id=grant.id,
                    traffic_group='ordinary',
                    operation_key='synthetic-extension',
                    request_hash='f' * 64,
                    requested_at=now - timedelta(hours=4),
                    starts_at=now - timedelta(hours=4),
                    expires_at=now + timedelta(days=1),
                )
            )
        elif case == 'exhaustion':
            await db.execute(
                update(BsTrafficAllowance)
                .where(BsTrafficAllowance.traffic_group == 'ordinary')
                .values(remaining_bytes=0)
            )
            grant.remaining_bytes = 1
        elif case == 'grace_overlap':
            from uuid import uuid4

            from app.database.models import GraceAccessSessionModel

            db.add(
                GraceAccessSessionModel(
                    id=str(uuid4()),
                    subscription_id=1,
                    remnawave_id=12,
                    reason='limited',
                    incident_key='synthetic-closed',
                    state='completed',
                    snapshot_version=2,
                    billing_before={},
                    panel_before={},
                    overlay={},
                    started_at=now - timedelta(minutes=2),
                    grace_until=now - timedelta(minutes=1),
                    updated_at=now - timedelta(minutes=1),
                    completed_at=now - timedelta(minutes=1),
                    completion_reason='timeout',
                )
            )
        elif case == 'time_boundary':
            grant.starts_at = now - timedelta(days=1)
            grant.expires_at = now - timedelta(seconds=1)
        elif case == 'purchase_boundary':
            grant.starts_at = now - timedelta(hours=1)
            await db.execute(update(BsTrafficUsageCursor).values(last_poll_started_at=now - timedelta(hours=2)))
        await db.flush()
        # Both executions use the exact same original account state via an
        # explicit transaction rollback between fast and ordinary workers.
        await db.execute(update(BsTrafficObservation).values(high_water_bytes=101))
        work = await db.get(BsTrafficFleetWork, 1)
        work.due_at = datetime.now(UTC) - timedelta(seconds=1)
        work.completed_round = None
        work.settled_at = datetime.now(UTC) - (timedelta(seconds=2) if case == 'time_boundary' else timedelta())
        await db.commit()
        original = list(await db.execute(select(BsTrafficAllowance.id, BsTrafficAllowance.remaining_bytes)))
        revision = (await db.get(BsTrafficLedger, 1)).revision
        cursors = list(await db.execute(select(BsTrafficUsageCursor.node_uuid, BsTrafficUsageCursor.processed_bytes)))
        await db.rollback()
        cache = TopologyEvidenceCache(reader, panel_identity=reader.panel_scope)
        handled = await settle_idle_batch(reader._factory, reader, cache, settle_deltas=True)
        if case in ('exhaustion', 'purchase_boundary', 'grace_overlap', 'time_boundary'):
            assert handled == 0
            assert list(await db.execute(select(BsTrafficAllowance.id, BsTrafficAllowance.remaining_bytes))) == original
            assert await db.scalar(select(BsTrafficUsage.id)) is None
            return
        assert handled == 1

        async def snapshot():
            db.expire_all()
            grants = list(
                await db.execute(
                    select(BsTrafficAllowance.id, BsTrafficAllowance.remaining_bytes).order_by(BsTrafficAllowance.id)
                )
            )
            usage = list(
                await db.execute(
                    select(
                        BsTrafficUsage.traffic_group,
                        BsTrafficUsage.source_key,
                        BsTrafficUsage.request_hash,
                        BsTrafficUsage.account_generation,
                        BsTrafficUsage.evidence_kind,
                        BsTrafficUsage.interval_start,
                        BsTrafficUsage.interval_end,
                        BsTrafficUsage.amount_bytes,
                        BsTrafficUsage.eligible_grants,
                        BsTrafficUsage.allocations,
                        BsTrafficUsage.state,
                        BsTrafficUsage.charged_bytes,
                        BsTrafficUsage.unallocated_bytes,
                    ).order_by(BsTrafficUsage.source_key)
                )
            )
            positions = list(
                await db.execute(
                    select(
                        BsTrafficUsageCursor.node_uuid,
                        BsTrafficUsageCursor.processed_bytes,
                        BsTrafficUsageCursor.last_poll_started_at,
                    ).order_by(BsTrafficUsageCursor.node_uuid)
                )
            )
            return grants, usage, positions, (await db.get(BsTrafficLedger, 1)).revision

        fast = await snapshot()
        from sqlalchemy import delete

        await db.execute(delete(BsTrafficUsage))
        for identifier, remaining in original:
            await db.execute(
                update(BsTrafficAllowance).where(BsTrafficAllowance.id == identifier).values(remaining_bytes=remaining)
            )
        await db.execute(update(BsTrafficLedger).values(revision=revision))
        for node, processed in cursors:
            await db.execute(
                update(BsTrafficUsageCursor)
                .where(BsTrafficUsageCursor.node_uuid == node)
                .values(processed_bytes=processed)
            )
        await db.execute(
            update(BsTrafficFleetWork).values(due_at=datetime.now(UTC) - timedelta(seconds=1), completed_round=None)
        )
        await db.commit()
        assert await run_local_once(reader._factory, reader, cache)
        assert await snapshot() == fast
