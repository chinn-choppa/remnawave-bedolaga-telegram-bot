"""Persisted reverse scheduling: finite pages, poisoned work and recovery health."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from app.database.models import (
    BsTrafficFleetLane,
    BsTrafficFleetRound,
    BsTrafficLedger,
    BsTrafficOwnershipCycle,
    Subscription,
)
from tests.database.test_bs_fleet_round_postgres import bulk_fleet
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('failure', ['unconfirmed', 'error', 'timeout', 'local_wait'])
async def test_reverse_cursor_survives_poison_restart_and_finite_window(postgres_database, monkeypatch, failure):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, 1)
        ledger.access_policy = dict(
            ledger.access_policy,
            bs_purchase_enabled=False,
            bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        await db.flush()
        await bulk_fleet(db, 4)
        (await db.get(BsTrafficLedger, 12)).panel_scope = str(uuid4())
        # The second item is durable recovery, not currently activated split.
        (await db.get(Subscription, 10)).traffic_accounting_mode = 'split_pending'
        db.add(
            BsTrafficOwnershipCycle(
                subscription_id=10,
                ownership_cycle=0,
                state='reverse_pending',
                forward_receipt={},
                reverse_attempt={'state': 'attempted'},
            )
        )
        # A future drain must remain excluded.
        other = await db.get(BsTrafficLedger, 11)
        other.access_policy = dict(
            other.access_policy, bs_drain_until=(datetime.now(UTC) + timedelta(days=1)).isoformat()
        )
        await db.commit()
        calls = []

        async def advance(bound, **keys):
            calls.append(keys['subscription_id'])
            if keys['subscription_id'] == 1:
                if failure == 'timeout':
                    await asyncio.Event().wait()
                if failure == 'error':
                    raise ValueError('synthetic')
                if failure == 'local_wait':
                    from app.services.bs_traffic.ledger import LedgerConflict

                    raise LedgerConflict('Synthetic grace obligation')
                return 'unconfirmed'
            return 'managed'

        monkeypatch.setattr('app.services.bs_traffic.reverse_delivery.advance_reverse', advance)
        monkeypatch.setattr('app.services.bs_traffic.runtime.REVERSE_STEP_SECONDS', 0.02)
        runtime = runtime_for(reader)
        await runtime._reverse(reader)
        restarted = runtime_for(reader)
        await restarted._reverse(reader)
        await restarted._reverse(reader)
        assert calls == [1, 10]
        assert (reader.panel_scope in runtime._scope_failures) is (failure != 'local_wait')
        lane = await db.get(BsTrafficFleetLane, (reader.panel_scope, 'reverse'))
        assert lane.upper_id == 0 and lane.completed_at is not None and lane.lease_token is None


async def test_reverse_pending_only_current_cycle_blocks_health(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        now = datetime.now(UTC)
        db.add(
            BsTrafficFleetRound(
                panel_scope=reader.panel_scope,
                round_id=str(uuid4()),
                day=now.date(),
                upper_id=0,
                cursor=0,
                phase='complete',
                started_at=now,
                completed_at=now,
                last_success_at=now,
                error_count=0,
            )
        )
        cycle = BsTrafficOwnershipCycle(
            subscription_id=1,
            ownership_cycle=0,
            state='reverse_pending',
            forward_receipt={},
            reverse_attempt={'state': 'attempted'},
        )
        db.add(cycle)
        await db.commit()
        runtime = runtime_for(reader)
        assert await runtime._health(reader) is None
        cycle.state = 'managed'
        await db.commit()
        assert await runtime._health(reader) is not None
        cycle.state = 'reverse_pending'
        (await db.get(BsTrafficLedger, 1)).ownership_cycle = 1
        await db.commit()
        assert await runtime._health(reader) is not None


@pytest.mark.parametrize('lost_response', [False, True])
async def test_reverse_lane_real_transfer_and_replay_never_recharges(postgres_database, monkeypatch, lost_response):
    from dataclasses import replace
    from unittest.mock import AsyncMock

    from sqlalchemy import func, select

    from app.database.models import BsTrafficFleetWork, TrafficAllowance
    from app.services.bs_traffic.cutover_activation import activate_cutover
    from tests.database.test_bs_traffic_cutover_activation_postgres import ready
    from tests.database.test_bs_traffic_purchase_postgres import totals

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, operation = await ready(db, monkeypatch)
        await db.commit()
        assert await activate_cutover(reader, **keys, operation_id=operation) == 'activated'
        ledger = await db.get(BsTrafficLedger, 1, populate_existing=True)
        ledger.access_policy = dict(
            ledger.access_policy,
            bs_purchase_enabled=False,
            bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        now = datetime.now(UTC)
        db.add(
            BsTrafficFleetRound(
                panel_scope=reader.panel_scope,
                round_id=str(uuid4()),
                day=now.date(),
                upper_id=0,
                cursor=0,
                phase='complete',
                started_at=now,
                completed_at=now,
                last_success_at=now,
                error_count=0,
            )
        )
        db.add(
            BsTrafficFleetWork(
                subscription_id=1,
                panel_scope=reader.panel_scope,
                account_generation=ledger.account_generation,
                target_round=str(uuid4()),
                due_at=now,
                access_dirty=True,
                error='old_split_work',
            )
        )
        before = await totals(db)
        await db.commit()
        source = reader._api.get_split_grace_snapshot.return_value
        counters = reader._api.get_split_transfer_snapshot.return_value
        reader._api.get_split_transfer_snapshot.return_value = replace(
            counters, used_bytes=source.used_traffic_bytes, traffic_limit_bytes=0
        )

        async def patch(target):
            reader._api.get_split_grace_snapshot.return_value = target
            reader._api.get_split_transfer_snapshot.return_value = replace(
                counters, used_bytes=target.used_traffic_bytes, traffic_limit_bytes=target.traffic_limit_bytes
            )
            if lost_response:
                raise TimeoutError('Synthetic accepted write with lost response')

        monkeypatch.setattr(
            'app.services.bs_traffic.reverse_delivery.collect_topology_evidence', AsyncMock(return_value=plan.evidence)
        )
        reader._api.return_split_to_managed_once = AsyncMock(side_effect=patch)
        runtime = runtime_for(reader)
        assert await runtime._health(reader) is None
        await runtime._reverse(reader)
        await db.rollback()
        if lost_response:
            assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
            assert await runtime._health(reader) is None
            lane = await db.get(BsTrafficFleetLane, (reader.panel_scope, 'reverse'))
            lane.due_at = datetime.now(UTC) - timedelta(seconds=1)
            await db.commit()
            runtime = runtime_for(reader)
            await runtime._reverse(reader)
            await db.rollback()
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'
        assert await runtime._health(reader) is not None
        lane = await db.get(BsTrafficFleetLane, (reader.panel_scope, 'reverse'))
        lane.due_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        await runtime_for(reader)._reverse(reader)
        reader._api.return_split_to_managed_once.assert_awaited_once()
        await db.rollback()
        assert await totals(db) == before
        assert (
            await db.scalar(
                select(func.count()).select_from(TrafficAllowance).where(TrafficAllowance.retired_at.is_(None))
            )
            == 2
        )
