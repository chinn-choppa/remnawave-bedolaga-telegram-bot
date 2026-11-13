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
    [
        'basic',
        'fifo',
        'extension',
        'retired',
        'exhaustion',
        'purchase_boundary',
        'grace_overlap',
        'time_boundary',
        'mode_boundary',
        'mode_tail',
    ],
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
        elif case == 'mode_boundary':
            ledger = await db.get(BsTrafficLedger, 1)
            ledger.access_policy = dict(
                ledger.access_policy, finite_since={'ordinary': now.isoformat(), 'bs': now.isoformat()}
            )
        elif case == 'mode_tail':
            from app.database.models import BsTrafficPoll

            ledger = await db.get(BsTrafficLedger, 1)
            cutoff = now - timedelta(milliseconds=1)
            ledger.access_policy = dict(
                ledger.access_policy, finite_since={'ordinary': cutoff.isoformat(), 'bs': cutoff.isoformat()}
            )
            await db.execute(
                update(BsTrafficUsageCursor).values(last_poll_started_at=cutoff - timedelta(milliseconds=1))
            )
            await db.execute(update(BsTrafficPoll).values(created_at=now))
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
        if case in ('exhaustion', 'purchase_boundary', 'grace_overlap', 'time_boundary', 'mode_boundary', 'mode_tail'):
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


@pytest.mark.parametrize(
    'fault',
    [
        'none',
        'member',
        'generation',
        'unfinished',
        'unproven',
        'stale',
        'cycle',
        'opt_out',
        'raw',
        'hash',
        'racing_raw',
        'empty_nodes',
    ],
)
async def test_idle_batch_covers_only_proven_sparse_accounts(postgres_database, monkeypatch, fault):
    from sqlalchemy import delete, select

    from app.config import settings
    from app.database.models import (
        BsTrafficCollectionJob as Job,
        BsTrafficFleetMember as Member,
        BsTrafficFleetRound as Round,
        BsTrafficUsage as Usage,
        BsTrafficUsageCursor as Cursor,
    )
    from app.services.bs_traffic.fleet_idle import settle_idle_batch
    from tests.database.test_bs_omitted_baseline_postgres import sparse
    from tests.database.test_bs_traffic_policy_postgres import BS, NODE_B, ORDINARY

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, purchase, _ = await purchased(db, monkeypatch)
        from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery

        await advance_purchase_delivery(reader, **keys, operation_id=purchase.delivery_id)
        now = datetime.now(UTC)
        ledger = await db.get(BsTrafficLedger, 1)
        policy = dict(ledger.access_policy, ordinary_mode='disabled')
        ledger.access_policy = policy
        await db.execute(delete(BsTrafficObservation))
        await db.execute(delete(Cursor))
        poll = await sparse(db, reader, keys, now)
        cursor = Cursor(
            subscription_id=1,
            node_uuid=NODE_B,
            day=now.date(),
            account_generation=keys['account_generation'],
            ownership_cycle=ledger.ownership_cycle,
            traffic_group='bs',
            baseline_bytes=0,
            processed_bytes=0,
            needs_review=False,
            last_poll_started_at=None,
            bootstrap_evidence=dict(
                version=1,
                kind='omitted',
                scope=reader.panel_scope,
                panel_id=12,
                generation=keys['account_generation'],
                cycle=ledger.ownership_cycle,
                node=NODE_B,
                day=now.date().isoformat(),
                poll_id=poll.id,
                response_hash=poll.response_hash,
                source_cutover_id=1,
                source_hash='a' * 64,
                first_observed_day=None,
                first_observed_poll_id=None,
            ),
        )
        db.add(cursor)
        round_row = await db.get(Round, reader.panel_scope)
        db.add(
            BsTrafficFleetWork(
                subscription_id=1,
                panel_scope=reader.panel_scope,
                account_generation=ledger.account_generation,
                target_round=round_row.round_id,
                due_at=now - timedelta(seconds=1),
                applied_squads=[ORDINARY, BS],
                settled_revision=ledger.revision,
                settled_at=now,
                access_dirty=False,
                calendar_receipt_id=0,
                calendar_due_at=now + timedelta(days=1),
            )
        )
        if fault == 'member':
            await db.execute(delete(Member))
        elif fault == 'generation':
            (await db.scalar(select(Member))).account_generation = 'f' * 64
        elif fault == 'unfinished':
            (await db.scalar(select(Job).where(Job.poll_id == poll.id))).state = 'pending'
        elif fault == 'unproven':
            poll.response_nodes = None
        elif fault == 'empty_nodes':
            poll.response_nodes = []
        elif fault == 'stale':
            poll.created_at = now - timedelta(minutes=1)
            # Isolate freshness expiry, rather than a sample preceding its round.
            round_row.started_at = now - timedelta(minutes=2)
        elif fault == 'raw':
            from tests.database.test_bs_traffic_cutover_baseline_postgres import observe

            await observe(db, reader, keys, NODE_B, 100)
        elif fault == 'hash':
            cursor.bootstrap_evidence = dict(cursor.bootstrap_evidence, response_hash='f' * 64)
        elif fault == 'cycle':
            cursor.ownership_cycle += 1
        elif fault == 'opt_out':
            monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
        await db.commit()
        if fault == 'racing_raw':
            from app.services.bs_traffic import omitted_baseline
            from tests.database.test_bs_traffic_cutover_baseline_postgres import observe

            original = omitted_baseline.plan_omitted_batch

            async def publish_after_cursor_lock(*args, **kwargs):
                plan = await original(*args, **kwargs)
                async with reader._factory() as collector, collector.begin():
                    await observe(collector, reader, keys, NODE_B, 100)
                return plan

            monkeypatch.setattr(omitted_baseline, 'plan_omitted_batch', publish_after_cursor_lock)
        result = await settle_idle_batch(
            reader._factory,
            reader,
            TopologyEvidenceCache(reader, panel_identity=reader.panel_scope),
            settle_deltas=True,
        )
        assert result == (1 if fault in ('none', 'empty_nodes') else 0)
        assert (await db.scalar(select(BsTrafficObservation)) is None) == (fault not in ('raw', 'racing_raw'))
        assert await db.scalar(select(Usage)) is None
        db.expire_all()
        cursor = await db.get(Cursor, (1, NODE_B, now.date()))
        assert cursor.processed_bytes == 0 and cursor.last_poll_started_at is None
        assert cursor.bootstrap_evidence['first_observed_poll_id'] is None

        if fault == 'stale':
            from app.services.bs_traffic.fleet_settlement import run_local_once

            # A declined fast page is not success. Runtime's ordinary worker
            # records the failed attempt and schedules a retry, without using
            # stale omission as evidence of zero traffic or writing access.
            await db.rollback()
            reader._api.get_split_access_snapshot.reset_mock()
            assert await run_local_once(
                reader._factory, reader, TopologyEvidenceCache(reader, panel_identity=reader.panel_scope)
            )
            work = await db.get(BsTrafficFleetWork, 1, populate_existing=True)
            assert work.error == 'settlement_deferred'
            assert work.completed_round == work.target_round
            assert work.claim_token is None and work.lease_until is None
            assert work.due_at > now
            assert await db.scalar(select(Usage)) is None
            assert await db.scalar(select(BsTrafficObservation)) is None
            reader._api.get_split_access_snapshot.assert_not_awaited()

        if fault == 'none':
            from sqlalchemy import update

            from app.services.bs_traffic.intake import settle_reported_observation
            from tests.database.test_bs_traffic_cutover_baseline_postgres import observe

            await db.execute(update(BsTrafficFleetWork).values(due_at=datetime.now(UTC) - timedelta(seconds=1)))
            _, day = await observe(db, reader, keys, NODE_B, 100)
            await db.commit()
            # The first real sample never becomes chargeable through the sparse
            # shortcut; normal intake writes off only this ambiguous boundary.
            assert (
                await settle_idle_batch(
                    reader._factory,
                    reader,
                    TopologyEvidenceCache(reader, panel_identity=reader.panel_scope),
                    settle_deltas=True,
                )
                == 0
            )
            event_id = await settle_reported_observation(db, **keys, node_uuid=NODE_B, day=day)
            event = await db.get(Usage, event_id)
            assert event.evidence_kind == 'raw_pending' and event.charged_bytes == 0
            ledger = await db.get(BsTrafficLedger, 1)
            work = await db.get(BsTrafficFleetWork, 1)
            work.settled_revision = ledger.revision
            await observe(db, reader, keys, NODE_B, 140)
            await db.commit()
            assert (
                await settle_idle_batch(
                    reader._factory,
                    reader,
                    TopologyEvidenceCache(reader, panel_identity=reader.panel_scope),
                    settle_deltas=True,
                )
                == 1
            )
            charged = await db.scalar(select(Usage).where(Usage.evidence_kind == 'reported'))
            assert charged.charged_bytes == 40
