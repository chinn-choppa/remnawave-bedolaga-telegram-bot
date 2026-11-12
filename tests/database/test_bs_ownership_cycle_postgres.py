from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficLedger, BsTrafficManagedTransfer, BsTrafficOwnershipCycle
from app.services.bs_traffic.ownership_cycle import archive_forward, current_transfer
from tests.database.test_bs_traffic_managed_commit_postgres import ready
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_forward_archive_is_exact_and_current_pointer_is_cycle_bound(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        from app.services.bs_traffic.managed_transfer_commit import commit_managed_transfer

        await commit_managed_transfer(db, **keys)
        receipt = await db.get(BsTrafficManagedTransfer, 1)
        ledger = await db.get(BsTrafficLedger, 1)
        assert receipt is not None
        archived = await archive_forward(db, ledger)
        assert archived.forward_receipt['manifest_hash'] == receipt.manifest_hash
        assert archived.forward_receipt['grants'] == receipt.grants
        assert await current_transfer(db, 1) is receipt
        ledger.ownership_cycle += 1
        await db.flush()
        assert await current_transfer(db, 1) is None
        assert (await db.get(BsTrafficOwnershipCycle, (1, 0))).forward_receipt['operation_key'] == receipt.operation_key


async def test_reverse_inspection_preserves_spent_bytes_and_rejects_live_bs(postgres_database, monkeypatch):
    from datetime import timedelta

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.cutover_activation import activate_cutover
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.reverse_transfer import inspect_reverse
    from tests.database.test_bs_traffic_cutover_activation_postgres import ready as activated_ready

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await activated_ready(db, monkeypatch)
        await db.commit()
        assert await activate_cutover(reader, **keys, operation_id=op) == 'activated'
        ledger = await db.get(BsTrafficLedger, 1, populate_existing=True)
        ledger.access_policy = dict(
            ledger.access_policy,
            bs_purchase_enabled=False,
            bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        grant = await db.scalar(select(BsTrafficAllowance).order_by(BsTrafficAllowance.id))
        grant.remaining_bytes -= 20
        await db.flush()
        manifest = await inspect_reverse(db, reader, **keys)
        assert sum(g['remaining_bytes'] for g in manifest['grants']) == 780
        assert grant.granted_bytes != grant.remaining_bytes
        ledger.access_policy = dict(
            ledger.access_policy, bs_drain_until=(datetime.now(UTC) + timedelta(days=1)).isoformat()
        )
        await db.flush()
        with pytest.raises(LedgerConflict, match='BS rights'):
            await inspect_reverse(db, reader, **keys)


@pytest.mark.parametrize(
    'scenario,late_readback',
    [('live', False), ('live', True), ('expired', False), ('unlimited', False), ('concurrent', False)],
)
async def test_reverse_no_reset_imports_once_retains_full_history(
    postgres_database, monkeypatch, scenario, late_readback
):
    from dataclasses import replace
    from datetime import timedelta
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficAllowance, Subscription, TrafficAccountingState, TrafficAllowance
    from app.services.bs_traffic.cutover_activation import activate_cutover
    from app.services.bs_traffic.reverse_delivery import advance_reverse
    from tests.database.test_bs_traffic_cutover_activation_postgres import ready as activated_ready

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await activated_ready(db, monkeypatch)
        await db.commit()
        assert await activate_cutover(reader, **keys, operation_id=op) == 'activated'
        ledger = await db.get(BsTrafficLedger, 1, populate_existing=True)
        ledger.access_policy = dict(
            ledger.access_policy,
            bs_purchase_enabled=False,
            bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        if scenario == 'expired':
            (await db.get(Subscription, 1)).end_date = datetime.now(UTC) - timedelta(days=1)
        if scenario == 'unlimited':
            ledger.access_policy = dict(ledger.access_policy, ordinary_mode='unlimited')
        if late_readback:
            allowance = await db.scalar(select(BsTrafficAllowance).order_by(BsTrafficAllowance.id))
            allowance.expires_at = datetime.now(UTC) + timedelta(seconds=2)
        await db.commit()
        current = reader._api.get_split_grace_snapshot.return_value
        old_counters = reader._api.get_split_transfer_snapshot.return_value
        reader._api.get_split_transfer_snapshot.return_value = replace(
            old_counters, used_bytes=current.used_traffic_bytes, traffic_limit_bytes=0
        )

        async def patch(target):
            reader._api.get_split_grace_snapshot.return_value = target
            reader._api.get_split_transfer_snapshot.return_value = replace(
                old_counters, used_bytes=target.used_traffic_bytes, traffic_limit_bytes=target.traffic_limit_bytes
            )

            if late_readback:
                raise TimeoutError('Synthetic accepted write with lost response')

        import app.services.bs_traffic.reverse_delivery as delivery

        monkeypatch.setattr(delivery, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
        reader._api.return_split_to_managed_once = AsyncMock(side_effect=patch)
        if late_readback:
            import asyncio

            with pytest.raises(TimeoutError):
                await advance_reverse(reader, **keys)
            await asyncio.sleep(2.1)
        if scenario == 'concurrent':
            import asyncio

            outcomes = await asyncio.gather(
                advance_reverse(reader, **keys), advance_reverse(reader, **keys), return_exceptions=True
            )
            assert 'managed' in outcomes
        assert await advance_reverse(reader, **keys) == 'managed'
        assert await advance_reverse(reader, **keys) == 'managed'
        assert reader._api.return_split_to_managed_once.await_count == 1
        await db.rollback()
        rows = (await db.scalars(select(TrafficAllowance).where(TrafficAllowance.retired_at.is_(None)))).all()
        assert sum(row.remaining_bytes for row in rows) == 800
        assert len(rows) == 2
        assert all('reverse:0:' in row.operation_key for row in rows)
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'
        state = await db.get(TrafficAccountingState, 1)
        assert state.used_bytes == current.used_traffic_bytes
        assert state.phase == ('unlimited' if scenario == 'unlimited' else 'ready')
        if scenario == 'expired':
            assert reader._api.get_split_grace_snapshot.return_value.status == 'DISABLED'
            assert not reader._api.get_split_grace_snapshot.return_value.active_internal_squads
            assert (await db.get(Subscription, 1)).grace_tail_expire_at is not None
        if late_readback:
            assert state.projected_revision < state.revision
        assert all(
            row.retired_at
            for row in (await db.scalars(select(BsTrafficAllowance).execution_options(populate_existing=True))).all()
        )
        cycle = await db.get(BsTrafficOwnershipCycle, (1, 0))
        assert cycle.forward_receipt['grants'] and cycle.reverse_attempt['state'] == 'verified'
        if not late_readback and scenario == 'live':
            from app.database.models import BsTrafficUsage, BsTrafficUsageCursor
            from app.services.bs_traffic.intake import attach_cursor, settle_reported_observation
            from app.services.bs_traffic.managed_transfer_capture import capture_managed_transfer
            from app.services.bs_traffic.managed_transfer_commit import commit_managed_transfer
            from app.services.bs_traffic.ownership_cycle import begin_next_cycle
            from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
            from tests.database.test_bs_traffic_policy_postgres import NODE_A

            old_revision = (await db.get(BsTrafficLedger, 1)).revision
            from app.services.bs_traffic.ledger import LedgerConflict

            state.pending_sync_fields = ['expire_at']
            state.revision += 1
            await db.flush()
            with pytest.raises(LedgerConflict):
                await begin_next_cycle(db, **keys)
            assert await begin_next_cycle(db, **keys, allow_funded_pending_sync=True) == 1
            state.pending_sync_fields = []
            state.projected_revision = state.revision
            await db.commit()
            # A late observation still belongs to the archived cursor/cycle.
            _, day = await observe(db, reader, keys, amount=130)
            old_event = await settle_reported_observation(db, **keys, node_uuid=NODE_A, day=day)
            old_usage = await db.get(BsTrafficUsage, old_event)
            assert old_usage.ownership_cycle == 0 and old_usage.evidence_kind == 'raw_pending'
            assert old_usage.charged_bytes == 0 and old_usage.state == 'written_off'
            await db.commit()
            observation = await capture_managed_transfer(reader, **keys, destination_group='ordinary')
            second = dict(keys, operation_key='second-cycle-funded-transfer', observation=observation)
            await commit_managed_transfer(db, **second)
            await db.commit()
            await commit_managed_transfer(db, **second)
            await db.commit()
            current_receipt = await current_transfer(db, 1)
            assert current_receipt.ownership_cycle == 1
            assert current_receipt.imported_bytes == 800
            assert (await db.get(BsTrafficOwnershipCycle, (1, 0))).forward_receipt[
                'operation_key'
            ] != current_receipt.operation_key
            assert (await db.get(BsTrafficLedger, 1)).revision > old_revision
            new_grants = (
                await db.scalars(select(BsTrafficAllowance).where(BsTrafficAllowance.ownership_cycle == 1))
            ).all()
            assert len(new_grants) == 2 and sum(row.remaining_bytes for row in new_grants) == 800
            await archive_forward(db, await db.get(BsTrafficLedger, 1))
            assert len((await db.scalars(select(BsTrafficOwnershipCycle))).all()) == 2
            # New explicit baseline retains the old one in cycle 0 history.
            _, day = await observe(db, reader, keys, amount=130)
            await attach_cursor(db, **keys, group='ordinary', node_uuid=NODE_A, day=day)
            cursor = await db.get(BsTrafficUsageCursor, (1, NODE_A, day))
            assert cursor.ownership_cycle == 1
            await db.commit()
            from app.services.bs_traffic.cutover_intent import stage_cutover
            from app.services.bs_traffic.cutover_sender import send_cutover
            from app.services.bs_traffic.managed_cutover import prepare_managed_cutover

            next_plan = await prepare_managed_cutover(reader, **keys)
            next_op = await stage_cutover(db, reader, **keys, operation_key='second-cycle-cutover', plan=next_plan)
            await db.commit()
            reader._api.restore_split_user_once = AsyncMock(side_effect=patch)
            assert await send_cutover(reader, **keys, operation_id=next_op) == 'verified'
            assert await activate_cutover(reader, **keys, operation_id=next_op) == 'activated'
            await observe(db, reader, keys, amount=160)
            event = await settle_reported_observation(db, **keys, node_uuid=NODE_A, day=day)
            usage = await db.get(BsTrafficUsage, event)
            assert usage.evidence_kind == 'reported' and usage.charged_bytes == 30
            await db.flush()
            remaining = await db.scalar(
                select(func.sum(BsTrafficAllowance.remaining_bytes)).where(BsTrafficAllowance.ownership_cycle == 1)
            )
            assert remaining == 770


@pytest.mark.parametrize('change', ['owner', 'generation', 'grace', 'usage', 'restriction', 'active_bs', 'binding'])
async def test_reverse_gates_preserve_split_ownership(postgres_database, monkeypatch, change):
    from datetime import timedelta

    from app.database.models import BsTrafficPanelBinding, Subscription
    from app.services.bs_traffic.cutover_activation import activate_cutover
    from app.services.bs_traffic.ledger import LedgerConflict, open_usage
    from app.services.bs_traffic.reverse_transfer import inspect_reverse
    from tests.database.test_bs_traffic_cutover_activation_postgres import ready as activated_ready

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await activated_ready(db, monkeypatch)
        await db.commit()
        assert await activate_cutover(reader, **keys, operation_id=op) == 'activated'
        ledger = await db.get(BsTrafficLedger, 1, populate_existing=True)
        ledger.access_policy = dict(
            ledger.access_policy,
            bs_purchase_enabled=False,
            bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        sub = await db.get(Subscription, 1, populate_existing=True)
        if change == 'owner':
            keys = dict(keys, user_id=2)
        elif change == 'generation':
            keys = dict(keys, account_generation='f' * 64)
        elif change == 'grace':
            sub.grace_session_open = True
        elif change == 'restriction':
            ledger.restriction_attempt = {'state': 'attempted'}
        elif change == 'active_bs':
            ledger.access_policy = dict(
                ledger.access_policy, bs_drain_until=(datetime.now(UTC) + timedelta(days=1)).isoformat()
            )
        elif change == 'binding':
            (await db.get(BsTrafficPanelBinding, reader.panel_scope)).retired_at = datetime.now(UTC)
        else:
            await open_usage(
                db,
                **keys,
                group='ordinary',
                source_key='unresolved-old-cycle',
                amount_bytes=2,
                interval_start=datetime.now(UTC) - timedelta(hours=1),
                interval_end=datetime.now(UTC),
                evidence_kind='raw_pending',
            )
        await db.flush()
        with pytest.raises(LedgerConflict):
            await inspect_reverse(db, reader, **keys)
        assert sub.traffic_accounting_mode == 'split'
        assert await db.get(BsTrafficOwnershipCycle, (1, 0)) is None
