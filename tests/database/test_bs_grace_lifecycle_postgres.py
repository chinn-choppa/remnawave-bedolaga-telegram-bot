from dataclasses import replace

import pytest
from sqlalchemy import select

from app.database.models import Base, GraceAccessSessionModel, Subscription
from app.services.grace_access_codec import GraceSnapshotError
from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore
from tests.database.test_grace_split_snapshot_postgres import prepared_session
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = list(Base.metadata.sorted_tables)


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_missing_context_never_checkpoints_legacy_grace(postgres_database, mode):
    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        sub = await db.get(Subscription, 1)
        sub.traffic_accounting_mode = mode
        await db.commit()
        with pytest.raises(GraceSnapshotError):
            await SQLAlchemyGraceSessionStore(db).create(replace(source, split_before=None))
        assert await db.scalar(select(GraceAccessSessionModel)) is None


async def ready_lifecycle(db, monkeypatch, *, duration=60):
    from datetime import UTC, datetime, timedelta
    from unittest.mock import AsyncMock

    from app.services.grace_access_service import GraceAccessPolicy
    from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase

    _, reader, keys, _ = await prepared_purchase(db, monkeypatch)
    sub = await db.get(Subscription, 1)
    sub.status = 'expired'
    sub.end_date = datetime.now(UTC) - timedelta(days=1)
    await db.commit()
    current = replace(
        reader._api.get_split_grace_snapshot.return_value,
        status='EXPIRED',
        expire_at=sub.end_date,
        hwid_device_limit=sub.device_limit,
    )
    reader._api.get_split_grace_snapshot.side_effect = None
    reader._api.get_split_grace_snapshot.return_value = current

    async def apply(target):
        reader._api.get_split_grace_snapshot.return_value = target
        return target

    reader._api.restore_split_user_once = AsyncMock(side_effect=apply)
    reader._api.apply_split_grace_overlay_once = AsyncMock(side_effect=apply)
    reader._api.reset_user_traffic = AsyncMock()
    policy = GraceAccessPolicy(
        duration=timedelta(seconds=duration),
        expired_squad_uuid='88888888-8888-4888-8888-888888888888',
        limited_squad_uuid='77777777-7777-4777-8777-777777777777',
        traffic_bytes=1000,
        reset_traffic_on_start=True,
    )
    return reader, keys, policy, current


@pytest.mark.parametrize('exit_reason', ['timeout', 'revoked'])
async def test_actual_expired_issue_v4_no_reset_and_safe_exit(postgres_database, monkeypatch, exit_reason):
    import asyncio

    from app.database.models import BsTrafficGraceIntent, User
    from app.services.bs_traffic.grace_lifecycle import advance_split_grace, start_split_grace
    from app.services.grace_access_service import GraceReason, GraceSessionState

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, before = await ready_lifecycle(
            db, monkeypatch, duration=2 if exit_reason == 'timeout' else 60
        )
        result = await start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        assert result.session.state is GraceSessionState.ACTIVE
        assert result.session.split_before is not None
        row = await db.get(GraceAccessSessionModel, result.session.id)
        assert row.snapshot_version == 4 and row.split_overlay_attempt['state'] == 'verified'
        assert not result.session.overlay.reset_used_traffic
        assert reader._api.get_split_grace_snapshot.return_value.used_traffic_bytes == before.used_traffic_bytes
        if exit_reason == 'timeout':
            await db.rollback()
            await asyncio.sleep(2.1)
        else:
            user = await db.get(User, 1)
            user.status = 'blocked'
            await db.commit()
        assert await advance_split_grace(reader, **keys) == exit_reason
        target = reader._api.get_split_grace_snapshot.return_value
        assert target.status == 'DISABLED' and target.active_internal_squads == []
        assert target.traffic_limit_strategy == 'NO_RESET'
        assert target.used_traffic_bytes == before.used_traffic_bytes
        db.expire_all()
        row = await db.get(GraceAccessSessionModel, result.session.id)
        sub = await db.get(Subscription, 1)
        assert row.state == 'completed' and row.completion_reason == exit_reason
        assert sub.grace_tail_expire_at == target.expire_at
        assert (await db.scalar(select(BsTrafficGraceIntent))).completion_reason == exit_reason
        reader._api.reset_user_traffic.assert_not_awaited()
        assert reader._api.restore_split_user_once.await_count == 1
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()


async def test_ambiguous_overlay_is_get_only_on_restart(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.grace_lifecycle import advance_split_grace, start_split_grace
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, before = await ready_lifecycle(db, monkeypatch)
        reader._api.apply_split_grace_overlay_once = AsyncMock(side_effect=TimeoutError())
        with pytest.raises(TimeoutError):
            await start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        row = await db.scalar(select(GraceAccessSessionModel))
        assert row.split_overlay_attempt['state'] == 'attempted'
        await db.rollback()
        assert await advance_split_grace(reader, **keys) == 'unconfirmed'
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()
        reader._api.restore_split_user_once.assert_not_awaited()
        assert reader._api.get_split_grace_snapshot.return_value == before


@pytest.mark.parametrize('both_empty', [False, True])
async def test_only_bs_exhausted_never_takes_ordinary_rights(postgres_database, monkeypatch, both_empty):
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.grace_lifecycle import start_split_grace
    from app.services.grace_access_service import GraceReason, GraceSessionState

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        sub.status = 'active'
        sub.end_date = datetime.now(UTC) + timedelta(days=10)
        for grant in (await db.scalars(select(BsTrafficAllowance))).all():
            if both_empty:
                grant.remaining_bytes = 0
        await db.commit()
        reader._api.get_split_grace_snapshot.return_value = replace(
            reader._api.get_split_grace_snapshot.return_value,
            status='ACTIVE',
            expire_at=sub.end_date,
        )
        if both_empty:
            result = await start_split_grace(reader, **keys, reason=GraceReason.LIMITED, policy=policy)
            assert result.session.state is GraceSessionState.ACTIVE
        else:
            with pytest.raises(GraceSnapshotError):
                await start_split_grace(reader, **keys, reason=GraceReason.LIMITED, policy=policy)
            assert await db.scalar(select(GraceAccessSessionModel)) is None
            reader._api.restore_split_user_once.assert_not_awaited()
            reader._api.apply_split_grace_overlay_once.assert_not_awaited()


async def test_concurrent_actual_start_issues_only_once(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy import func

    from app.services.bs_traffic.grace_lifecycle import start_split_grace
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        results = await asyncio.gather(
            *[start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy) for _ in range(2)]
        )
        assert results[0].session.id == results[1].session.id
        assert await db.scalar(select(func.count()).select_from(GraceAccessSessionModel)) == 1
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()
        reader._api.restore_split_user_once.assert_not_awaited()


async def test_actual_issued_grace_paid_renewal_restore(postgres_database, monkeypatch):
    from app.config import settings
    from app.database.models import Tariff, User
    from app.services.bs_traffic.grace_lifecycle import advance_split_grace, start_split_grace
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, before = await ready_lifecycle(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.period_prices = {'30': 2000}
        tariff.traffic_reset_mode = 'NO_RESET'
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', True)
        await db.commit()
        started = await start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        receipt = await renew_from_balance(
            db, **keys, period_days=30, expected_price_kopeks=2000, operation_key='actual-grace-renewal'
        )
        await db.commit()
        assert await advance_split_grace(reader, **keys) == 'paid'
        assert await advance_split_grace(reader, **keys) == 'completed'
        row = await db.get(GraceAccessSessionModel, started.session.id)
        assert row.state == 'completed' and row.completion_reason == 'paid'
        assert reader._api.get_split_grace_snapshot.return_value.expire_at == receipt.new_end_date
        assert reader._api.get_split_grace_snapshot.return_value.used_traffic_bytes == before.used_traffic_bytes
        assert (await db.get(User, 1)).balance_kopeks == 8000
        reader._api.reset_user_traffic.assert_not_awaited()
        assert reader._api.restore_split_user_once.await_count == 1
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()


async def test_runtime_entrypoints_choose_split_coordinator(postgres_database, monkeypatch):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from app.services import grace_access_runtime as runtime
    from app.services.bs_traffic import grace_lifecycle
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)

        @asynccontextmanager
        async def bound(subscription_id, **kwargs):
            assert subscription_id == keys['subscription_id']
            yield reader, keys

        monkeypatch.setattr(grace_lifecycle, 'runtime_reader', bound)
        monkeypatch.setattr(runtime, '_build_policy', lambda: policy)
        instance = runtime.GraceAccessRuntime()
        instance._mode = runtime.GraceAccessMode.ACTIVE
        monkeypatch.setattr(instance, '_announce_start', AsyncMock())
        monkeypatch.setattr(instance, '_announce_reconcile', AsyncMock())
        result = await instance.consider_candidate(1, GraceReason.EXPIRED, source='test')
        assert result.session is not None and result.session.split_before is not None
        finished = await instance._process_open(1, drain=True, force_restore=True)
        assert finished.drained == 1
        reader._api.reset_user_traffic.assert_not_awaited()


async def test_verified_overlay_predicate_rejects_unknown_or_foreign(postgres_database, monkeypatch):
    from datetime import UTC, datetime

    from app.services.bs_traffic.grace_lifecycle import confirmed_active_overlay, start_split_grace
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        result = await start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        args = dict(
            subscription_id=1,
            account_generation=keys['account_generation'],
            panel_scope=reader.panel_scope,
            now=datetime.now(UTC),
        )
        reads = reader._api.get_split_grace_snapshot.await_count
        assert await confirmed_active_overlay(db, **args)
        assert not await confirmed_active_overlay(db, **dict(args, account_generation='f' * 64))
        row = await db.get(GraceAccessSessionModel, result.session.id)
        row.split_overlay_attempt = dict(row.split_overlay_attempt, state='attempted')
        await db.flush()
        assert not await confirmed_active_overlay(db, **args)
        assert reader._api.get_split_grace_snapshot.await_count == reads


async def test_0158_upgrade_preserves_existing_grace_and_refuses_history_loss(postgres_database):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0158_bs_grace_lifecycle.py')
    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        saved = await SQLAlchemyGraceSessionStore(db).create(source)
        row = await db.get(GraceAccessSessionModel, saved.id)
        old_snapshot = row.panel_before

        def cycle(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()
                migration.upgrade()

        await (await db.connection()).run_sync(cycle)
        await db.refresh(row)
        assert row.panel_before == old_snapshot and row.split_overlay_attempt is None
        row.split_overlay_attempt = {'state': 'attempted'}
        await db.flush()

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)):
                with pytest.raises(RuntimeError, match='history must be retained'):
                    migration.downgrade()

        await (await db.connection()).run_sync(refuse)


@pytest.mark.parametrize('change', ['generation', 'binding', 'restriction'])
async def test_capture_is_revalidated_before_checkpoint(postgres_database, monkeypatch, change):
    from datetime import UTC, datetime

    from app.database.models import BsTrafficLedger, BsTrafficPanelBinding
    from app.services.bs_traffic import grace_lifecycle
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        original = grace_lifecycle.capture_grace_context

        async def changed(*args, **kwargs):
            result = await original(*args, **kwargs)
            async with reader._factory() as other, other.begin():
                if change == 'generation':
                    (await other.get(BsTrafficLedger, 1)).account_generation = 'e' * 64
                elif change == 'restriction':
                    (await other.get(BsTrafficLedger, 1)).restriction_attempt = {'state': 'attempted'}
                else:
                    (await other.get(BsTrafficPanelBinding, reader.panel_scope)).retired_at = datetime.now(UTC)
            return result

        monkeypatch.setattr(grace_lifecycle, 'capture_grace_context', changed)
        with pytest.raises(LedgerConflict):
            await grace_lifecycle.start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        assert await db.scalar(select(GraceAccessSessionModel)) is None
        reader._api.restore_split_user_once.assert_not_awaited()


async def test_actual_exhausted_capture_pending_usage_never_restores_stale_squads(postgres_database, monkeypatch):
    import asyncio
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficAllowance, BsTrafficUsage
    from app.services.bs_traffic.grace_lifecycle import advance_split_grace, start_split_grace
    from app.services.bs_traffic.ledger import open_usage
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch, duration=2)
        sub = await db.get(Subscription, 1)
        sub.status = 'active'
        sub.end_date = datetime.now(UTC) + timedelta(days=10)
        for grant in (await db.scalars(select(BsTrafficAllowance))).all():
            grant.remaining_bytes = 0
        await db.commit()
        reader._api.get_split_grace_snapshot.return_value = replace(
            reader._api.get_split_grace_snapshot.return_value, status='ACTIVE', expire_at=sub.end_date
        )
        issued = await start_split_grace(reader, **keys, reason=GraceReason.LIMITED, policy=policy)
        assert issued.session.split_before.paid_squads  # Actual stale panel selection is retained as evidence.
        now = datetime.now(UTC)
        usage_id = await open_usage(
            db,
            **keys,
            group='ordinary',
            source_key='grace-raw-uncertain',
            amount_bytes=50,
            interval_start=now - timedelta(minutes=1),
            interval_end=now,
            evidence_kind='raw_pending',
        )
        await db.commit()
        await asyncio.sleep(2.1)
        assert await advance_split_grace(reader, **keys) == 'timeout'
        assert reader._api.get_split_grace_snapshot.return_value.active_internal_squads == []
        db.expire_all()
        usage = await db.get(BsTrafficUsage, usage_id)
        assert usage.state == 'pending' and usage.charged_bytes == 0 and usage.eligible_grants == []


@pytest.mark.parametrize('both_empty', [False, True])
async def test_fleet_marks_and_discovers_only_confirmed_whole_account_exhaustion(
    postgres_database, monkeypatch, both_empty
):
    from datetime import UTC, datetime, timedelta
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficAllowance
    from app.services import grace_access_runtime as grace_runtime
    from app.services.bs_traffic.intake import attach_cursor
    from app.services.grace_access_service import GraceReason
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_B
    from tests.database.test_bs_traffic_runtime_postgres import runtime_for

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        sub.status = 'active'
        sub.end_date = datetime.now(UTC) + timedelta(days=10)
        if both_empty:
            for grant in (await db.scalars(select(BsTrafficAllowance))).all():
                grant.remaining_bytes = 0
        _, day = await observe(db, reader, keys, NODE_B, 100)
        await attach_cursor(db, **keys, group='bs', node_uuid=NODE_B, day=day)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 100}]} for node in nodes]
            }
        )
        await runtime_for(reader).run_once()
        db.expire_all()
        sub = await db.get(Subscription, 1)
        assert (sub.grace_candidate_reason == 'limited') == both_empty
        await db.rollback()
        monkeypatch.setattr(grace_runtime, 'AsyncSessionLocal', reader._factory)
        monkeypatch.setattr(grace_runtime, '_build_policy', lambda: policy)
        candidates = await grace_runtime.GraceAccessRuntime()._recent_candidate_ids()
        assert ((1, GraceReason.LIMITED) in candidates) == both_empty


async def test_confirmed_expiry_restriction_still_allows_configured_grace(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.grace_lifecycle import start_split_grace
    from app.services.bs_traffic.restriction import restrict_account
    from app.services.grace_access_service import GraceReason, GraceSessionState

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)

        async def restrict(target):
            reader._api.get_split_grace_snapshot.return_value = target

        reader._api.restrict_split_user_once = AsyncMock(side_effect=restrict)
        assert await restrict_account(reader, **keys) == 'verified'
        result = await start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        assert result.session.state is GraceSessionState.ACTIVE
        assert result.session.panel_before.status == 'DISABLED'
        assert result.session.panel_before.squad_uuids == ()


@pytest.mark.parametrize('attempted', [False, True])
async def test_payment_between_pending_checkpoint_and_overlay_is_safe(postgres_database, monkeypatch, attempted):
    from unittest.mock import AsyncMock

    from app.config import settings
    from app.database.models import Tariff, User
    from app.services.bs_traffic import grace_lifecycle
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.period_prices = {'30': 2000}
        tariff.traffic_reset_mode = 'NO_RESET'
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', True)
        await db.commit()
        advance = grace_lifecycle.advance_split_grace
        if attempted:
            reader._api.apply_split_grace_overlay_once = AsyncMock(side_effect=TimeoutError())
            with pytest.raises(TimeoutError):
                await grace_lifecycle.start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        else:
            monkeypatch.setattr(grace_lifecycle, 'advance_split_grace', AsyncMock(return_value='unchanged'))
            await grace_lifecycle.start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
            monkeypatch.setattr(grace_lifecycle, 'advance_split_grace', advance)
        if attempted:
            with pytest.raises(LedgerConflict, match='overlay'):
                await renew_from_balance(
                    db, **keys, period_days=30, expected_price_kopeks=2000, operation_key='pending-race'
                )
            await db.rollback()
            assert (await db.get(User, 1)).balance_kopeks == 10000
        else:
            await renew_from_balance(
                db, **keys, period_days=30, expected_price_kopeks=2000, operation_key='pending-race'
            )
            await db.commit()
            assert await advance(reader, **keys) == 'paid'
            row = await db.scalar(select(GraceAccessSessionModel))
            assert row.state == 'completed' and row.completion_reason == 'paid'
            reader._api.apply_split_grace_overlay_once.assert_not_awaited()
            reader._api.restore_split_user_once.assert_awaited_once()


async def test_limited_incident_changes_only_for_started_base_period(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficAllowance, BsTrafficLedger
    from app.services.bs_traffic.grace_lifecycle import split_incident_key
    from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, _, _ = await ready_lifecycle(db, monkeypatch)
        billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(1)
        now = datetime.now(UTC)
        args = dict(billing=billing, reason=GraceReason.LIMITED, last_traffic_reset_at=None, now=now)
        initial = await split_incident_key(db, **args)

        def grant(key, start):
            row = BsTrafficAllowance(
                subscription_id=1,
                traffic_group='ordinary',
                operation_key=key,
                granted_bytes=100,
                remaining_bytes=0,
                starts_at=start,
                expires_at=start + timedelta(days=30),
            )
            db.add(row)
            return row

        grant('purchase:topup', now)
        future = grant('base:renew:future', now + timedelta(days=1))
        grant('service:correction', now)
        await db.flush()
        assert await split_incident_key(db, **args) == initial
        current = grant('base:renew:calendar-current', now)
        await db.flush()
        new = await split_incident_key(db, **args)
        assert new != initial
        current.retired_at = now + timedelta(seconds=1)
        (await db.get(BsTrafficLedger, 1)).revision += 1
        await db.flush()
        assert await split_incident_key(db, **dict(args, billing=replace(billing, used_traffic_bytes=999))) == new
        assert await split_incident_key(db, **dict(args, now=future.starts_at)) != new


async def test_new_calendar_base_permits_new_limited_grace_once(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.base_renewal import reset_renewal_bases
    from app.services.bs_traffic.grace_lifecycle import advance_split_grace, start_split_grace
    from app.services.grace_access_service import GraceReason, GraceStartDecision

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        sub.status = 'active'
        sub.end_date = datetime.now(UTC) + timedelta(days=10)
        end = sub.end_date
        for grant in (await db.scalars(select(BsTrafficAllowance))).all():
            grant.remaining_bytes = 0
        await db.commit()
        reader._api.get_split_grace_snapshot.return_value = replace(
            reader._api.get_split_grace_snapshot.return_value, status='ACTIVE', expire_at=end
        )
        first = await start_split_grace(reader, **keys, reason=GraceReason.LIMITED, policy=policy)
        assert await advance_split_grace(reader, **keys, force_restore=True) == 'drained'
        replay = await start_split_grace(reader, **keys, reason=GraceReason.LIMITED, policy=policy)
        assert replay.decision is GraceStartDecision.ALREADY_GRANTED
        grants = await reset_renewal_bases(
            db,
            **keys,
            operation_key='calendar:funded:next',
            amounts={'ordinary': 100},
            starts_at=datetime.now(UTC),
            expires_at=end,
            reset_base=True,
        )
        (await db.get(BsTrafficAllowance, grants['ordinary'])).remaining_bytes = 0
        await db.commit()
        second = await start_split_grace(reader, **keys, reason=GraceReason.LIMITED, policy=policy)
        assert second.decision is GraceStartDecision.STARTED
        assert first.session.id != second.session.id


async def test_explicit_unsent_overlay_preflight_is_archived_and_retryable(postgres_database, monkeypatch):
    from app.services.bs_traffic.grace_lifecycle import advance_split_grace, start_split_grace
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, policy, _ = await ready_lifecycle(db, monkeypatch)
        calls = 0

        async def changed_source(_):
            nonlocal calls
            calls += 1
            current = reader._api.get_split_grace_snapshot.return_value
            return replace(current, hwid_device_limit=99) if calls == 4 else current

        reader._api.get_split_grace_snapshot.side_effect = changed_source
        first = await start_split_grace(reader, **keys, reason=GraceReason.EXPIRED, policy=policy)
        row = await db.get(GraceAccessSessionModel, first.session.id)
        assert row.split_overlay_attempt['state'] == 'cancelled'
        await db.rollback()
        reader._api.apply_split_grace_overlay_once.assert_not_awaited()
        assert await advance_split_grace(reader, **keys) == 'activated'
        row = await db.get(GraceAccessSessionModel, first.session.id)
        assert row.split_overlay_attempt['history'][0]['state'] == 'cancelled'
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()
