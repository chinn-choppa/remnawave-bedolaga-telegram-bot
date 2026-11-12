"""Split balances must survive retries and must not charge future purchases."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficAllowance, BsTrafficUsage, Subscription, User
from app.services.bs_traffic.ledger import (
    LedgerConflict,
    grant,
    initialize,
    open_usage,
    resolve_usage,
    summary,
)
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
NOW = datetime(2026, 11, 4, tzinfo=UTC)
END = NOW + timedelta(days=30)
SCOPE = '11111111-1111-4111-8111-111111111111'
GENERATION = 'a' * 64
TARGET = {'user_id': 1, 'subscription_id': 1, 'account_generation': GENERATION}


async def seed(db):
    db.add_all([User(id=1), User(id=2)])
    await db.flush()
    db.add_all(
        [
            Subscription(
                id=1, user_id=1, remnawave_id=12, remnawave_short_id='synthetic-one', start_date=NOW, end_date=END
            ),
            Subscription(
                id=2, user_id=1, remnawave_id=13, remnawave_short_id='synthetic-two', start_date=NOW, end_date=END
            ),
        ]
    )
    await db.flush()
    await initialize(db, **TARGET, panel_scope=SCOPE, panel_user_id=12)


async def issue(db, key='old', amount=100, group='bs', **changes):
    kwargs = dict(TARGET, group=group, operation_key=key, amount_bytes=amount, starts_at=NOW, expires_at=END)
    kwargs.update(changes)
    return await grant(db, **kwargs)


async def usage(db, key='sample', amount=150, group='bs'):
    return await open_usage(
        db,
        **TARGET,
        group=group,
        source_key=key,
        amount_bytes=amount,
        interval_start=NOW,
        interval_end=NOW + timedelta(seconds=10),
    )


async def resolve(db, usage_id, decision='charge', **changes):
    from unittest.mock import patch

    from app.config import settings

    enabled = changes.pop('allow_write_off', False)
    with patch.object(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', enabled):
        return await resolve_usage(db, **(TARGET | changes), usage_id=usage_id, decision=decision)


async def remaining(db, group='bs'):
    return (await summary(db, **TARGET, now=NOW + timedelta(seconds=20)))[group]['remaining_bytes']


async def test_late_usage_cannot_consume_a_package_purchased_after_its_snapshot(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        pending = await usage(db)
        await issue(db, 'new', 200)
        await resolve(db, pending)
        assert await remaining(db) == 200
        event = await db.get(BsTrafficUsage, pending)
        assert (event.charged_bytes, event.unallocated_bytes) == (100, 50)


async def test_pending_usage_is_visible_without_spending_or_claiming_exhaustion(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        await usage(db)
        result = (await summary(db, **TARGET, now=NOW))['bs']
        assert result['remaining_bytes'] == 100
        assert result['pending_bytes'] == 150 and result['pending_operations'] == 1


async def test_grant_and_usage_replay_do_not_restore_or_double_spend(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        grant_id = await issue(db)
        event_id = await usage(db, amount=60)
        await resolve(db, event_id)
        assert await issue(db) == grant_id
        assert await usage(db, amount=60) == event_id
        await resolve(db, event_id)
        assert await remaining(db) == 40


async def test_other_pool_and_other_subscription_keep_their_balances(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        await issue(db, group='ordinary', amount=300)
        await initialize(db, **(TARGET | {'subscription_id': 2}), panel_scope=SCOPE, panel_user_id=13)
        await issue(db, subscription_id=2, amount=500)
        await resolve(db, await usage(db))
        assert await remaining(db) == 0 and await remaining(db, 'ordinary') == 300
        other = await summary(db, **(TARGET | {'subscription_id': 2}), now=NOW)
        assert other['bs']['remaining_bytes'] == 500


async def test_conflicting_replays_cannot_reinterpret_money_or_usage(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db)
        with pytest.raises(LedgerConflict):
            await issue(db, amount=200)
        with pytest.raises(LedgerConflict):
            await usage(db, amount=200)
        await resolve(db, event_id)
        with pytest.raises(LedgerConflict):
            await resolve(db, event_id, 'write_off', allow_write_off=True)


async def test_ambiguous_write_off_requires_opt_in_and_never_charges_client(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db)
        with pytest.raises(LedgerConflict):
            await resolve(db, event_id, 'write_off')
        await resolve(db, event_id, 'write_off', allow_write_off=True)
        assert await remaining(db) == 100
        event = await db.get(BsTrafficUsage, event_id)
        assert event.state == 'written_off' and event.unallocated_bytes == 150 and event.charged_bytes == 0


@pytest.mark.parametrize('changes', [{'user_id': 2}, {'subscription_id': 2}, {'account_generation': 'b' * 64}])
async def test_wrong_owner_target_or_generation_cannot_settle_usage(postgres_database, changes):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db)
        with pytest.raises(LedgerConflict):
            await resolve(db, event_id, **changes)
        assert await remaining(db) == 100


async def test_fefo_and_late_delivery_keep_original_expiry_eligibility(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db, 'long', 100)
        await issue(db, 'short', 50, expires_at=NOW + timedelta(seconds=15))
        await resolve(db, await usage(db, amount=70))
        rows = {r.operation_key: r.remaining_bytes for r in (await db.scalars(select(BsTrafficAllowance))).all()}
        assert rows == {'long': 80, 'short': 0}


async def test_new_or_expiring_mid_interval_grants_are_not_eligible(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db, 'future', starts_at=NOW + timedelta(seconds=5))
        await issue(db, 'expires', expires_at=NOW + timedelta(seconds=5))
        await resolve(db, await usage(db))
        rows = (await db.scalars(select(BsTrafficAllowance))).all()
        assert all(row.remaining_bytes == 100 for row in rows)


async def test_rollback_removes_grant_and_usage_together(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await db.commit()
        await issue(db)
        await usage(db)
        await db.rollback()
        assert not (await db.scalars(select(BsTrafficAllowance))).all()
        assert not (await db.scalars(select(BsTrafficUsage))).all()


async def test_concurrent_resolution_cannot_double_spend(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db, amount=60)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)

        async def worker():
            async with maker() as conn, conn.begin():
                await barrier.wait()
                await resolve(conn, event_id)

        await asyncio.wait_for(asyncio.gather(worker(), worker()), 10)
        assert await remaining(db) == 40


@pytest.mark.parametrize('amount', [True, -1, 0, 1.5, 2**63])
async def test_invalid_grants_are_rejected_before_writing(postgres_database, amount):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        with pytest.raises(LedgerConflict):
            await issue(db, amount=amount)
        assert not (await db.scalars(select(BsTrafficAllowance))).all()


async def test_concurrent_purchase_and_old_usage_resolution_do_not_reach_new_grant(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db, amount=300)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)

        async def purchase():
            async with maker() as conn, conn.begin():
                await barrier.wait()
                await issue(conn, 'new', 500)

        async def settle():
            async with maker() as conn, conn.begin():
                await barrier.wait()
                await resolve(conn, event_id)

        await asyncio.wait_for(asyncio.gather(purchase(), settle()), 10)
        assert await remaining(db) == 500


async def test_same_source_cannot_be_charged_again_in_other_pool(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        await usage(db)
        with pytest.raises(LedgerConflict):
            await usage(db, group='ordinary')


async def test_raw_daily_history_cannot_be_mistaken_for_classified_consumption(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await open_usage(
            db,
            **TARGET,
            group='bs',
            source_key='raw',
            amount_bytes=50,
            interval_start=NOW,
            interval_end=END,
            evidence_kind='raw_pending',
        )
        assert (await db.get(BsTrafficUsage, event_id)).eligible_grants == []
        with pytest.raises(LedgerConflict):
            await resolve(db, event_id)
        assert await remaining(db) == 100
        await resolve(db, event_id, 'write_off', allow_write_off=True)
        assert await remaining(db) == 100


async def test_account_rebinding_cannot_apply_pending_usage_to_recreated_account(postgres_database):
    from sqlalchemy import update

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db)
        await db.execute(update(Subscription).where(Subscription.id == 1).values(remnawave_id=99))
        with pytest.raises(LedgerConflict):
            await resolve(db, event_id)


async def test_long_period_is_not_capped_at_a_year_and_future_grant_is_hidden(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db, 'long', expires_at=NOW + timedelta(days=10000))
        await issue(db, 'tomorrow', starts_at=NOW + timedelta(days=1))
        assert await remaining(db) == 100


async def test_migration_is_additive_and_preserves_pending_financial_records(postgres_database):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.database.models import (
        BsTrafficBaseExtension,
        BsTrafficCutoverIntent,
        BsTrafficDelivery,
        BsTrafficEnrollment,
        BsTrafficLedger,
        BsTrafficManagedTransfer,
        BsTrafficUsageCursor,
    )
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0143_bs_traffic_ledger.py')
    tables = [
        BsTrafficLedger.__table__,
        BsTrafficAllowance.__table__,
        BsTrafficUsage.__table__,
        BsTrafficUsageCursor.__table__,
    ]
    async with postgres_session(postgres_database, [User.__table__]) as db:
        connection = await db.connection()

        def exercise(conn):
            BsTrafficEnrollment.__table__.drop(conn)
            BsTrafficBaseExtension.__table__.drop(conn)
            BsTrafficCutoverIntent.__table__.drop(conn)
            BsTrafficManagedTransfer.__table__.drop(conn)
            BsTrafficDelivery.__table__.drop(conn)
            for table in reversed(tables):
                table.drop(conn)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
                migration.downgrade()
                _load('0150_bs_reported_usage.py').upgrade()
                _load('0152_bs_cursor_poll_boundary.py').upgrade()
                _load('0153_bs_base_retirement.py').upgrade()
                _load('0160_bs_account_restriction.py').upgrade()
            inspector = sa.inspect(conn)
            for table in tables:
                assert {c['name'] for c in inspector.get_columns(table.name)} == set(table.c.keys())
                assert {c['name'] for c in inspector.get_check_constraints(table.name)} == {
                    c.name for c in table.constraints if isinstance(c, sa.CheckConstraint)
                }

        await connection.run_sync(exercise)
        await connection.run_sync(BsTrafficBaseExtension.__table__.create)
        await connection.run_sync(BsTrafficDelivery.__table__.create)
        await connection.run_sync(BsTrafficManagedTransfer.__table__.create)
        await connection.run_sync(BsTrafficCutoverIntent.__table__.create)
        await connection.run_sync(BsTrafficEnrollment.__table__.create)
        await seed(db)
        await issue(db)
        event_id = await usage(db)

        def preserve(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.downgrade()
                migration.upgrade()

        await connection.run_sync(preserve)
        assert await remaining(db) == 100
        assert (await db.get(BsTrafficUsage, event_id)).state == 'pending'


async def test_operator_database_override_controls_write_off_and_reset_revokes_it(postgres_database, monkeypatch):
    from app.config import settings
    from app.database.models import SystemSetting
    from app.services.system_settings_service import bot_configuration_service as service

    key = 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED'
    monkeypatch.setattr(settings, key, False)
    monkeypatch.setattr(service, '_overrides_raw', dict(service._overrides_raw))
    monkeypatch.setattr(service, '_env_override_keys', set(service._env_override_keys) - {key})
    monkeypatch.setattr(service, '_original_values', {**service._original_values, key: False})
    async with postgres_session(postgres_database, [User.__table__, SystemSetting.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db)
        with pytest.raises(LedgerConflict):
            await resolve_usage(db, **TARGET, usage_id=event_id, decision='write_off')
        await service.set_value(db, key, True)
        await resolve_usage(db, **TARGET, usage_id=event_id, decision='write_off')
        assert await remaining(db) == 100
        await service.reset_value(db, key)
        next_id = await usage(db, key='next')
        with pytest.raises(LedgerConflict):
            await resolve_usage(db, **TARGET, usage_id=next_id, decision='write_off')


async def test_opt_out_while_waiting_for_lock_prevents_write_off(postgres_database, monkeypatch):
    from app.config import settings
    from app.services.bs_traffic import ledger

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        event_id = await usage(db)
        original_lock = ledger._lock
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)

        async def lock_then_observe_opt_out(*args, **kwargs):
            result = await original_lock(*args, **kwargs)
            settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED = False
            return result

        monkeypatch.setattr(ledger, '_lock', lock_then_observe_opt_out)
        with pytest.raises(LedgerConflict):
            await resolve_usage(db, **TARGET, usage_id=event_id, decision='write_off')
        assert (await db.get(BsTrafficUsage, event_id)).state == 'pending'


async def test_settlement_records_resolution_time_without_changing_source_interval(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        event_id = await usage(db, amount=50)
        row = await db.get(BsTrafficUsage, event_id)
        assert row.created_at is not None and row.resolved_at is None
        await resolve(db, event_id)
        assert row.resolved_at >= row.created_at
        first_resolution = row.resolved_at
        await resolve(db, event_id)
        assert row.resolved_at == first_resolution
        assert row.interval_start == NOW


async def test_new_grant_is_not_encumbered_by_frozen_old_usage(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db, amount=100)
        await usage(db, amount=150)
        await issue(db, key='new', amount=200)
        result = await summary(db, **TARGET, now=NOW)
        assert result['bs']['remaining_bytes'] == 300
        assert result['bs']['unencumbered_bytes'] == 200
        assert result['bs']['pending_operations'] == 1
        await usage(db, key='later', amount=1)
        assert (await summary(db, **TARGET, now=NOW))['bs']['unencumbered_bytes'] == 0


@pytest.mark.parametrize('kind', ['raw', 'review', 'expired', 'future'])
async def test_unencumbered_balance_requires_current_unambiguous_entitlement(postgres_database, kind):
    from app.database.models import BsTrafficUsageCursor

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        if kind == 'raw':
            await open_usage(
                db,
                **TARGET,
                group='bs',
                source_key='raw',
                amount_bytes=1,
                interval_start=NOW,
                interval_end=NOW + timedelta(seconds=1),
                evidence_kind='raw_pending',
            )
        elif kind == 'review':
            db.add(
                BsTrafficUsageCursor(
                    subscription_id=1,
                    account_generation=TARGET['account_generation'],
                    node_uuid='22222222-2222-4222-8222-222222222222',
                    day=NOW.date(),
                    traffic_group='bs',
                    baseline_bytes=0,
                    processed_bytes=0,
                    needs_review=True,
                )
            )
        start = NOW + timedelta(days=1) if kind == 'future' else NOW - timedelta(days=2)
        end = NOW - timedelta(days=1) if kind == 'expired' else END
        await issue(db, key='new', amount=200, starts_at=start, expires_at=end)
        await issue(db, key='ordinary', group='ordinary', amount=50)
        result = await summary(db, **TARGET, now=NOW)
        assert result['bs']['unencumbered_bytes'] == 0
        assert result['ordinary']['unencumbered_bytes'] == 50


@pytest.mark.parametrize('limit_on', ['grants', 'pending'])
async def test_large_pending_history_defers_extra_access_proof(postgres_database, limit_on):
    from app.services.bs_traffic.ledger import UNENCUMBERED_SCAN_LIMIT

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        for index in range(UNENCUMBERED_SCAN_LIMIT + 1 if limit_on == 'pending' else 1):
            await usage(db, key=f'old-{index}', amount=1)
        for index in range(UNENCUMBERED_SCAN_LIMIT + 1 if limit_on == 'grants' else 1):
            await issue(db, key=f'new-{index}', amount=200)
        result = await summary(db, **TARGET, now=NOW)
        assert result['bs']['remaining_bytes'] > 0
        assert result['bs']['unencumbered_bytes'] == 0
        assert result['bs']['pending_operations'] > 0
