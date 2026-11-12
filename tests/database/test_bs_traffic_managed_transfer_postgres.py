from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficPanelBinding,
    Subscription,
    TrafficAccountingState,
    TrafficAllowance,
    User,
)
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_transfer import inspect_managed_transfer
from app.services.bs_traffic.panel_binding import register_binding
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, TARGET
from tests.database.test_bs_traffic_policy_postgres import seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [User.__table__, BsTrafficPanelBinding.__table__]


async def setup(db):
    await seed(db)
    await register_binding(db, panel_scope=SCOPE, endpoint='https://panel.example')
    sub = await db.get(Subscription, 1)
    sub.traffic_accounting_mode = 'managed'
    state = TrafficAccountingState(
        subscription_id=1,
        panel_identity='synthetic-account',
        lifetime_bytes=1000,
        used_bytes=200,
        phase='ready',
        revision=3,
        projected_revision=3,
        business_strategy='MONTH',
        calendar_timezone='UTC',
        calendar_anchor=datetime.now(UTC),
    )
    db.add(state)
    await db.flush()
    expiry = datetime.now(UTC) + timedelta(days=30)
    for key, original, remaining, until in [
        ('base:paid', 500, 100, expiry),
        ('purchase:synthetic', 900, 700, expiry),
        ('expired', 100, 80, datetime.now(UTC) - timedelta(days=1)),
        ('spent', 100, 0, expiry),
    ]:
        db.add(
            TrafficAllowance(
                subscription_id=1,
                operation_key=key,
                granted_bytes=original,
                remaining_bytes=remaining,
                expires_at=until,
            )
        )
    await db.flush()
    return state


async def inspect(db, **changes):
    return await inspect_managed_transfer(db, **(dict(TARGET, destination_group='ordinary') | changes))


async def test_manifest_preserves_remainders_and_history_without_enrollment(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        state = await setup(db)
        manifest = await inspect(db)
        assert manifest.total_bytes == 800
        assert [(g.source_key, g.original_bytes, g.remaining_bytes) for g in manifest.grants] == [
            ('base:paid', 500, 100),
            ('purchase:synthetic', 900, 700),
        ]
        assert manifest.source_revision == 3 and manifest.business_strategy == 'MONTH'
        assert (await inspect(db)) == manifest
        assert len((await db.scalars(select(TrafficAllowance))).all()) == 4
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'
        assert state.revision == 3 and state.used_bytes == 200


async def test_destination_is_explicit_and_changes_fingerprint_not_amount(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        first, second = await inspect(db), await inspect(db, destination_group='bs')
        assert first.fingerprint != second.fingerprint and first.total_bytes == second.total_bytes
        assert first.grants == second.grants
        with pytest.raises(LedgerConflict):
            await inspect(db, destination_group=None)


@pytest.mark.parametrize(
    'field,value',
    [
        ('phase', 'prepared'),
        ('phase', 'recreating'),
        ('grace_excluded', True),
        ('activation_requested', True),
        ('pending_sync_fields', ['expire_at']),
        ('projected_revision', 2),
    ],
)
async def test_unsettled_source_is_rejected(postgres_database, field, value):
    async with postgres_session(postgres_database, TABLES) as db:
        state = await setup(db)
        setattr(state, field, value)
        await db.flush()
        with pytest.raises(LedgerConflict, match='not ready'):
            await inspect(db)


async def test_grant_budget_never_silently_truncates(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        for i in range(63):
            db.add(
                TrafficAllowance(
                    subscription_id=1,
                    operation_key=f'package:{i}',
                    granted_bytes=1,
                    remaining_bytes=1,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
        await db.flush()
        with pytest.raises(LedgerConflict, match='budget'):
            await inspect(db)


async def test_spent_source_and_revision_change_manifest_without_restoring_spent_bytes(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        state = await setup(db)
        first = await inspect(db)
        row = await db.get(TrafficAllowance, (1, 'purchase:synthetic'))
        row.remaining_bytes = 600
        state.revision = state.projected_revision = 4
        await db.flush()
        second = await inspect(db)
        assert first.fingerprint != second.fingerprint and second.total_bytes == 700
        assert first.grants[1].destination_key == second.grants[1].destination_key


async def test_foreign_owner_cannot_inspect(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        with pytest.raises(LedgerConflict, match='another owner'):
            await inspect(db, user_id=2)


async def test_unlimited_source_does_not_invent_finite_entitlement(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        state = await setup(db)
        state.phase = 'unlimited'
        await db.flush()
        result = await inspect(db)
        assert result.source_phase == 'unlimited' and result.total_bytes == 800


@pytest.mark.parametrize('kind', ['base', 'reset', 'grace'])
async def test_unfinished_money_or_grace_operation_blocks_inspection(postgres_database, kind):
    from app.database.models import PaidTrafficResetIntent, TrafficBaseGrantIntent

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        now = datetime.now(UTC)
        if kind == 'base':
            db.add(
                TrafficBaseGrantIntent(
                    subscription_id=1,
                    operation_key='pending-base',
                    base_bytes=100,
                    business_strategy='MONTH',
                    requested_at=now,
                    expires_at=now + timedelta(days=30),
                )
            )
        elif kind == 'reset':
            db.add(
                PaidTrafficResetIntent(
                    subscription_id=1, user_id=1, operation_key='pending-reset', charge_at=now, reason='synthetic'
                )
            )
        else:
            (await db.get(Subscription, 1)).grace_session_open = True
        await db.flush()
        with pytest.raises(LedgerConflict):
            await inspect(db)


async def test_existing_split_grant_prevents_double_import_preparation(postgres_database):
    from app.services.bs_traffic.ledger import grant

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        now = datetime.now(UTC)
        await grant(
            db,
            **TARGET,
            group='ordinary',
            operation_key='already-issued',
            amount_bytes=100,
            starts_at=now,
            expires_at=now + timedelta(days=1),
        )
        with pytest.raises(LedgerConflict, match='empty'):
            await inspect(db)


async def test_manifest_lock_blocks_concurrent_purchase_until_release(postgres_database):
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await db.commit()
        manifest = await inspect(db)
        factory = async_sessionmaker(db.bind)
        async with factory() as other:
            with pytest.raises(DBAPIError):
                await other.scalar(select(Subscription.id).where(Subscription.id == 1).with_for_update(nowait=True))
            await other.rollback()
            await db.rollback()
            assert (
                await other.scalar(select(Subscription.id).where(Subscription.id == 1).with_for_update(nowait=True))
                == 1
            )
        assert manifest.total_bytes == 800
