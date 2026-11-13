from datetime import timedelta

import pytest

from app.database.models import BsTrafficAllowance
from app.services.bs_traffic.base_renewal import reset_base_group
from app.services.bs_traffic.ledger import LedgerConflict, open_usage, resolve_usage, summary
from tests.database.test_bs_base_renewal_postgres import setup
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('pending_first', [False, True])
async def test_no_reset_keeps_one_balance_for_late_and_pending_usage(postgres_database, monkeypatch, pending_first):
    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, topup = await setup(db, monkeypatch)
        usage = dict(
            target,
            group='bs',
            source_key='historical',
            evidence_kind='classified',
            amount_bytes=30,
            interval_start=now,
            interval_end=now + timedelta(milliseconds=500),
        )
        event = await open_usage(db, **usage) if pending_first else None
        terms = dict(
            target,
            group='bs',
            operation_key='no-reset',
            amount_bytes=100,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=60),
            reset_base=False,
        )
        result = await reset_base_group(db, **terms)
        assert result == base
        event = event or await open_usage(db, **usage)
        await resolve_usage(db, **target, usage_id=event, decision='charge')
        assert (await db.get(BsTrafficAllowance, result)).remaining_bytes == 70
        assert (await db.get(BsTrafficAllowance, topup)).expires_at == now + timedelta(days=30)
        assert await reset_base_group(db, **terms) == base
        future = await summary(db, **target, now=now + timedelta(days=40))
        assert future['bs']['remaining_bytes'] == 70
        with pytest.raises(LedgerConflict):
            await reset_base_group(db, **(terms | {'reset_base': True}))


async def test_extension_cannot_fill_unpaid_gap_or_resize_budget(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, _ = await setup(db, monkeypatch)
        terms = dict(
            target,
            group='bs',
            operation_key='after-expiry',
            amount_bytes=100,
            starts_at=now + timedelta(days=40),
            expires_at=now + timedelta(days=70),
            reset_base=False,
        )
        assert await reset_base_group(db, **terms) == base
        gap = await summary(db, **target, now=now + timedelta(days=35))
        assert gap['bs']['remaining_bytes'] == 0
        funded = await summary(db, **target, now=now + timedelta(days=50))
        assert funded['bs']['remaining_bytes'] == 100
        from sqlalchemy import select

        from app.services.bs_traffic.base_extension import effective_expiry, extension_boundary

        assert (
            await db.scalar(
                select(effective_expiry(BsTrafficAllowance)).where(
                    BsTrafficAllowance.id == base,
                )
            )
            == terms['expires_at']
        )
        assert await extension_boundary(db, target['subscription_id'], now + timedelta(days=35)) == terms['starts_at']
        with pytest.raises(LedgerConflict):
            await reset_base_group(
                db,
                **(
                    terms
                    | {
                        'operation_key': 'stale',
                        'starts_at': now + timedelta(days=39),
                        'expires_at': now + timedelta(days=90),
                    }
                ),
            )
        for change in ({'user_id': 2}, {'amount_bytes': 200}):
            with pytest.raises(LedgerConflict):
                await reset_base_group(db, **(terms | change))


async def test_new_usage_shares_extended_base_and_reset_retires_extension(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, _ = await setup(db, monkeypatch)
        terms = dict(
            target,
            group='bs',
            operation_key='shared',
            amount_bytes=100,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=60),
            reset_base=False,
        )
        await reset_base_group(db, **terms)
        event = await open_usage(
            db,
            **target,
            group='bs',
            source_key='new-interval',
            amount_bytes=40,
            interval_start=now + timedelta(days=35),
            interval_end=now + timedelta(days=36),
        )
        await resolve_usage(db, **target, usage_id=event, decision='charge')
        assert (await db.get(BsTrafficAllowance, base)).remaining_bytes == 60
        fresh = await reset_base_group(
            db, **(terms | {'operation_key': 'new-reset', 'reset_base': True, 'starts_at': now + timedelta(days=40)})
        )
        assert fresh != base
        assert (await summary(db, **target, now=now + timedelta(days=41)))['bs']['remaining_bytes'] == 100
        with pytest.raises(LedgerConflict):
            await reset_base_group(
                db, **(terms | {'operation_key': 'new-reset', 'starts_at': now + timedelta(days=40)})
            )


async def test_extension_migration_refuses_loss_of_paid_history(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0156_bs_base_extension.py')
    async with postgres_session(postgres_database, TABLES) as db:
        target, now, _, _ = await setup(db, monkeypatch)
        connection = await db.connection()

        def empty_cycle(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()
                migration.upgrade()

        await connection.run_sync(empty_cycle)
        await reset_base_group(
            db,
            **target,
            group='bs',
            operation_key='retained',
            amount_bytes=100,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=60),
            reset_base=False,
        )

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)):
                with pytest.raises(RuntimeError, match='retain usage rights'):
                    migration.downgrade()

        await connection.run_sync(refuse)
