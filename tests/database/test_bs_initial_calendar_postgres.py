import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficManagedTransfer, Subscription, Transaction
from app.services.bs_traffic.calendar_renewal import refresh_calendar_base
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_initial_calendar_uses_original_paid_budget_not_remainder_or_current_tariff(
    postgres_database, monkeypatch
):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        transfer = await db.get(BsTrafficManagedTransfer, 1)
        terms = transfer.calendar_terms
        assert terms['amounts'] == {'ordinary': 500}
        base = next(item for item in transfer.grants if item['source_key'].startswith('base:'))
        topup = next(item for item in transfer.grants if item['source_key'].startswith('purchase:'))
        assert (await db.get(BsTrafficAllowance, base['grant_id'])).remaining_bytes == 100
        due = datetime.fromisoformat(terms['next_refresh_at'])
        result = await refresh_calendar_base(db, **target, now=due)
        assert set(result.grants) == {'ordinary'}
        assert (await db.get(BsTrafficAllowance, result.grants['ordinary'])).remaining_bytes == 500
        assert (await db.get(BsTrafficAllowance, topup['grant_id'])).remaining_bytes == topup['imported_bytes']
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        await db.commit()
        assert (await refresh_calendar_base(db, **target, now=due)).status == 'not_due'


@pytest.mark.parametrize('change', ['generation', 'expired', 'legacy_receipt'])
async def test_initial_calendar_rejects_unproved_or_expired_source(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        transfer = await db.get(BsTrafficManagedTransfer, 1)
        due = datetime.fromisoformat(transfer.calendar_terms['next_refresh_at'])
        before = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        if change == 'generation':
            with pytest.raises(LedgerConflict):
                await refresh_calendar_base(db, **(target | {'account_generation': 'f' * 64}), now=due)
        elif change == 'expired':
            assert (
                await refresh_calendar_base(db, **target, now=(await db.get(Subscription, 1)).end_date)
            ).status == 'expired'
        else:
            transfer.calendar_terms = None
            await db.flush()
            assert (await refresh_calendar_base(db, **target, now=due)).status == 'no_paid_calendar'
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == before


async def test_initial_calendar_concurrent_workers_do_not_duplicate_grant(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        due = datetime.fromisoformat((await db.get(BsTrafficManagedTransfer, 1)).calendar_terms['next_refresh_at'])
        await db.rollback()

        async def refresh():
            async with reader._factory() as other, other.begin():
                return await refresh_calendar_base(other, **target, now=due)

        results = await asyncio.gather(refresh(), refresh())
        assert sorted(item.status for item in results) == ['not_due', 'refreshed']


async def test_initial_no_reset_source_does_not_replenish(postgres_database, monkeypatch):
    from app.database.models import TrafficAccountingState
    from tests.database import test_bs_traffic_managed_cutover_postgres as fixture

    original = fixture.prepared

    async def no_reset(db):
        result = await original(db)
        source = await db.get(TrafficAccountingState, 1)
        source.business_strategy = 'NO_RESET'
        source.next_refresh_at = None
        await db.commit()
        return result

    monkeypatch.setattr(fixture, 'prepared', no_reset)
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        terms = (await db.get(BsTrafficManagedTransfer, 1)).calendar_terms
        assert terms['calendar_strategy'] == 'NO_RESET' and terms['next_refresh_at'] is None
        now = datetime.fromisoformat(terms['captured_at']) + timedelta(days=1)
        assert (await refresh_calendar_base(db, **target, now=now)).status == 'not_due'


async def test_initial_calendar_waits_for_confirmed_activation(postgres_database, monkeypatch):
    from tests.database.test_bs_traffic_cutover_activation_postgres import ready

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, _, _ = await ready(db, monkeypatch)
        due = datetime.fromisoformat((await db.get(BsTrafficManagedTransfer, 1)).calendar_terms['next_refresh_at'])
        with pytest.raises(LedgerConflict):
            await refresh_calendar_base(db, **target, now=due)


async def test_genuine_renewal_supersedes_initial_calendar(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal import renew_from_balance
    from tests.database.test_bs_paid_renewal_postgres import prepared

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, _ = await prepared(db, monkeypatch)
        due = datetime.fromisoformat((await db.get(BsTrafficManagedTransfer, 1)).calendar_terms['next_refresh_at'])
        await renew_from_balance(db, **target, period_days=30, expected_price_kopeks=2000, operation_key='genuine')
        await db.commit()
        # New paid terms use NO_RESET; the imported MONTH calendar must not fire.
        assert (await refresh_calendar_base(db, **target, now=due)).status == 'not_due'


async def test_fully_spent_retained_paid_base_can_refresh_without_importing_balance(postgres_database, monkeypatch):
    from dataclasses import replace

    from app.database.models import TrafficAccountingState, TrafficAllowance
    from tests.database import (
        test_bs_traffic_cutover_intent_postgres as intent_fixture,
        test_bs_traffic_managed_cutover_postgres as fixture,
    )

    original = fixture.prepared

    async def exhausted(db):
        result = await original(db)
        (await db.get(TrafficAccountingState, 1)).base_budget_bytes = 500
        base = await db.scalar(select(TrafficAllowance).where(TrafficAllowance.operation_key == 'base:paid'))
        base.remaining_bytes = 0
        reader = result[0]
        reader._api.get_split_transfer_snapshot.return_value = replace(
            reader._api.get_split_transfer_snapshot.return_value,
            traffic_limit_bytes=900,
        )
        await db.commit()
        return result

    monkeypatch.setattr(fixture, 'prepared', exhausted)
    original_setup = intent_fixture.setup

    async def exhausted_snapshot(db, monkeypatch):
        result = await original_setup(db, monkeypatch)
        api = result[1]
        api.get_split_grace_snapshot.return_value = replace(
            api.get_split_grace_snapshot.return_value,
            traffic_limit_bytes=900,
        )
        return result

    monkeypatch.setattr(intent_fixture, 'setup', exhausted_snapshot)
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        transfer = await db.get(BsTrafficManagedTransfer, 1)
        assert all(not item['source_key'].startswith('base:') for item in transfer.grants)
        assert transfer.calendar_terms['amounts'] == {'ordinary': 500}
        due = datetime.fromisoformat(transfer.calendar_terms['next_refresh_at'])
        refreshed = await refresh_calendar_base(db, **target, now=due)
        assert (await db.get(BsTrafficAllowance, refreshed.grants['ordinary'])).remaining_bytes == 500


async def test_initial_calendar_upgrade_keeps_existing_receipt_without_guessing_terms(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0157_bs_initial_calendar.py')
    async with postgres_session(postgres_database, TABLES) as db:
        await prepared_purchase(db, monkeypatch)
        transfer = await db.get(BsTrafficManagedTransfer, 1)
        saved_grants = transfer.grants
        transfer.calendar_terms = None
        await db.flush()

        def cycle(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()
                migration.upgrade()

        await (await db.connection()).run_sync(cycle)
        await db.refresh(transfer)
        assert transfer.grants == saved_grants and transfer.calendar_terms is None
        transfer.calendar_terms = {'version': 1}
        await db.flush()

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)):
                with pytest.raises(RuntimeError, match='history must be retained'):
                    migration.downgrade()

        await (await db.connection()).run_sync(refuse)
