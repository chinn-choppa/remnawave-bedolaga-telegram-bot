"""A paid base refresh must preserve independently purchased split traffic."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficAllowance, BsTrafficLedger
from app.services.bs_traffic.ledger import LedgerConflict, grant, open_usage
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def setup(db, monkeypatch):
    _, _, target, _ = await prepared_purchase(db, monkeypatch)
    now = datetime.now(UTC)
    terms = dict(target, group='bs', starts_at=now, expires_at=now + timedelta(days=30))
    base = await grant(db, **terms, operation_key='base:previous', amount_bytes=100)
    topup = await grant(db, **terms, operation_key='purchase:extra', amount_bytes=70)
    await db.commit()
    return target, now, base, topup


async def test_reset_preserves_topup_and_replay_does_not_reset_new_usage(postgres_database, monkeypatch):
    from app.services.bs_traffic.base_renewal import reset_base_group

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, topup = await setup(db, monkeypatch)
        terms = dict(
            target,
            group='bs',
            operation_key='renewal:one',
            amount_bytes=200,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=30),
        )
        result = await reset_base_group(db, **terms)
        assert (await db.get(BsTrafficAllowance, base)).retired_at == terms['starts_at']
        assert (await db.get(BsTrafficAllowance, topup)).remaining_bytes == 70
        new = await db.get(BsTrafficAllowance, result)
        new.remaining_bytes = 130
        await db.commit()
        assert await reset_base_group(db, **terms) == result
        assert new.remaining_bytes == 130
        with pytest.raises(LedgerConflict):
            await reset_base_group(db, **(terms | {'amount_bytes': 300}))


async def test_pending_usage_keeps_its_old_base_after_refresh(postgres_database, monkeypatch):
    from app.services.bs_traffic.base_renewal import reset_base_group
    from app.services.bs_traffic.ledger import resolve_usage

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, topup = await setup(db, monkeypatch)
        event = await open_usage(
            db,
            **target,
            group='bs',
            source_key='old-usage',
            amount_bytes=40,
            interval_start=now,
            interval_end=now + timedelta(microseconds=1),
        )
        await db.commit()
        new = await reset_base_group(
            db,
            **target,
            group='bs',
            operation_key='renewal:pending',
            amount_bytes=200,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=30),
        )
        await resolve_usage(db, **target, usage_id=event, decision='charge')
        await db.commit()
        assert (await db.get(BsTrafficAllowance, base)).remaining_bytes == 60
        assert (await db.get(BsTrafficAllowance, topup)).remaining_bytes == 70
        assert (await db.get(BsTrafficAllowance, new)).remaining_bytes == 200


async def test_old_new_operation_cannot_replace_later_base(postgres_database, monkeypatch):
    from app.services.bs_traffic.base_renewal import reset_base_group

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, _ = await setup(db, monkeypatch)
        await reset_base_group(
            db,
            **target,
            group='bs',
            operation_key='renewal:new',
            amount_bytes=200,
            starts_at=now + timedelta(seconds=2),
            expires_at=now + timedelta(days=30),
        )
        await db.commit()
        revision = (await db.get(BsTrafficLedger, target['subscription_id'])).revision
        with pytest.raises(LedgerConflict):
            await reset_base_group(
                db,
                **target,
                group='bs',
                operation_key='renewal:older',
                amount_bytes=200,
                starts_at=now + timedelta(seconds=1),
                expires_at=now + timedelta(days=30),
            )
        await db.commit()
        assert (await db.get(BsTrafficLedger, target['subscription_id'])).revision == revision


async def test_imported_base_is_replaced_but_imported_topup_survives(postgres_database, monkeypatch):
    from app.database.models import BsTrafficManagedTransfer
    from app.services.bs_traffic.base_renewal import reset_base_group

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, _, _ = await setup(db, monkeypatch)
        transfer = await db.get(BsTrafficManagedTransfer, target['subscription_id'])
        base_ids = [item['grant_id'] for item in transfer.grants if item['source_key'].startswith('base:')]
        topup_ids = [item['grant_id'] for item in transfer.grants if item['source_key'].startswith('purchase:')]
        assert base_ids and topup_ids
        amounts = {
            identifier: (await db.get(BsTrafficAllowance, identifier)).remaining_bytes for identifier in topup_ids
        }
        await reset_base_group(
            db,
            **target,
            group=transfer.destination_group,
            operation_key='renewal:imported',
            amount_bytes=200,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=30),
        )
        for identifier in base_ids:
            assert (await db.get(BsTrafficAllowance, identifier)).retired_at == now + timedelta(seconds=1)
        for identifier, amount in amounts.items():
            assert (await db.get(BsTrafficAllowance, identifier)).remaining_bytes == amount


async def test_both_groups_roll_back_when_second_group_rejects_refresh(postgres_database, monkeypatch):
    from app.services.bs_traffic.base_renewal import reset_renewal_bases

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, _, _ = await setup(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, target['subscription_id'])
        ledger.access_policy = dict(ledger.access_policy, bs_purchase_enabled=False)
        await db.commit()
        before = list(
            (
                await db.execute(
                    select(BsTrafficAllowance.id, BsTrafficAllowance.remaining_bytes).order_by(BsTrafficAllowance.id)
                )
            ).all()
        )
        with pytest.raises(LedgerConflict):
            await reset_renewal_bases(
                db,
                **target,
                operation_key='renewal:both',
                amounts={'ordinary': 200, 'bs': 200},
                starts_at=now + timedelta(seconds=1),
                expires_at=now + timedelta(days=30),
            )
        await db.commit()
        after = list(
            (
                await db.execute(
                    select(BsTrafficAllowance.id, BsTrafficAllowance.remaining_bytes).order_by(BsTrafficAllowance.id)
                )
            ).all()
        )
        assert before == after


async def test_late_classified_old_usage_does_not_spend_topup_or_new_base(postgres_database, monkeypatch):
    from app.services.bs_traffic.base_renewal import reset_base_group
    from app.services.bs_traffic.ledger import resolve_usage, summary

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, topup = await setup(db, monkeypatch)
        new = await reset_base_group(
            db,
            **target,
            group='bs',
            operation_key='renewal:late',
            amount_bytes=200,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=30),
        )
        event = await open_usage(
            db,
            **target,
            group='bs',
            source_key='late-before-renewal',
            amount_bytes=40,
            interval_start=now,
            interval_end=now + timedelta(milliseconds=1),
        )
        await resolve_usage(db, **target, usage_id=event, decision='charge')
        assert (await db.get(BsTrafficAllowance, topup)).remaining_bytes == 70
        assert (await db.get(BsTrafficAllowance, new)).remaining_bytes == 200
        assert (await db.get(BsTrafficAllowance, base)).remaining_bytes == 60
        assert (await summary(db, **target, now=now + timedelta(seconds=2)))['bs']['remaining_bytes'] == 270


async def test_base_retirement_migration_preserves_history(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import inspect

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0154_bs_base_retirement.py')
    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, _ = await setup(db, monkeypatch)
        connection = await db.connection()

        def cycle(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                assert 'retired_at' not in {c['name'] for c in inspect(sync).get_columns('bs_traffic_allowances')}
                migration.upgrade()
                migration.upgrade()

        await connection.run_sync(cycle)
        item = await db.get(BsTrafficAllowance, base, populate_existing=True)
        assert item.retired_at is None and item.remaining_bytes == 100
        item.retired_at = now + timedelta(seconds=1)
        await db.flush()

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)):
                with pytest.raises(RuntimeError, match='historical usage'):
                    migration.downgrade()

        await connection.run_sync(refuse)
        assert item.remaining_bytes == 100


async def test_two_concurrent_replays_replace_bases_once(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.base_renewal import reset_renewal_bases

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, _, topup = await setup(db, monkeypatch)
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def renew():
            async with factory() as other, other.begin():
                return await reset_renewal_bases(
                    other,
                    **target,
                    operation_key='renewal:concurrent',
                    amounts={'ordinary': 200, 'bs': 300},
                    starts_at=now + timedelta(seconds=1),
                    expires_at=now + timedelta(days=30),
                )

        first, second = await asyncio.gather(renew(), renew())
        assert first == second
        assert len(first) == 2
        assert (await db.get(BsTrafficAllowance, topup)).remaining_bytes == 70


async def test_no_reset_renewal_preserves_spent_base_not_only_topups(postgres_database, monkeypatch):
    from app.services.bs_traffic.base_renewal import reset_renewal_bases

    async with postgres_session(postgres_database, TABLES) as db:
        target, now, base, topup = await setup(db, monkeypatch)
        (await db.get(BsTrafficAllowance, base)).remaining_bytes = 20
        await db.commit()
        result = await reset_renewal_bases(
            db,
            **target,
            operation_key='renewal:keep',
            amounts={'bs': 100},
            reset_base=False,
            starts_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(days=60),
        )
        assert (await db.get(BsTrafficAllowance, result['bs'])).remaining_bytes == 20
        assert (await db.get(BsTrafficAllowance, topup)).remaining_bytes == 70
