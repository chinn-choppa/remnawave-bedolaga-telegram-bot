from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficCutoverIntent,
    BsTrafficManagedTransfer,
    Subscription,
    TrafficAccountingState,
    TrafficAllowance,
)
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_transfer_capture import capture_managed_transfer
from app.services.bs_traffic.managed_transfer_commit import commit_managed_transfer
from tests.database.test_bs_traffic_managed_capture_postgres import prepared
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def ready(db, monkeypatch):
    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    reader, api, keys = await prepared(db)
    observation = await capture_managed_transfer(reader, **keys)
    keys.pop('destination_group')
    return reader, dict(keys, operation_key='transfer-1', observation=observation)


async def test_atomic_import_preserves_source_and_replay_does_not_recredit(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        assert await commit_managed_transfer(db, **keys) == 1
        await db.commit()
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
        assert (await db.get(TrafficAccountingState, 1)).phase == 'transferred'
        assert len((await db.scalars(select(TrafficAllowance))).all()) == 4
        imported = (await db.scalars(select(BsTrafficAllowance).order_by(BsTrafficAllowance.id))).all()
        assert sum(g.remaining_bytes for g in imported) == 800
        receipt = await db.get(BsTrafficManagedTransfer, 1)
        assert receipt.imported_bytes == 800 and len(receipt.grants) == 2
        imported[0].remaining_bytes = 0
        await db.commit()
        assert await commit_managed_transfer(db, **keys) == 1
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == 2
        assert (await db.get(BsTrafficAllowance, imported[0].id)).remaining_bytes == 0


@pytest.mark.parametrize('reason', ['no_opt_in', 'expired', 'foreign', 'changed_source'])
async def test_invalid_transfer_does_not_mutate_balances(postgres_database, monkeypatch, reason):
    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        if reason == 'no_opt_in':
            monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
        elif reason == 'expired':
            keys['observation'] = replace(keys['observation'], valid_until=datetime.now(UTC) - timedelta(seconds=1))
        elif reason == 'foreign':
            keys['user_id'] = 2
        else:
            (await db.get(TrafficAllowance, (1, 'purchase:synthetic'))).remaining_bytes = 600
            await db.commit()
        with pytest.raises(LedgerConflict):
            await commit_managed_transfer(db, **keys)
        await db.commit()
        assert await db.get(BsTrafficManagedTransfer, 1) is None
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'


async def test_failure_after_first_grant_rolls_back_even_if_caller_commits(postgres_database, monkeypatch):
    from app.services.bs_traffic import managed_transfer_commit as module

    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        original = module.grant

        async def fail(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError('synthetic interruption')

        monkeypatch.setattr(module, 'grant', fail)
        with pytest.raises(RuntimeError):
            await commit_managed_transfer(db, **keys)
        await db.commit()
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'
        assert (await db.get(TrafficAccountingState, 1)).phase == 'ready'


async def test_concurrent_transfers_create_one_receipt_and_two_grants(postgres_database, monkeypatch):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys = await ready(db, monkeypatch)

        async def run():
            async with reader._factory() as worker, worker.begin():
                return await commit_managed_transfer(worker, **keys)

        assert await asyncio.gather(run(), run()) == [1, 1]
        assert await db.scalar(select(func.count()).select_from(BsTrafficManagedTransfer)) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == 2


async def test_outer_rollback_reverts_successful_transfer(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        await commit_managed_transfer(db, **keys)
        await db.rollback()
        assert await db.get(BsTrafficManagedTransfer, 1) is None
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'
        assert (await db.get(TrafficAccountingState, 1)).phase == 'ready'


async def test_opt_in_revoked_during_import_reverts_all_writes(postgres_database, monkeypatch):
    from app.services.bs_traffic import managed_transfer_commit as module

    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        original = module.grant

        async def revoke(*args, **kwargs):
            result = await original(*args, **kwargs)
            monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
            return result

        monkeypatch.setattr(module, 'grant', revoke)
        with pytest.raises(LedgerConflict):
            await commit_managed_transfer(db, **keys)
        await db.commit()
        assert await db.get(BsTrafficManagedTransfer, 1) is None
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'


async def test_migration_and_downgrade_preserve_receipt_against_reimport(postgres_database, monkeypatch):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0149_bs_managed_transfer.py')
    async with postgres_session(postgres_database, TABLES) as db:
        connection = await db.connection()

        def install(conn):
            with Operations.context(MigrationContext.configure(conn)):
                _load('0162_bs_enrollment.py').downgrade()
            BsTrafficCutoverIntent.__table__.drop(conn)
            BsTrafficManagedTransfer.__table__.drop(conn)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
                _load('0157_bs_initial_calendar.py').upgrade()
            BsTrafficCutoverIntent.__table__.create(conn)
            with Operations.context(MigrationContext.configure(conn)):
                _load('0162_bs_enrollment.py').upgrade()
                _load('0163_bs_ownership_cycles.py').upgrade()
            table = BsTrafficManagedTransfer.__table__
            inspector = sa.inspect(conn)
            assert {c['name'] for c in inspector.get_columns(table.name)} == set(table.c.keys())
            assert {c['name'] for c in inspector.get_check_constraints(table.name)} == {
                c.name for c in table.constraints if isinstance(c, sa.CheckConstraint)
            }

        await connection.run_sync(install)
        _, keys = await ready(db, monkeypatch)
        await commit_managed_transfer(db, **keys)
        await db.commit()

        def preserve(conn):
            with Operations.context(MigrationContext.configure(conn)):
                migration.downgrade()
                migration.upgrade()

        await (await db.connection()).run_sync(preserve)
        with pytest.raises(LedgerConflict, match='different transfer'):
            await commit_managed_transfer(db, **dict(keys, operation_key='transfer-2'))
        await db.commit()
        assert await db.get(BsTrafficManagedTransfer, 1) is not None
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == 2


@pytest.mark.parametrize('configured', [False, True])
async def test_tariff_off_after_observation_refuses_new_transfer(postgres_database, monkeypatch, configured):
    from app.database.models import Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.bs_traffic_settings = (
            {'enabled': False, 'squad_uuids': ['33333333-3333-4333-8333-333333333333'], 'limit_gb': 10}
            if configured
            else None
        )
        await db.commit()
        with pytest.raises(LedgerConflict, match='tariff'):
            await commit_managed_transfer(db, **keys)
        await db.commit()
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'managed'
        assert (await db.get(TrafficAccountingState, 1)).phase == 'ready'
        assert await db.get(BsTrafficManagedTransfer, 1) is None
        assert await db.scalar(select(BsTrafficAllowance.id)) is None


async def test_saved_transfer_receipt_remains_replayable_after_tariff_off(postgres_database, monkeypatch):
    from app.database.models import Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        await commit_managed_transfer(db, **keys)
        await db.commit()
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.bs_traffic_settings = dict(tariff.bs_traffic_settings, enabled=False)
        await db.commit()
        original = (await db.scalars(select(BsTrafficAllowance).order_by(BsTrafficAllowance.id))).all()
        before = [(g.id, g.remaining_bytes, g.expires_at) for g in original]
        assert await commit_managed_transfer(db, **keys) == 1
        await db.commit()
        after = (await db.scalars(select(BsTrafficAllowance).order_by(BsTrafficAllowance.id))).all()
        assert [(g.id, g.remaining_bytes, g.expires_at) for g in after] == before


async def test_tariff_disable_cannot_interleave_with_uncommitted_transfer(postgres_database, monkeypatch):
    from sqlalchemy.exc import DBAPIError

    from app.database.models import Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys = await ready(db, monkeypatch)
        await commit_managed_transfer(db, **keys)
        tariff_id = (await db.get(Subscription, 1)).tariff_id
        async with reader._factory() as other:
            with pytest.raises(DBAPIError) as error:
                await other.execute(select(Tariff.id).where(Tariff.id == tariff_id).with_for_update(nowait=True))
            assert error.value.orig.sqlstate == '55P03'
            await other.rollback()
        await db.rollback()
        assert await db.get(BsTrafficManagedTransfer, 1) is None


async def test_classic_mode_cannot_enroll_even_with_configured_tariff(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, keys = await ready(db, monkeypatch)
        monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: False)
        with pytest.raises(LedgerConflict, match='tariff'):
            await commit_managed_transfer(db, **keys)
        assert await db.get(BsTrafficManagedTransfer, 1) is None
        assert await db.scalar(select(BsTrafficAllowance.id)) is None
