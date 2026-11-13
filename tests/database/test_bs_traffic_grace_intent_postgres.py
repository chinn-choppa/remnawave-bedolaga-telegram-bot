from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficLedger, BsTrafficPanelBinding, User
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore
from app.services.grace_access_service import GraceSessionState
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, TARGET
from tests.database.test_grace_split_snapshot_postgres import prepared_session
from tests.fixtures.postgres_db import postgres_session
from tests.services.test_bs_traffic_grace_confirmation import state


pytestmark = pytest.mark.postgres
TABLES = [BsTrafficPanelBinding.__table__, User.__table__]


async def setup(db):
    source = await prepared_session(db)
    # The synthetic source uses a placeholder generation; bind to the strict
    # full account used by this test before persisting its grace snapshot.
    panel = state()
    generation = account_generation(panel_scope=SCOPE, account=panel)
    ledger = await db.get(BsTrafficLedger, 1)
    ledger.account_generation = generation
    await db.flush()
    source = replace(source, split_before=replace(source.split_before, account_generation=generation))
    store = SQLAlchemyGraceSessionStore(db)
    saved = await store.create(source)
    saved = await store.save(replace(saved, state=GraceSessionState.RESTORING))
    await db.commit()
    revision = await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1))
    return dict(
        TARGET,
        account_generation=generation,
        grace_id=saved.id,
        grace_version=saved.version,
        revision=revision,
        target=panel,
        expected_source=replace(
            panel,
            status=saved.overlay.status,
            expire_at=saved.overlay.expire_at,
            traffic_limit_bytes=saved.overlay.traffic_limit_bytes,
            external_squad_uuid=saved.overlay.external_squad_uuid,
            active_internal_squads=[{'uuid': item} for item in saved.overlay.squad_uuids],
        ),
        valid_until=datetime.now(UTC) + timedelta(seconds=30),
    )


async def test_persisted_target_survives_reload_and_same_retry(postgres_database):
    from app.database.models import BsTrafficGraceIntent
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        identity = await stage_restore(db, **args)
        await db.commit()
        db.expire_all()
        assert await stage_restore(db, **args) == identity
        row = await db.get(BsTrafficGraceIntent, identity)
        assert row.target['hwid_device_limit'] == 2
        assert row.target['used_traffic_bytes'] == 100
        assert row.state == 'pending'


@pytest.mark.parametrize('field', ['device', 'expiry', 'counter'])
async def test_retry_cannot_replace_durable_target(postgres_database, field):
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        await stage_restore(db, **args)
        await db.commit()
        changes = {
            'device': {'hwid_device_limit': 3},
            'expiry': {'expire_at': args['target'].expire_at + timedelta(days=1)},
            'counter': {'used_traffic_bytes': 101},
        }
        args['target'] = replace(args['target'], **changes[field])
        with pytest.raises(LedgerConflict):
            await stage_restore(db, **args)


@pytest.mark.parametrize(
    'field,value', [('user_id', 2), ('revision', 999), ('grace_version', 999), ('grace_id', 'missing')]
)
async def test_stale_or_foreign_context_cannot_stage(postgres_database, field, value):
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        with pytest.raises(LedgerConflict):
            await stage_restore(db, **(args | {field: value}))


async def test_expired_evidence_does_not_stage(postgres_database):
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        with pytest.raises(LedgerConflict):
            await stage_restore(db, **(args | {'valid_until': datetime.now(UTC) - timedelta(seconds=1)}))


async def test_concurrent_identical_requests_store_one_target(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import BsTrafficGraceIntent
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        await db.commit()
        factory = async_sessionmaker(db.bind)

        async def stage():
            async with factory() as worker, worker.begin():
                return await stage_restore(worker, **args)

        first, second = await asyncio.gather(stage(), stage())
        assert first == second
        assert len((await db.scalars(select(BsTrafficGraceIntent))).all()) == 1


async def test_rollback_does_not_leave_target(postgres_database):
    from app.database.models import BsTrafficGraceIntent
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        await stage_restore(db, **args)
        await db.rollback()
        assert await db.scalar(select(BsTrafficGraceIntent)) is None


async def test_retired_panel_epoch_cannot_stage(postgres_database):
    from app.services.bs_traffic.grace_intent import stage_restore
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        await retire_binding(db, panel_scope=SCOPE)
        with pytest.raises(LedgerConflict):
            await stage_restore(db, **args)


async def test_migration_upgrade_twice_and_stage(postgres_database):
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import text

    from app.services.bs_traffic.grace_intent import stage_restore

    spec = importlib.util.spec_from_file_location(
        'intent_migration', Path('migrations/alembic/versions/0148_bs_traffic_grace_intent.py')
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    async with postgres_session(postgres_database, TABLES) as db:
        await db.execute(text('DROP TABLE bs_traffic_grace_intents'))
        connection = await db.connection()

        def upgrade(sync):
            with Operations.context(MigrationContext.configure(sync)):
                module.upgrade()
                module.upgrade()
                # Current ORM includes the later durable lifecycle columns.
                from tests.database.test_retired_gateway_migration_postgres import _load

                _load('0159_bs_grace_lifecycle.py').upgrade()

        await connection.run_sync(upgrade)
        await db.commit()
        args = await setup(db)
        first = await stage_restore(db, **args)
        from app.services.bs_traffic.grace_intent import cancel_pending_restore

        keys = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        assert await cancel_pending_restore(db, **keys, operation_id=first)
        assert await stage_restore(db, **args) != first


async def test_only_one_committed_claim_survives_restart(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import BsTrafficGraceIntent
    from app.services.bs_traffic.grace_intent import claim_restore, stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        operation = await stage_restore(db, **args)
        await db.commit()
        factory = async_sessionmaker(db.bind)
        claim_args = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}

        async def claim():
            async with factory() as worker, worker.begin():
                return await claim_restore(worker, **claim_args, operation_id=operation)

        results = await asyncio.gather(claim(), claim())
        receipts = [result for result in results if result is not None]
        assert len(receipts) == 1
        assert receipts[0].target == args['target']
        assert receipts[0].expected_source == args['expected_source']
        assert await claim() is None
        row = await db.get(BsTrafficGraceIntent, operation)
        assert row.state == 'attempted' and row.attempt_token == receipts[0].token
        assert row.attempted_at is not None


@pytest.mark.parametrize(
    'change', ['expiry', 'revision', 'grace_version', 'owner_status', 'devices', 'external', 'retired']
)
async def test_stale_local_state_cannot_claim_restore(postgres_database, change):
    from sqlalchemy import update

    from app.database.models import BsTrafficGraceIntent, GraceAccessSessionModel
    from app.services.bs_traffic.grace_intent import claim_restore, stage_restore
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        operation = await stage_restore(db, **args)
        await db.commit()
        if change == 'expiry':
            await db.execute(update(BsTrafficGraceIntent).values(valid_until=datetime.now(UTC) - timedelta(seconds=1)))
        elif change == 'revision':
            await db.execute(update(BsTrafficLedger).values(revision=BsTrafficLedger.revision + 1))
        elif change == 'grace_version':
            await db.execute(update(GraceAccessSessionModel).values(version=GraceAccessSessionModel.version + 1))
        elif change == 'devices':
            from app.database.models import Subscription

            await db.execute(update(Subscription).where(Subscription.id == 1).values(device_limit=99))
        elif change == 'external':
            from app.database.models import Tariff

            await db.execute(update(Tariff).values(external_squad_uuid='99999999-9999-4999-8999-999999999999'))
        elif change == 'owner_status':
            await db.execute(User.__table__.update().where(User.id == 1).values(status='blocked'))
        else:
            await retire_binding(db, panel_scope=SCOPE)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await claim_restore(
                db,
                **{key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')},
                operation_id=operation,
            )
        assert (await db.get(BsTrafficGraceIntent, operation)).state == 'pending'


async def test_expired_attempt_is_not_reissued_and_payload_corruption_is_rejected(postgres_database):
    from sqlalchemy import update

    from app.database.models import BsTrafficGraceIntent
    from app.services.bs_traffic.grace_intent import claim_restore, stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        operation = await stage_restore(db, **args)
        await db.commit()
        keys = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        row = await db.get(BsTrafficGraceIntent, operation)
        original = dict(row.target)
        row.target = original | {'hwid_device_limit': 999}
        await db.commit()
        with pytest.raises(LedgerConflict):
            await claim_restore(db, **keys, operation_id=operation)
        await db.rollback()
        await db.execute(
            update(BsTrafficGraceIntent).where(BsTrafficGraceIntent.id == operation).values(target=original)
        )
        await db.commit()
        assert await claim_restore(db, **keys, operation_id=operation) is not None
        await db.commit()
        await db.execute(
            update(BsTrafficGraceIntent)
            .where(BsTrafficGraceIntent.id == operation)
            .values(send_deadline=datetime.now(UTC) - timedelta(seconds=1))
        )
        await db.commit()
        assert await claim_restore(db, **keys, operation_id=operation) is None


async def test_expected_source_is_immutable_and_must_match_saved_overlay(postgres_database):
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        with pytest.raises(LedgerConflict):
            await stage_restore(
                db, **(args | {'expected_source': replace(args['expected_source'], traffic_limit_bytes=999)})
            )
        await stage_restore(db, **args)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await stage_restore(
                db, **(args | {'expected_source': replace(args['expected_source'], hwid_device_limit=99)})
            )


@pytest.mark.parametrize('change', ['account', 'reset', 'counter', 'status'])
async def test_foreign_reset_or_disabled_source_cannot_be_staged(postgres_database, change):
    from app.services.bs_traffic.grace_intent import stage_restore

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db)
        source = args['expected_source']
        changes = {
            'account': {'id': 99},
            'reset': {'last_traffic_reset_at': datetime.now(UTC)},
            'counter': {'used_traffic_bytes': source.used_traffic_bytes + 1},
            'status': {'status': 'DISABLED'},
        }
        with pytest.raises(LedgerConflict):
            await stage_restore(db, **(args | {'expected_source': replace(source, **changes[change])}))
