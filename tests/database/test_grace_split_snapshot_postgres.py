from dataclasses import replace

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficLedger, BsTrafficPanelBinding, GraceAccessSessionModel, Subscription, User
from app.services.grace_access_codec import GraceSnapshotError
from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore
from app.services.grace_access_service import GraceSessionState
from tests.database.test_bs_traffic_grace_transition_postgres import grace_session
from tests.database.test_bs_traffic_policy_postgres import configure, seed
from tests.fixtures.postgres_db import postgres_session
from tests.services.test_grace_split_snapshot import context


pytestmark = pytest.mark.postgres
TABLES = [BsTrafficPanelBinding.__table__, User.__table__]


async def prepared_session(db):
    from app.services.bs_traffic.panel_binding import register_binding
    from tests.database.test_bs_traffic_ledger_postgres import SCOPE

    await register_binding(db, panel_scope=SCOPE, endpoint='https://panel.example')
    await seed(db)
    evidence = await configure(db)
    revision = await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1))
    session = grace_session()
    return replace(
        replace(session, panel_before=replace(session.panel_before, squad_uuids=context().paid_squads)),
        split_before=context(
            ledger_revision=revision,
            topology_fingerprint=evidence.fingerprint,
            tariff_id=await db.scalar(select(Subscription.tariff_id).where(Subscription.id == 1)),
        ),
    )


@pytest.mark.parametrize('change', ['drop', 'replace'])
async def test_saved_split_context_cannot_be_replaced_or_dropped(postgres_database, change):
    async with postgres_session(postgres_database, TABLES) as db:
        store = SQLAlchemyGraceSessionStore(db)
        saved = await store.create(await prepared_session(db))
        changed = None if change == 'drop' else replace(saved.split_before, paid_squads=())
        with pytest.raises(GraceSnapshotError):
            await store.save(replace(saved, split_before=changed, state=GraceSessionState.ACTIVE))
        await db.rollback()
        reloaded = await store.get_open(1)
        assert reloaded.split_before == saved.split_before


async def test_legacy_session_cannot_gain_invented_historical_context(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        store = SQLAlchemyGraceSessionStore(db)
        saved = await store.create(grace_session())
        with pytest.raises(GraceSnapshotError):
            await store.save(replace(saved, split_before=context(), state=GraceSessionState.ACTIVE))


async def test_create_rejects_stale_split_provenance(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        session = await prepared_session(db)
        store = SQLAlchemyGraceSessionStore(db)
        with pytest.raises(GraceSnapshotError):
            await store.create(replace(session, split_before=replace(session.split_before, ledger_revision=999)))
        assert await db.scalar(select(GraceAccessSessionModel)) is None


async def test_split_context_survives_completion_reload_and_duplicate_create(postgres_database):
    from app.services.grace_access_service import GraceCompletionReason
    from tests.database.test_bs_traffic_grace_transition_postgres import complete

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        store = SQLAlchemyGraceSessionStore(db)
        saved = await store.create(source)
        active = await store.save(replace(saved, state=GraceSessionState.ACTIVE))
        completed = await complete(store, active, GraceCompletionReason.PAID)
        await db.commit()
        reloaded = await store.get_by_incident(1, source.incident_key)
        assert reloaded.split_before == source.split_before
        assert reloaded.state is GraceSessionState.COMPLETED
        assert (await store.create(source)).split_before == source.split_before
        assert (await store.save(completed)).split_before == source.split_before


@pytest.mark.parametrize(
    'changes',
    [
        {'panel_scope': '99999999-9999-4999-8999-999999999999'},
        {'account_generation': 'c' * 64},
        {'tariff_id': 999},
        {'topology_fingerprint': 'c' * 64},
    ],
)
async def test_create_rejects_changed_identity_or_policy(postgres_database, changes):
    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        store = SQLAlchemyGraceSessionStore(db)
        with pytest.raises(GraceSnapshotError):
            await store.create(replace(source, split_before=replace(source.split_before, **changes)))
        assert await db.scalar(select(GraceAccessSessionModel)) is None


async def test_concurrent_create_returns_one_preserved_snapshot(postgres_database):
    import asyncio

    from sqlalchemy import func
    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        barrier = asyncio.Barrier(2)

        async def create():
            async with factory() as worker:
                store = SQLAlchemyGraceSessionStore(worker)
                original = store.get_by_incident
                first = True

                async def synchronized_read(*args):
                    nonlocal first
                    result = await original(*args)
                    if first:
                        first = False
                        assert result is None
                        await barrier.wait()
                    return result

                store.get_by_incident = synchronized_read
                return await store.create(source)

        one, two = await asyncio.wait_for(asyncio.gather(create(), create()), 10)
        assert one.id == two.id == source.id
        assert one.split_before == two.split_before == source.split_before
        assert await db.scalar(select(func.count()).select_from(GraceAccessSessionModel)) == 1


async def test_expired_live_evidence_cannot_start_new_grace(postgres_database):
    from datetime import UTC, datetime, timedelta

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        now = datetime.now(UTC)
        with pytest.raises(GraceSnapshotError):
            await SQLAlchemyGraceSessionStore(db).create(
                replace(
                    source,
                    split_before=replace(
                        source.split_before,
                        observed_at=now - timedelta(minutes=2),
                        valid_until=now - timedelta(minutes=1),
                    ),
                )
            )


async def test_retired_capture_cannot_be_persisted_as_new_grace(postgres_database):
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        await retire_binding(db, panel_scope=source.split_before.panel_scope)
        await db.commit()
        with pytest.raises(GraceSnapshotError):
            await SQLAlchemyGraceSessionStore(db).create(source)
        assert await db.scalar(select(GraceAccessSessionModel)) is None


async def test_capture_expiring_during_accounting_preparation_is_not_saved(postgres_database, monkeypatch):
    import asyncio
    from datetime import UTC, datetime, timedelta

    from app.services import traffic_accounting_service

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        now = datetime.now(UTC)
        source = replace(
            source,
            split_before=replace(
                source.split_before, observed_at=now - timedelta(seconds=1), valid_until=now + timedelta(seconds=1)
            ),
        )

        async def slow_prepare(*args):
            await asyncio.sleep(1.2)

        monkeypatch.setattr(traffic_accounting_service, 'prepare_grace_accounting', slow_prepare)
        with pytest.raises(GraceSnapshotError):
            await SQLAlchemyGraceSessionStore(db).create(source)
        assert await db.scalar(select(GraceAccessSessionModel)) is None


@pytest.mark.parametrize('case', ['ordinary_paid', 'ordinary_unlimited', 'bs_paid', 'unsettled'])
async def test_limited_split_grace_cannot_replace_available_or_uncertain_access(postgres_database, case):
    from app.services.grace_access_service import GraceReason
    from tests.database.test_bs_traffic_ledger_postgres import issue, usage

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        if case == 'ordinary_unlimited':
            await configure(db, ordinary='unlimited')
        elif case == 'unsettled':
            await usage(db, group='ordinary')
        else:
            await issue(db, group='ordinary' if case == 'ordinary_paid' else 'bs')
        revision = await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1))
        source = replace(
            source, reason=GraceReason.LIMITED, split_before=replace(source.split_before, ledger_revision=revision)
        )
        with pytest.raises(GraceSnapshotError):
            await SQLAlchemyGraceSessionStore(db).create(source)
        assert await db.scalar(select(GraceAccessSessionModel)) is None
        assert not await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == 1))


@pytest.mark.parametrize('disabled', [False, True])
async def test_limited_split_requires_confirmed_exhaustion_of_enabled_groups(postgres_database, disabled):
    from app.services.grace_access_service import GraceReason

    async with postgres_session(postgres_database, TABLES) as db:
        source = await prepared_session(db)
        if disabled:
            await configure(db, ordinary='disabled', bs='disabled')
        revision = await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1))
        source = replace(
            source, reason=GraceReason.LIMITED, split_before=replace(source.split_before, ledger_revision=revision)
        )
        store = SQLAlchemyGraceSessionStore(db)
        if disabled:
            with pytest.raises(GraceSnapshotError):
                await store.create(source)
            assert await db.scalar(select(GraceAccessSessionModel)) is None
        else:
            saved = await store.create(source)
            assert saved.reason is GraceReason.LIMITED
            assert saved.split_before == source.split_before
