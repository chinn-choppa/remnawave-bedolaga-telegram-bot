from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficLedger, Subscription, User
from app.services.bs_traffic.delivery_store import reconcile
from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore
from app.services.grace_access_service import (
    GraceAccessSession,
    GraceBillingState,
    GraceCompletionReason,
    GracePanelOverlay,
    GracePanelSnapshot,
    GraceReason,
    GraceSessionState,
)
from tests.database.test_bs_traffic_delivery_postgres import GUARD, SQUAD, stage, take
from tests.database.test_bs_traffic_ledger_postgres import TARGET, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def grace_session(subscription_id=1):
    now = datetime.now(UTC)
    until = now + timedelta(hours=1)
    return GraceAccessSession(
        id=str(uuid4()),
        subscription_id=subscription_id,
        remnawave_id=12,
        reason=GraceReason.LIMITED,
        incident_key='synthetic',
        state=GraceSessionState.PENDING,
        billing_before=GraceBillingState(subscription_id, 12, 'limited', until, 100, 100, 2, (SQUAD,)),
        panel_before=GracePanelSnapshot(12, 'LIMITED', until, 100, 100, (SQUAD,)),
        overlay=GracePanelOverlay('ACTIVE', until, 200, (SQUAD,)),
        started_at=now,
        grace_until=until,
        updated_at=now,
    )


async def complete(store, session, reason):
    return await store.save(
        replace(
            session,
            state=GraceSessionState.COMPLETED,
            completion_reason=reason,
            completed_at=datetime.now(UTC),
        )
    )


@pytest.mark.parametrize('reason', list(GraceCompletionReason))
@pytest.mark.parametrize('already_attempted', [False, True])
async def test_completed_grace_cycle_invalidates_old_delivery(postgres_database, reason, already_attempted):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        operation = await stage(db)
        attempt = await take(db, operation) if already_attempted else None
        await db.commit()
        store = SQLAlchemyGraceSessionStore(db)
        session = await store.create(grace_session())
        await complete(store, session, reason)
        await db.commit()
        assert await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == 1)) is False
        if attempt is None:
            assert await take(db, operation) is None
        else:
            assert (
                await reconcile(
                    db,
                    **TARGET,
                    operation_id=operation,
                    token=attempt.token,
                    current_guard_hash=GUARD,
                    observed_squads=(SQUAD,),
                )
                == 'attention'
            )


async def test_duplicate_grace_updates_do_not_invalidate_other_subscription(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        store = SQLAlchemyGraceSessionStore(db)
        session = await store.create(grace_session())
        revision = await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1))
        assert revision == 1
        active = await store.save(replace(session, state=GraceSessionState.ACTIVE))
        assert await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1)) == 1
        await complete(store, active, GraceCompletionReason.PAID)
        await complete(store, active, GraceCompletionReason.PAID)  # stale CAS replay
        assert await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1)) == 2
        # A subscription without split accounting keeps its normal grace flow.
        other = await store.create(grace_session(2))
        await complete(store, other, GraceCompletionReason.TIMEOUT)
        assert await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1)) == 2
        assert await db.scalar(select(BsTrafficLedger).where(BsTrafficLedger.subscription_id == 2)) is None


async def test_grace_exit_rollback_restores_marker_and_revision(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        store = SQLAlchemyGraceSessionStore(db)
        session = await store.create(grace_session())
        await complete(store, session, GraceCompletionReason.PAID)
        await db.flush()
        await db.rollback()
        assert await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == 1)) is True
        assert await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1)) == 1
        assert (await store.get_open(1)).state is GraceSessionState.PENDING


async def test_concurrent_marker_replay_increments_revision_once(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.grace_access_runtime import _mark_subscription_grace_open

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await db.commit()
        factory = async_sessionmaker(db.bind)

        async def enter():
            async with factory() as worker, worker.begin():
                await _mark_subscription_grace_open(worker, 1, open_=True)

        await asyncio.gather(enter(), enter())
        assert await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1)) == 1


async def test_grace_and_new_purchase_do_not_move_old_usage_to_new_grant(postgres_database):
    from app.database.models import BsTrafficAllowance, BsTrafficUsage
    from app.services.bs_traffic.ledger import resolve_usage
    from tests.database.test_bs_traffic_ledger_postgres import issue, usage

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        old = await issue(db, amount=100)
        pending = await usage(db, amount=150)
        store = SQLAlchemyGraceSessionStore(db)
        session = await store.create(grace_session())
        new = await issue(db, key='new-purchase', amount=200)
        await complete(store, session, GraceCompletionReason.PAID)
        await resolve_usage(db, **TARGET, usage_id=pending, decision='charge')
        assert await db.scalar(select(BsTrafficAllowance.remaining_bytes).where(BsTrafficAllowance.id == old)) == 0
        assert await db.scalar(select(BsTrafficAllowance.remaining_bytes).where(BsTrafficAllowance.id == new)) == 200
        assert await db.scalar(select(BsTrafficUsage.unallocated_bytes).where(BsTrafficUsage.id == pending)) == 50


async def test_full_grace_cycle_during_preparation_cannot_queue_old_snapshot(postgres_database):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import BsTrafficCollectionBudget, BsTrafficDelivery
    from tests.database.test_bs_traffic_prepare_postgres import client, prepare, setup

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        configuration = api.get_system_configuration.return_value

        async def cycle_during_network():
            async with async_sessionmaker(db.bind, expire_on_commit=False)() as worker:
                store = SQLAlchemyGraceSessionStore(worker)
                session = await store.create(grace_session())
                await complete(store, session, GraceCompletionReason.PAID)
                await worker.commit()
            return configuration

        api.get_system_configuration.side_effect = cycle_during_network
        assert (await prepare(db, api, target)).status == 'stale'
        assert await db.scalar(select(BsTrafficDelivery)) is None
