"""A persisted attempt must precede one fenced panel write and strict readback."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficDelivery, BsTrafficLedger, User
from app.services.bs_traffic.delivery_store import enqueue
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.sender import send_delivery
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, TARGET
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY, configure, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
GUARD = 'b' * 64


def panel(squads=(ORDINARY, BS), **changes):
    args = dict(
        id=12, created_at=datetime(2026, 1, 1, tzinfo=UTC), active_internal_squads=[{'uuid': s} for s in squads]
    )
    args.update(changes)
    return SimpleNamespace(**args)


async def prepare(db, squads=(ORDINARY,)):
    await seed(db)
    generation = account_generation(panel_scope=SCOPE, account=panel())
    await db.execute(
        update(BsTrafficLedger).where(BsTrafficLedger.subscription_id == 1).values(account_generation=generation)
    )
    target = dict(TARGET, account_generation=generation)
    evidence = await configure(db, ordinary='unlimited', account_generation=generation)
    state = await db.get(BsTrafficLedger, 1)
    operation = await enqueue(
        db,
        **target,
        operation_key='send-1',
        revision=state.revision,
        guard_hash=GUARD,
        squads=squads,
        valid_until=evidence.valid_until,
    )
    await db.commit()
    return target, evidence, operation


async def run(db, api, prepared):
    target, evidence, operation = prepared
    return await send_delivery(
        async_sessionmaker(db.bind, expire_on_commit=True),
        api,
        **target,
        operation_id=operation,
        evidence=evidence,
        current_guard_hash=GUARD,
    )


async def test_claim_is_committed_before_patch_and_retry_does_not_resend(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        current = panel()

        async def write(user_id, squads):
            async with async_sessionmaker(db.bind)() as observer:
                assert (await observer.get(BsTrafficDelivery, prepared[2])).state == 'attempted'
            current.active_internal_squads = [{'uuid': s} for s in squads]
            return current

        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(side_effect=lambda _: current),
            update_internal_squads_once=AsyncMock(side_effect=write),
        )
        assert await run(db, api, prepared) == 'verified'
        assert await run(db, api, prepared) == 'already_verified'
        assert api.update_internal_squads_once.await_count == 1


async def test_changed_account_never_receives_patch(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel(id=13)), update_internal_squads_once=AsyncMock()
        )
        assert await run(db, api, prepared) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()


async def test_saved_selection_must_match_fresh_policy(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db, squads=())
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel()), update_internal_squads_once=AsyncMock()
        )
        assert await run(db, api, prepared) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()


async def test_lost_response_is_recovered_by_get_not_second_patch(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        current = panel()

        async def write(user_id, squads):
            current.active_internal_squads = [{'uuid': s} for s in squads]
            raise TimeoutError

        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(side_effect=lambda _: current),
            update_internal_squads_once=AsyncMock(side_effect=write),
        )
        assert await run(db, api, prepared) == 'verified'
        assert api.update_internal_squads_once.await_count == 1


async def test_failed_write_does_not_get_automatic_retry(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel()),
            update_internal_squads_once=AsyncMock(side_effect=TimeoutError),
        )
        assert await run(db, api, prepared) == 'attention'
        assert await run(db, api, prepared) == 'attention'
        assert api.update_internal_squads_once.await_count == 1


async def test_cancelled_send_is_not_repeated(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel()),
            update_internal_squads_once=AsyncMock(side_effect=asyncio.CancelledError),
        )
        with pytest.raises(asyncio.CancelledError):
            await run(db, api, prepared)
        assert (await db.get(BsTrafficDelivery, prepared[2])).state == 'attempted'
        await db.rollback()
        assert await run(db, api, prepared) == 'in_progress'
        assert api.update_internal_squads_once.await_count == 1


async def test_expired_topology_evidence_does_not_send(postgres_database):
    from dataclasses import replace
    from datetime import timedelta

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        target, evidence, operation = await prepare(db)
        evidence = replace(evidence, valid_until=datetime.now(UTC) - timedelta(seconds=1))
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(), update_internal_squads_once=AsyncMock())
        assert await run(db, api, (target, evidence, operation)) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()
        api.get_split_access_snapshot.assert_not_awaited()


async def test_unknown_panel_squad_is_not_silently_removed(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel(('99999999-9999-4999-8999-999999999999',))),
            update_internal_squads_once=AsyncMock(),
        )
        assert await run(db, api, prepared) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()


async def test_payment_state_mutation_waits_until_the_write_finishes(postgres_database):
    import asyncio

    from app.database.models import Subscription

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        current = panel()
        waiting = asyncio.Event()
        tasks = []

        async def change():
            async with async_sessionmaker(db.bind)() as other, other.begin():
                waiting.set()
                await other.execute(update(Subscription).where(Subscription.id == 1).values(status='disabled'))

        async def write(user_id, squads):
            task = asyncio.create_task(change())
            tasks.append(task)
            await waiting.wait()
            await asyncio.sleep(0.05)
            assert not task.done()
            current.active_internal_squads = [{'uuid': s} for s in squads]
            return current

        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(side_effect=lambda _: current),
            update_internal_squads_once=AsyncMock(side_effect=write),
        )
        try:
            assert await run(db, api, prepared) in ('verified', 'attention')
        finally:
            await asyncio.wait_for(asyncio.gather(*tasks), 5)
        api.update_internal_squads_once.assert_awaited_once()


async def test_another_sender_does_not_recover_an_active_attempt(postgres_database):
    from app.services.bs_traffic.delivery_store import claim

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        target, evidence, operation = await prepare(db)
        await claim(db, **target, operation_id=operation, current_guard_hash=GUARD)
        await db.commit()
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(), update_internal_squads_once=AsyncMock())
        assert await run(db, api, (target, evidence, operation)) == 'in_progress'
        api.get_split_access_snapshot.assert_not_awaited()


async def test_subscription_expiring_after_projection_is_not_sent(postgres_database, monkeypatch):
    import asyncio
    from datetime import timedelta

    from sqlalchemy import func, select

    from app.database.models import Subscription
    from app.services.bs_traffic import sender
    from app.services.bs_traffic.delivery_store import _local_guard

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        # Establish a valid initial operation with a near future paid deadline.
        await db.execute(
            update(Subscription).where(Subscription.id == 1).values(end_date=datetime.now(UTC) + timedelta(seconds=1))
        )
        now = await db.scalar(select(func.clock_timestamp()))
        row = await db.get(BsTrafficDelivery, prepared[2])
        row.local_guard_hash = await _local_guard(db, 1, 1, now)
        await db.commit()
        original = sender.project_access

        async def slow(*args, **kwargs):
            result = await original(*args, **kwargs)
            await asyncio.sleep(1.1)
            return result

        monkeypatch.setattr(sender, 'project_access', slow)
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel()),
            update_internal_squads_once=AsyncMock(return_value=panel()),
        )
        assert await run(db, api, prepared) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()


async def test_grant_expiring_after_projection_is_not_added(postgres_database, monkeypatch):
    import asyncio
    from datetime import timedelta

    from app.services.bs_traffic import sender
    from tests.database.test_bs_traffic_ledger_postgres import issue

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        target, evidence, operation = await prepare(db)
        now = datetime.now(UTC)
        await issue(
            db,
            account_generation=target['account_generation'],
            starts_at=now - timedelta(seconds=1),
            expires_at=now + timedelta(seconds=1),
        )
        row = await db.get(BsTrafficDelivery, operation)
        row.revision = (await db.get(BsTrafficLedger, 1)).revision
        row.squads = [ORDINARY, BS]
        await db.commit()
        original = sender.project_access

        async def slow(*args, **kwargs):
            result = await original(*args, **kwargs)
            await asyncio.sleep(1.1)
            return result

        monkeypatch.setattr(sender, 'project_access', slow)
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel((ORDINARY,))),
            update_internal_squads_once=AsyncMock(return_value=panel()),
        )
        assert await run(db, api, (target, evidence, operation)) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()


async def test_known_unsent_operation_releases_slot_and_keeps_audit(postgres_database):
    from dataclasses import replace
    from datetime import timedelta

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        target, evidence, operation = await prepare(db)
        stale = replace(evidence, valid_until=datetime.now(UTC) - timedelta(seconds=1))
        api = SimpleNamespace(get_split_access_snapshot=AsyncMock(), update_internal_squads_once=AsyncMock())
        assert await run(db, api, (target, stale, operation)) == 'not_sent'
        old = await db.get(BsTrafficDelivery, operation)
        assert old.token is not None and old.attempted_at is not None
        assert old.not_sent_reason == 'stale_evidence'
        new_id = await enqueue(
            db,
            **target,
            operation_key='send-2',
            revision=old.revision,
            guard_hash=GUARD,
            squads=(ORDINARY,),
            valid_until=evidence.valid_until,
        )
        await db.commit()
        assert new_id != operation
        assert await run(db, api, (target, evidence, operation)) == 'not_sent'
        api.update_internal_squads_once.assert_not_awaited()


async def test_ambiguous_failure_cannot_release_slot_for_another_write(postgres_database):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        prepared = await prepare(db)
        target, evidence, operation = prepared
        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(return_value=panel()),
            update_internal_squads_once=AsyncMock(side_effect=TimeoutError),
        )
        assert await run(db, api, prepared) == 'attention'
        old = await db.get(BsTrafficDelivery, operation)
        with pytest.raises(LedgerConflict):
            await enqueue(
                db,
                **target,
                operation_key='send-2',
                revision=old.revision,
                guard_hash=GUARD,
                squads=(ORDINARY,),
                valid_until=evidence.valid_until,
            )
