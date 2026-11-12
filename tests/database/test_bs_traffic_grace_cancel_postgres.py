from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficGraceIntent
from app.services.bs_traffic.grace_intent import _decode, cancel_pending_restore, claim_restore, stage_restore
from app.services.bs_traffic.grace_sender import send_restore
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_grace_sender_postgres import TABLES, prepared
from tests.database.test_bs_traffic_policy_postgres import evidence
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def restage(db, keys):
    row = await db.get(BsTrafficGraceIntent, keys['operation_id'], populate_existing=True)
    return await stage_restore(
        db,
        user_id=keys['user_id'],
        subscription_id=keys['subscription_id'],
        account_generation=keys['account_generation'],
        grace_id=row.grace_id,
        grace_version=row.grace_version,
        revision=row.ledger_revision,
        target=_decode(row.target, row.target_hash),
        expected_source=_decode(row.expected_source, row.source_hash, paid=False),
        valid_until=datetime.now(UTC) + timedelta(seconds=30),
    )


async def test_explicit_preflight_abort_can_restage_without_losing_history(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        stale = replace(evidence(), valid_until=datetime.now(UTC) - timedelta(seconds=1))
        assert await send_restore(reader, **keys, evidence=stale) == 'obsolete'
        api.restore_split_user_once.assert_not_awaited()
        row = await db.get(BsTrafficGraceIntent, keys['operation_id'])
        assert row.state == 'cancelled' and row.attempt_token is not None
        new_id = await restage(db, keys)
        await db.commit()
        assert new_id != keys['operation_id']
        assert await send_restore(reader, **keys, evidence=evidence()) == 'cancelled'
        assert await send_restore(reader, **(keys | {'operation_id': new_id}), evidence=evidence()) == 'verified'
        api.restore_split_user_once.assert_awaited_once()
        assert len((await db.scalars(select(BsTrafficGraceIntent))).all()) == 2


async def test_pending_cancellation_never_revives_old_claim(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await cancel_pending_restore(db, **keys)
        await db.commit()
        new_id = await restage(db, keys)
        await db.commit()
        assert new_id != keys['operation_id']
        assert await claim_restore(db, **keys) is None
        await db.rollback()
        assert await send_restore(reader, **(keys | {'operation_id': new_id}), evidence=evidence()) == 'verified'


async def test_unknown_attempt_cannot_be_cancelled_or_replaced(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        api.restore_split_user_once.side_effect = TimeoutError
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        assert not await cancel_pending_restore(db, **keys)
        assert await restage(db, keys) == keys['operation_id']
        await db.commit()
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        api.restore_split_user_once.assert_awaited_once()


async def test_pending_cancel_rolls_back(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await cancel_pending_restore(db, **keys)
        await db.rollback()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'pending'


async def test_foreign_owner_cannot_cancel(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        with pytest.raises(LedgerConflict):
            await cancel_pending_restore(db, **(keys | {'user_id': 2}))


async def test_claim_and_pending_cancel_have_one_winner(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)

        async def run(function):
            async with reader._factory() as worker, worker.begin():
                return await function(worker, **keys)

        cancelled, claim = await asyncio.gather(run(cancel_pending_restore), run(claim_restore))
        assert (cancelled and claim is None) or (not cancelled and claim is not None)
        state = (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state
        assert state == ('cancelled' if cancelled else 'attempted')


async def test_concurrent_reprepare_creates_only_one_new_intent(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await cancel_pending_restore(db, **keys)
        await db.commit()

        async def stage():
            async with reader._factory() as worker, worker.begin():
                return await restage(worker, keys)

        first, second = await asyncio.gather(stage(), stage())
        assert first == second and first != keys['operation_id']
        rows = (await db.scalars(select(BsTrafficGraceIntent))).all()
        assert sorted(row.state for row in rows) == ['cancelled', 'pending']


async def test_exception_before_patch_does_not_assert_unsent_proof(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        api.get_split_grace_snapshot.side_effect = ValueError('synthetic malformed read')
        with pytest.raises(ValueError):
            await send_restore(reader, **keys, evidence=evidence())
        api.restore_split_user_once.assert_not_awaited()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'
        assert not await cancel_pending_restore(db, **keys)
