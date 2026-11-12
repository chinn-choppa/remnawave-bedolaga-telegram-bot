from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from app.database.models import BsTrafficAllowance, BsTrafficGraceIntent, GraceAccessSessionModel, Subscription
from app.services.bs_traffic.grace_completion import complete_restore
from app.services.bs_traffic.grace_sender import send_restore
from tests.database.test_bs_traffic_grace_sender_postgres import TABLES, prepared
from tests.database.test_bs_traffic_policy_postgres import evidence
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def verified(db):
    keys, reader, api, remote = await prepared(db, ordinary=True)
    assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
    return keys, reader, api, remote


async def test_complete_paid_restore_preserves_grant_and_is_idempotent(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'completed'
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'completed'
        grace = await db.scalar(select(GraceAccessSessionModel))
        assert grace.state == 'completed' and grace.completion_reason == 'paid'
        assert not (await db.get(Subscription, 1)).grace_session_open
        assert (await db.scalar(select(BsTrafficAllowance))).remaining_bytes == 1000
        api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('change', ['devices', 'expiry', 'panel', 'evidence', 'managed'])
async def test_changed_conditions_do_not_close_grace(postgres_database, change):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, remote = await verified(db)
        proof = evidence()
        if change in ('devices', 'expiry', 'managed'):
            fields = {
                'devices': {'device_limit': 99},
                'expiry': {'end_date': datetime.now(UTC) - timedelta(days=1)},
                'managed': {'traffic_accounting_mode': 'managed'},
            }[change]
            await db.execute(update(Subscription).where(Subscription.id == 1).values(**fields))
            await db.commit()
        elif change == 'panel':
            remote['state'] = replace(remote['state'], hwid_device_limit=99)
        else:
            proof = replace(proof, valid_until=datetime.now(UTC) - timedelta(seconds=1))
        result = await complete_restore(reader, **keys, evidence=proof)
        assert result in ('obsolete', 'unconfirmed', 'accounting_pending')
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'
        assert (await db.get(Subscription, 1)).grace_session_open
        api.restore_split_user_once.assert_awaited_once()


async def test_admin_change_during_readback_is_rechecked(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, remote = await verified(db)

        async def read(_):
            async with reader._factory() as worker, worker.begin():
                sub = await worker.scalar(select(Subscription).where(Subscription.id == 1).with_for_update(nowait=True))
                sub.device_limit = 77
            return remote['state']

        api.get_split_grace_snapshot.side_effect = read
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'obsolete'
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'


async def test_pending_intent_cannot_complete(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'pending'
        api.get_split_grace_snapshot.assert_not_awaited()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'pending'


async def test_new_grant_invalidates_old_verification_without_debit(postgres_database):
    from app.services.bs_traffic.ledger import grant

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        await grant(
            db,
            user_id=1,
            subscription_id=1,
            account_generation=keys['account_generation'],
            group='bs',
            operation_key='after-verify',
            amount_bytes=500,
            starts_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(days=1),
        )
        await db.commit()
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'policy_changed'
        assert sorted((await db.scalars(select(BsTrafficAllowance.remaining_bytes))).all()) == [500, 1000]
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'


async def test_foreign_owner_cannot_complete(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        api.get_split_grace_snapshot.reset_mock()
        assert await complete_restore(reader, **(keys | {'user_id': 2}), evidence=evidence()) == 'obsolete'
        api.get_split_grace_snapshot.assert_not_awaited()
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'


async def test_cancelled_read_does_not_close_grace(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        api.get_split_grace_snapshot.side_effect = asyncio.CancelledError
        with pytest.raises(asyncio.CancelledError):
            await complete_restore(reader, **keys, evidence=evidence())
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'


async def test_completion_transaction_rolls_back_marker_and_session(postgres_database, monkeypatch):
    from app.services import grace_access_runtime

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        original = grace_access_runtime._mark_subscription_grace_open

        async def failing(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError('synthetic transaction failure')

        monkeypatch.setattr(grace_access_runtime, '_mark_subscription_grace_open', failing)
        with pytest.raises(RuntimeError, match='synthetic'):
            await complete_restore(reader, **keys, evidence=evidence())
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'
        assert (await db.get(Subscription, 1)).grace_session_open


async def test_concurrent_completion_changes_version_only_once(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        version = await db.scalar(select(GraceAccessSessionModel.version))
        await db.rollback()
        results = await asyncio.gather(*(complete_restore(reader, **keys, evidence=evidence()) for _ in range(2)))
        assert 'completed' in results
        assert set(results) <= {'completed', 'obsolete'}
        assert (await db.scalar(select(GraceAccessSessionModel.version))) == version + 1
        assert not (await db.get(Subscription, 1)).grace_session_open


async def test_evidence_expiring_during_policy_build_blocks_completion(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic import grace_completion

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await verified(db)
        proof = replace(evidence(), valid_until=datetime.now(UTC) + timedelta(seconds=1))
        original = grace_completion.SQLAlchemyGraceBillingGateway.get_subscription

        async def delayed(self, subscription_id):
            result = await original(self, subscription_id)
            await asyncio.sleep(max(0, (proof.valid_until - datetime.now(UTC)).total_seconds()) + 0.01)
            return result

        monkeypatch.setattr(grace_completion.SQLAlchemyGraceBillingGateway, 'get_subscription', delayed)
        assert await complete_restore(reader, **keys, evidence=proof) == 'obsolete'
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'
