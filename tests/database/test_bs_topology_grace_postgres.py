"""An undelivered known-unsent topology switch does not revoke expiry grace."""

import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficRenewalIntent, GraceAccessSessionModel, Subscription
from app.services.bs_traffic import tariff_switch_checkout as checkout
from app.services.bs_traffic.grace_lifecycle import start_split_grace
from app.services.grace_access_service import GraceAccessPolicy, GraceReason, GraceSessionState
from tests.database.test_bs_tariff_switch_topology_postgres import request, topology_setup
from tests.database.test_bs_traffic_policy_postgres import ORDINARY
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
OVERLAY = '77777777-7777-4777-8777-777777777777'


async def expired_transition(db, monkeypatch, *, attempted=False):
    reader, owner, sub, tariff, remote = await topology_setup(db, monkeypatch)
    args = await request(reader, owner, tariff)
    with monkeypatch.context() as patch:
        patch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        paid = await checkout.checkout_tariff_switch(reader, **args)
    if attempted:
        from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

        reader._api.restore_split_user_once = AsyncMock(side_effect=TimeoutError())
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) == 'unconfirmed'
    db.expire_all()
    sub = await db.get(Subscription, owner['subscription_id'])
    receipt = await db.get(BsTrafficRenewalIntent, paid.intent_id)
    # Move the complete synthetic paid window into history, without changing
    # its immutable terms/hash relationship or pretending a new payment exists.
    now = await db.scalar(select(func.clock_timestamp()))
    end = now - timedelta(minutes=1)
    sub.end_date, sub.status = end, 'expired'
    receipt.intended_expires_at = end
    receipt.created_at = end - timedelta(days=1)
    terms = dict(receipt.parameters['switch_terms'], end_date=end.isoformat())
    digest = hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt.parameters = dict(receipt.parameters, switch_terms=terms, switch_terms_hash=digest)
    remote['state'] = replace(remote['state'], expire_at=end, status='EXPIRED')

    async def apply(target):
        remote['state'] = target

    reader._api.apply_split_grace_overlay_once = AsyncMock(side_effect=apply)
    reader._api.reset_user_traffic = AsyncMock()
    await db.commit()
    policy = GraceAccessPolicy(
        duration=timedelta(minutes=10),
        expired_squad_uuid=OVERLAY,
        limited_squad_uuid=OVERLAY,
        traffic_bytes=1000,
        reset_traffic_on_start=True,
    )
    return reader, owner, sub, tariff, remote, paid, policy


async def test_expired_unsent_topology_switch_preserves_grace_and_actual_devices(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _, remote, paid, policy = await expired_transition(db, monkeypatch)
        before = remote['state']
        assert before.hwid_device_limit != sub.device_limit
        result = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        assert result.session.state is GraceSessionState.ACTIVE
        assert result.session.split_before.paid_squads == (ORDINARY,)
        assert remote['state'].hwid_device_limit == before.hwid_device_limit
        assert remote['state'].used_traffic_bytes == before.used_traffic_bytes
        assert {item['uuid'] for item in remote['state'].active_internal_squads} == {OVERLAY}
        db.expire_all()
        row = await db.get(GraceAccessSessionModel, result.session.id)
        assert row.split_overlay_attempt['state'] == 'verified'
        assert (await db.get(BsTrafficRenewalIntent, paid.intent_id)).state == 'pending'
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()
        reader._api.restore_split_user_once.assert_not_awaited()
        reader._api.reset_user_traffic.assert_not_awaited()


async def test_expired_transition_grace_new_payment_restores_current_rights_once(postgres_database, monkeypatch):
    from app.database.models import BsTrafficAllowance, BsTrafficLedger, Transaction, User
    from app.services.bs_traffic.ledger import grant
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from tests.database.test_bs_tariff_switch_topology_postgres import NEW_BS, NEW_ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _, remote, switched, policy = await expired_transition(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        topup_id = await grant(
            db,
            **owner,
            group='ordinary',
            operation_key='topup:grace-transition',
            amount_bytes=123,
            starts_at=now,
            expires_at=now + timedelta(days=2),
        )
        await db.commit()
        started = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        paid = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='renew:after-topology-grace'
        )
        await db.commit()
        db.expire_all()
        old = await db.get(BsTrafficRenewalIntent, switched.intent_id)
        assert old.state == 'superseded'
        assert old.parameters['superseded_by_intent_id'] == paid.intent_id
        assert 'topology_transition_id' not in (await db.get(BsTrafficLedger, owner['subscription_id'])).access_policy
        assert await advance_paid_renewal(reader, **owner, operation_id=paid.intent_id) == 'verified'
        db.expire_all()
        assert (await db.get(GraceAccessSessionModel, started.session.id)).state == 'completed'
        assert {item['uuid'] for item in remote['state'].active_internal_squads} == {NEW_ORDINARY, NEW_BS}
        assert remote['state'].hwid_device_limit == 4
        topup = await db.get(BsTrafficAllowance, topup_id)
        assert (topup.remaining_bytes, topup.starts_at, topup.expires_at, topup.retired_at) == (
            123,
            now,
            now + timedelta(days=2),
            None,
        )
        balance = (await db.get(User, owner['user_id'])).balance_kopeks
        replay = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='renew:after-topology-grace'
        )
        assert replay.replayed and replay.intent_id == paid.intent_id
        assert (await db.get(User, owner['user_id'])).balance_kopeks == balance
        assert await advance_paid_renewal(reader, **owner, operation_id=switched.intent_id) == 'superseded'
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()
        reader._api.restore_split_user_once.assert_awaited_once()
        reader._api.reset_user_traffic.assert_not_awaited()


@pytest.mark.parametrize('fault', ['unknown_squad', 'source_topology', 'payment', 'before_save', 'attempted'])
async def test_unproved_pending_transition_never_issues_grace(postgres_database, monkeypatch, fault):
    from app.database.models import Transaction
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.grace_access_codec import GraceSnapshotError
    from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, remote, paid, policy = await expired_transition(
            db, monkeypatch, attempted=fault == 'attempted'
        )
        if fault == 'unknown_squad':
            remote['state'] = replace(remote['state'], active_internal_squads=[{'uuid': OVERLAY}])
        elif fault == 'source_topology':
            original = reader._api.get_internal_squad_topology.side_effect

            async def drift(squad):
                result = await original(squad)
                if squad == ORDINARY:
                    result = dict(
                        result, accessibleNodes=[dict(result['accessibleNodes'][0], activeInbounds=['changed'])]
                    )
                return result

            reader._api.get_internal_squad_topology.side_effect = drift
        elif fault == 'payment':
            (await db.get(Transaction, paid.transaction_id)).is_completed = False
            await db.commit()
        elif fault == 'before_save':
            original_create = SQLAlchemyGraceSessionStore.create

            async def invalidate(store, session):
                payment = await store._db.get(Transaction, paid.transaction_id)
                payment.is_completed = False
                await store._db.flush()
                return await original_create(store, session)

            monkeypatch.setattr(SQLAlchemyGraceSessionStore, 'create', invalidate)
        before = remote['state']
        restore_count = reader._api.restore_split_user_once.await_count
        with pytest.raises((LedgerConflict, GraceSnapshotError)):
            await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        assert remote['state'] == before
        reader._api.apply_split_grace_overlay_once.assert_not_awaited()
        assert reader._api.restore_split_user_once.await_count == restore_count
        assert await db.scalar(select(func.count()).select_from(GraceAccessSessionModel)) == 0


@pytest.mark.parametrize('proof', ['pending', 'unconfirmed'])
async def test_expired_transition_requires_confirmed_open_grace_before_new_charge(
    postgres_database, monkeypatch, proof
):
    from app.database.models import Transaction, User
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, _, paid, policy = await expired_transition(db, monkeypatch)
        if proof == 'pending':
            from app.services.bs_traffic import grace_lifecycle

            with monkeypatch.context() as patch:
                patch.setattr(grace_lifecycle, 'advance_split_grace', AsyncMock(return_value='deferred'))
                await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        elif proof == 'unconfirmed':
            reader._api.apply_split_grace_overlay_once = AsyncMock(side_effect=TimeoutError())
            with pytest.raises(TimeoutError):
                await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        balance = (await db.get(User, owner['user_id'])).balance_kopeks
        with pytest.raises(LedgerConflict):
            await renew_from_balance(
                db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='renew:unproved-overlay'
            )
        await db.refresh(await db.get(User, owner['user_id']))
        assert (await db.get(User, owner['user_id'])).balance_kopeks == balance
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert (await db.get(BsTrafficRenewalIntent, paid.intent_id)).state == 'pending'


@pytest.mark.parametrize('applied', [False, True])
async def test_successor_unknown_restore_is_get_only_without_second_charge(postgres_database, monkeypatch, applied):
    from app.database.models import Transaction
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, remote, switched, policy = await expired_transition(db, monkeypatch)
        started = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        paid = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='renew:unknown-successor'
        )
        await db.commit()

        async def unknown(target):
            if applied:
                remote['state'] = target
            raise TimeoutError

        reader._api.restore_split_user_once = AsyncMock(side_effect=unknown)
        expected = 'verified' if applied else 'unconfirmed'
        assert await advance_paid_renewal(reader, **owner, operation_id=paid.intent_id) == expected
        again = await advance_paid_renewal(reader, **owner, operation_id=paid.intent_id)
        assert again in ('already_verified', 'verified') if applied else again == 'unconfirmed'
        db.expire_all()
        assert (await db.get(BsTrafficRenewalIntent, switched.intent_id)).state == 'superseded'
        grace = await db.get(GraceAccessSessionModel, started.session.id)
        assert grace.state == ('completed' if applied else 'restoring')
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('fault', ['target_hash', 'generation', 'created_after_grace'])
async def test_expired_successor_rejects_invalid_verified_grace_proof(postgres_database, monkeypatch, fault):
    from app.database.models import Transaction, User
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, _, paid, policy = await expired_transition(db, monkeypatch)
        started = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        row = await db.get(GraceAccessSessionModel, started.session.id)
        if fault == 'target_hash':
            row.split_overlay_attempt = dict(row.split_overlay_attempt, target_hash='b' * 64)
        elif fault == 'generation':
            target = dict(row.split_overlay_attempt['target'], id=999)
            digest = hashlib.sha256(json.dumps(target, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            row.split_overlay_attempt = dict(row.split_overlay_attempt, target=target, target_hash=digest)
        else:
            (await db.get(BsTrafficRenewalIntent, paid.intent_id)).created_at = row.started_at + timedelta(seconds=1)
        await db.commit()
        balance = (await db.get(User, owner['user_id'])).balance_kopeks
        with pytest.raises(LedgerConflict):
            await renew_from_balance(
                db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='renew:bad-proof'
            )
        await db.refresh(await db.get(User, owner['user_id']))
        assert (await db.get(User, owner['user_id'])).balance_kopeks == balance
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_delayed_pending_grace_recollects_source_topology_before_first_write(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic import grace_lifecycle
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, _, _, policy = await expired_transition(db, monkeypatch)
        original_capture = grace_lifecycle.capture_grace_context

        async def brief_capture(*args, **kwargs):
            context = await original_capture(*args, **kwargs)
            now = await db.scalar(select(func.clock_timestamp()))
            return replace(context, valid_until=now + timedelta(seconds=2))

        with monkeypatch.context() as patch:
            patch.setattr(grace_lifecycle, 'capture_grace_context', brief_capture)
            patch.setattr(grace_lifecycle, 'advance_split_grace', AsyncMock(return_value='deferred'))
            started = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        assert started.session.state is GraceSessionState.PENDING
        await db.rollback()
        await asyncio.sleep(2.1)
        original = reader._api.get_internal_squad_topology.side_effect

        async def drift(squad):
            result = await original(squad)
            if squad == ORDINARY:
                result = dict(result, accessibleNodes=[dict(result['accessibleNodes'][0], activeInbounds=['changed'])])
            return result

        reader._api.get_internal_squad_topology.side_effect = drift
        with pytest.raises(LedgerConflict):
            await grace_lifecycle.advance_split_grace(reader, **owner)
        reader._api.apply_split_grace_overlay_once.assert_not_awaited()
        row = await db.get(GraceAccessSessionModel, started.session.id, populate_existing=True)
        assert row.split_overlay_attempt is None
        await db.rollback()
        reader._api.get_internal_squad_topology.side_effect = original
        assert await grace_lifecycle.advance_split_grace(reader, **owner) == 'activated'
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()


@pytest.mark.parametrize('applied', [False, True])
async def test_unknown_overlay_recovers_without_revalidating_a_new_source(postgres_database, monkeypatch, applied):
    from app.services.bs_traffic.grace_lifecycle import advance_split_grace

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, remote, _, policy = await expired_transition(db, monkeypatch)

        async def unknown(target):
            if applied:
                remote['state'] = target
            raise TimeoutError

        reader._api.apply_split_grace_overlay_once = AsyncMock(side_effect=unknown)
        with pytest.raises(TimeoutError):
            await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        original = reader._api.get_internal_squad_topology.side_effect

        async def unavailable_old_source(squad):
            if squad == ORDINARY:
                raise AssertionError('Unknown overlay recovery must not prepare another source or PATCH')
            return await original(squad)

        reader._api.get_internal_squad_topology.side_effect = unavailable_old_source
        result = await advance_split_grace(reader, **owner)
        assert result == ('activated' if applied else 'unconfirmed')
        reader._api.apply_split_grace_overlay_once.assert_awaited_once()
        reader._api.restore_split_user_once.assert_not_awaited()


async def test_overlay_rechecks_source_payment_after_claim_before_patch(postgres_database, monkeypatch):
    from app.database.models import Transaction
    from app.services.bs_traffic import grace_lifecycle

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, _, paid, policy = await expired_transition(db, monkeypatch)
        original_lock = grace_lifecycle._locked
        revoked = False

        async def revoke_after_claim(session, bound_reader, keys):
            nonlocal revoked
            result = await original_lock(session, bound_reader, keys)
            row = await session.scalar(select(GraceAccessSessionModel))
            if row is not None and row.split_overlay_attempt is not None and not revoked:
                assert row.split_overlay_attempt['state'] == 'attempted'
                (await session.get(Transaction, paid.transaction_id)).is_completed = False
                await session.flush()
                revoked = True
            return result

        monkeypatch.setattr(grace_lifecycle, '_locked', revoke_after_claim)
        started = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
        assert revoked
        reader._api.apply_split_grace_overlay_once.assert_not_awaited()
        row = await db.get(GraceAccessSessionModel, started.session.id, populate_existing=True)
        assert row.state == 'pending'
        assert row.split_overlay_attempt['state'] == 'cancelled'
