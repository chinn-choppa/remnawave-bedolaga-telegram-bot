"""Unfinished squad delivery cannot be displaced by another paid period."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficLedger,
    BsTrafficRenewalIntent,
    Subscription,
    Transaction,
    User,
)
from app.services.bs_traffic import tariff_switch_checkout as checkout
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.renewal import renew_from_balance
from tests.database.test_bs_tariff_switch_topology_postgres import request, topology_setup
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def snapshot(db):
    result = []
    for model in (User, Subscription, BsTrafficAllowance, BsTrafficRenewalIntent, Transaction):
        result.append(tuple((await db.execute(select(*model.__table__.columns).order_by(model.id))).all()))
    return tuple(result)


async def pending_switch(db, monkeypatch):
    reader, owner, sub, tariff, _ = await topology_setup(db, monkeypatch)
    monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='unconfirmed'))
    paid = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
    assert paid.delivery_pending
    db.expire_all()
    row = await db.get(BsTrafficRenewalIntent, paid.intent_id)
    assert row.state == 'pending'
    return reader, owner, row


@pytest.mark.parametrize('state', ['pending', 'attempted'])
async def test_unresolved_topology_prevents_successor_payment(postgres_database, monkeypatch, state):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, row = await pending_switch(db, monkeypatch)
        if state == 'attempted':
            from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

            operation_id = row.id
            await db.commit()
            reader._api.restore_split_user_once.side_effect = TimeoutError
            assert await advance_renewal_delivery(reader, **owner, operation_id=operation_id) == 'unconfirmed'
            db.expire_all()
            assert (await db.get(BsTrafficRenewalIntent, operation_id)).state == 'attempted'
        await db.commit()
        before = await snapshot(db)
        with pytest.raises(LedgerConflict):
            await renew_from_balance(db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='successor')
        # A caller may commit unrelated work after catching the conflict.
        await db.commit()
        assert await snapshot(db) == before


async def test_verified_topology_allows_normal_renewal(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, row = await pending_switch(db, monkeypatch)
        operation_id = row.id
        await db.commit()
        assert await advance_renewal_delivery(reader, **owner, operation_id=operation_id) == 'verified'
        db.expire_all()
        sub = await db.get(Subscription, owner['subscription_id'])
        old_end = sub.end_date
        user = await db.get(User, owner['user_id'])
        balance = user.balance_kopeks
        result = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='after-verification'
        )
        await db.commit()
        assert result.charged_kopeks == 4000 and not result.replayed
        assert user.balance_kopeks == balance - 4000
        assert sub.end_date == old_end + timedelta(days=30)
        replay = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='after-verification'
        )
        assert replay.replayed and replay.intent_id == result.intent_id


@pytest.mark.parametrize('marker', [None, True, '1', 0, 999999])
async def test_invalid_or_missing_pointed_receipt_never_funds(postgres_database, monkeypatch, marker):
    from tests.database.test_bs_paid_renewal_postgres import prepared

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _ = await prepared(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, owner['subscription_id'])
        ledger.access_policy = dict(ledger.access_policy, topology_transition_id=marker)
        await db.commit()
        before = await snapshot(db)
        with pytest.raises(LedgerConflict):
            await renew_from_balance(db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='invalid')
        await db.commit()
        assert await snapshot(db) == before


async def test_daily_resume_uses_same_precharge_guard(postgres_database, monkeypatch):
    from uuid import uuid4

    from app.services.bs_traffic.daily_control import quote_daily_resume, resume_daily_from_balance
    from tests.database.test_bs_daily_renewal_postgres import prepared

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _ = await prepared(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, owner['subscription_id'])
        ledger.access_policy = dict(ledger.access_policy, topology_transition_id=999999)
        await db.commit()
        quote = await quote_daily_resume(db, **owner)
        before = await snapshot(db)
        with pytest.raises(LedgerConflict):
            await resume_daily_from_balance(
                db,
                **owner,
                operation_key=str(uuid4()),
                expected_price_kopeks=quote.price_kopeks,
                expected_terms_hash=quote.terms_hash,
            )
        await db.commit()
        assert await snapshot(db) == before


async def test_already_collected_provider_payment_is_credited_on_transition_conflict(postgres_database, monkeypatch):
    from types import SimpleNamespace

    from app.database.models import PaymentMethod
    from app.services.bs_traffic.provider_renewal import fulfill_or_credit_recurrent

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _ = await pending_switch(db, monkeypatch)
        sub = await db.get(Subscription, owner['subscription_id'])
        user = await db.get(User, owner['user_id'])
        old_end, old_balance = sub.end_date, user.balance_kopeks
        old_receipts = tuple((await db.scalars(select(BsTrafficRenewalIntent.id))).all())
        record = SimpleNamespace(currency='RUB', amount_kopeks=4000, user_id=user.id, charge_days=30, status='ACTIVE')
        renewal, transaction = await fulfill_or_credit_recurrent(
            db, record=record, subscription=sub, payment_method=PaymentMethod.PLATEGA, charge_id='synthetic-collected'
        )
        await db.commit()
        await db.refresh(user)
        assert renewal is None
        assert user.balance_kopeks == old_balance + 4000
        assert sub.end_date == old_end
        assert tuple((await db.scalars(select(BsTrafficRenewalIntent.id))).all()) == old_receipts
        assert transaction.type == 'deposit' and transaction.amount_kopeks == 4000
        assert transaction.external_id == 'synthetic-collected'
        assert record.status == 'CANCELLED'


async def test_ordinary_pending_renewal_without_transition_keeps_existing_behavior(postgres_database, monkeypatch):
    from tests.database.test_bs_paid_renewal_postgres import prepared

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        first = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='ordinary-first'
        )
        await db.commit()
        assert (await db.get(BsTrafficRenewalIntent, first.intent_id)).state == 'pending'
        second = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='ordinary-second'
        )
        await db.commit()
        assert second.intent_id != first.intent_id
        assert sub.end_date == old_end + timedelta(days=60)
        assert (await db.get(User, owner['user_id'])).balance_kopeks == 6000


@pytest.mark.parametrize('empty_source', [False, True])
async def test_expired_unsent_transition_without_grace_renews_and_delivers_once(
    postgres_database, monkeypatch, empty_source
):
    from dataclasses import replace

    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
    from tests.database.test_bs_tariff_switch_topology_postgres import NEW_BS, NEW_ORDINARY
    from tests.database.test_bs_topology_grace_postgres import expired_transition

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, remote, old, _ = await expired_transition(db, monkeypatch)
        if empty_source:
            remote['state'] = replace(remote['state'], active_internal_squads=[])
        balance = (await db.get(User, owner['user_id'])).balance_kopeks
        paid = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='expired-without-grace'
        )
        await db.commit()
        assert (await db.get(BsTrafficRenewalIntent, old.intent_id)).state == 'superseded'
        assert (await db.get(BsTrafficLedger, owner['subscription_id'])).access_policy[
            'topology_transition_id'
        ] == paid.intent_id
        assert (await db.get(User, owner['user_id'])).balance_kopeks == balance - 4000
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) == 'verified'
        assert {s['uuid'] for s in remote['state'].active_internal_squads} == {NEW_ORDINARY, NEW_BS}
        replay = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='expired-without-grace'
        )
        assert replay.replayed and replay.intent_id == paid.intent_id
        assert await advance_renewal_delivery(reader, **owner, operation_id=old.intent_id) == 'superseded'
        assert (await db.get(User, owner['user_id'])).balance_kopeks == balance - 4000
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_repeated_expired_unsent_successors_keep_flat_removal_proof(postgres_database, monkeypatch):
    from sqlalchemy import func

    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
    from tests.database.test_bs_topology_grace_postgres import expired_transition

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, _, old, _ = await expired_transition(db, monkeypatch)
        # Fund enough for the second synthetic paid period, independently of
        # the fixture's tariff-switch price rounding.
        user = await db.get(User, owner['user_id'])
        user.balance_kopeks = 20000
        await db.commit()
        first = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='chain-one'
        )
        await db.commit()
        row = await db.get(BsTrafficRenewalIntent, first.intent_id)
        original_manifest = row.parameters['topology_transition']['manifest']
        now = await db.scalar(select(func.clock_timestamp()))
        end = now - timedelta(minutes=1)
        row.intended_expires_at = end
        sub = await db.get(Subscription, owner['subscription_id'])
        sub.end_date, sub.status = end, 'expired'
        await db.commit()
        second = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='chain-two'
        )
        await db.commit()
        successor = await db.get(BsTrafficRenewalIntent, second.intent_id)
        assert successor.parameters['topology_transition']['manifest'] == original_manifest
        assert successor.parameters['topology_transition']['predecessor_id'] == first.intent_id
        assert (await db.get(BsTrafficRenewalIntent, old.intent_id)).state == 'superseded'
        assert (await db.get(BsTrafficRenewalIntent, first.intent_id)).state == 'superseded'
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'verified'
        assert (await db.get(User, owner['user_id'])).balance_kopeks == 12000
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize(
    'selection', [('88888888-0000-4000-8000-000000000001',), ('99999999-0000-4000-8000-000000000009',)]
)
async def test_inherited_transition_does_not_overwrite_partial_or_unknown_access(
    postgres_database, monkeypatch, selection
):
    from dataclasses import replace

    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
    from tests.database.test_bs_topology_grace_postgres import expired_transition

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, remote, _, _ = await expired_transition(db, monkeypatch)
        paid = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='inherited-unknown-source'
        )
        await db.commit()
        remote['state'] = replace(remote['state'], active_internal_squads=[{'uuid': value} for value in selection])
        before_remote = remote['state']
        before_db = await snapshot(db)
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) in (
            'obsolete',
            'unconfirmed',
        )
        assert remote['state'] == before_remote
        reader._api.restore_split_user_once.assert_not_awaited()
        replay = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='inherited-unknown-source'
        )
        assert replay.replayed and replay.intent_id == paid.intent_id
        with pytest.raises(LedgerConflict):
            await renew_from_balance(
                db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='second-active-payment'
            )
        await db.commit()
        assert await snapshot(db) == before_db


async def test_expired_successor_failure_rolls_back_payment_and_supersession(postgres_database, monkeypatch):
    from tests.database.test_bs_topology_grace_postgres import expired_transition

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _, _, switched, _ = await expired_transition(db, monkeypatch)
        old = await db.get(BsTrafficRenewalIntent, switched.intent_id)
        ledger = await db.get(BsTrafficLedger, owner['subscription_id'])
        old_policy = dict(ledger.access_policy)
        before = await snapshot(db)
        original_flush = db.flush

        async def fail_after_supersession(*args, **kwargs):
            await original_flush(*args, **kwargs)
            if old.state == 'superseded':
                raise RuntimeError('synthetic final persistence failure')

        with monkeypatch.context() as patch:
            patch.setattr(db, 'flush', fail_after_supersession)
            with pytest.raises(RuntimeError, match='synthetic final persistence failure'):
                await renew_from_balance(
                    db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='rollback-successor'
                )
        await db.commit()
        db.expire_all()
        assert await snapshot(db) == before
        assert (await db.get(BsTrafficLedger, owner['subscription_id'])).access_policy == old_policy


async def test_expired_stopped_unlimited_bs_inherits_only_current_ordinary_access(postgres_database, monkeypatch):
    from dataclasses import replace

    from sqlalchemy import func

    from app.database.crud.tariff import update_tariff
    from app.database.models import Tariff
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from app.services.bs_traffic.policy import configure_groups
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
    from tests.database import test_bs_topology_grace_postgres as grace_fixture
    from tests.database.test_bs_tariff_switch_topology_postgres import NEW_ORDINARY
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    original_setup = grace_fixture.topology_setup

    async def unlimited_setup(db, patch):
        reader, owner, sub, tariff, remote = await original_setup(db, patch)
        old = await db.get(Tariff, sub.tariff_id)
        for item in (old, tariff):
            item.bs_traffic_settings = dict(item.bs_traffic_settings, limit_gb=0, topup_enabled=False)
        ledger = await db.get(BsTrafficLedger, sub.id)
        evidence = await collect_topology_evidence(
            reader, panel_identity=reader.panel_scope, ordinary_squads=(ORDINARY,), bs_squads=(BS,)
        )
        await configure_groups(
            db,
            **owner,
            ordinary_mode='finite',
            bs_mode='unlimited',
            evidence=evidence,
            expected_revision=ledger.revision,
        )
        remote['state'] = replace(remote['state'], active_internal_squads=[{'uuid': ORDINARY}, {'uuid': BS}])
        await db.commit()
        return reader, owner, sub, tariff, remote

    monkeypatch.setattr(grace_fixture, 'topology_setup', unlimited_setup)
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, remote, _, _ = await grace_fixture.expired_transition(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        for allowance in await db.scalars(select(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'bs')):
            allowance.starts_at = now - timedelta(days=2)
            allowance.expires_at = now - timedelta(days=1)
        await db.refresh(tariff)
        await update_tariff(db, tariff, bs_traffic_settings=None)
        paid = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='expired-stopped-bs'
        )
        await db.commit()
        assert set(paid.grants) == {'ordinary'}
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) == 'verified'
        assert {s['uuid'] for s in remote['state'].active_internal_squads} == {NEW_ORDINARY}
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('expired', [False, True])
async def test_renewal_preflight_blocks_active_transition_and_preserves_expired_options(
    postgres_database, monkeypatch, expired
):
    from app.services.bs_traffic.renewal import prepare_renewal_topology
    from tests.database.test_bs_topology_grace_postgres import expired_transition

    async with postgres_session(postgres_database, TABLES) as db:
        if expired:
            _, owner, _, _, _, _, _ = await expired_transition(db, monkeypatch)
        else:
            _, owner, _ = await pending_switch(db, monkeypatch)
        before = await snapshot(db)
        policy = dict((await db.get(BsTrafficLedger, owner['subscription_id'])).access_policy)
        if expired:
            await prepare_renewal_topology(db, **owner)
        else:
            with pytest.raises(LedgerConflict):
                await prepare_renewal_topology(db, **owner)
        await db.commit()
        assert await snapshot(db) == before
        assert (await db.get(BsTrafficLedger, owner['subscription_id'])).access_policy == policy


@pytest.mark.parametrize('same_operation', [False, True])
async def test_concurrent_expired_successors_cannot_bypass_pending_guard(
    postgres_database, monkeypatch, same_operation
):
    import asyncio

    from sqlalchemy import func
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from tests.database.test_bs_topology_grace_postgres import expired_transition

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _, _, switched, _ = await expired_transition(db, monkeypatch)
        (await db.get(User, owner['user_id'])).balance_kopeks = 20000
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def pay(key):
            async with factory() as session:
                try:
                    result = await renew_from_balance(
                        session, **owner, period_days=30, expected_price_kopeks=4000, operation_key=key
                    )
                    await session.commit()
                    return result
                except LedgerConflict:
                    await session.rollback()
                    return None

        outcomes = await asyncio.gather(pay('concurrent-a'), pay('concurrent-a' if same_operation else 'concurrent-b'))
        successes = [result for result in outcomes if result is not None]
        assert len(successes) == (2 if same_operation else 1)
        assert sum(not result.replayed for result in successes) == 1
        assert len({result.intent_id for result in successes}) == 1
        db.expire_all()
        assert (await db.get(User, owner['user_id'])).balance_kopeks == 16000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 2
        assert (await db.get(BsTrafficRenewalIntent, switched.intent_id)).state == 'superseded'
        ledger = await db.get(BsTrafficLedger, owner['subscription_id'])
        assert ledger.access_policy['topology_transition_id'] == successes[0].intent_id
