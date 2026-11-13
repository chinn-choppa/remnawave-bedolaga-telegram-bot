import asyncio
from importlib import import_module, util
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.database.models import Subscription, Transaction, User
from tests.database.test_bs_tariff_switch_postgres import setup_switch
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('omitted', [False, True])
async def test_checkout_collects_new_finite_baseline_before_debit(postgres_database, monkeypatch, omitted):
    from app.config import settings
    from tests.database.test_bs_tariff_switch_modes_postgres import unlimited_source

    checkout = import_module('app.services.bs_traffic.tariff_switch_checkout')
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, ledger, node, _ = await unlimited_source(db, monkeypatch)
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
        payload = (
            {'nodes': []}
            if omitted
            else {'nodes': [{'uuid': node, 'users': [{'id': ledger.panel_user_id, 'totalBytes': 5000}]}]}
        )
        reader._api.get_daily_nodes_usage = AsyncMock(return_value=payload)
        await db.commit()
        quote = await checkout.prepare_tariff_switch(
            reader, user_id=target['user_id'], subscription_id=sub.id, new_tariff_id=new.id
        )
        args = dict(
            user_id=target['user_id'],
            subscription_id=sub.id,
            new_tariff_id=new.id,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        paid = await checkout.checkout_tariff_switch(reader, **args)
        replay = await checkout.checkout_tariff_switch(reader, **args)
        assert replay.replayed and replay.transaction_id == paid.transaction_id
        reader._api.get_daily_nodes_usage.assert_awaited_once()
        db.expire_all()
        assert (await db.get(User, args['user_id'])).balance_kopeks == 10000 - quote.price_kopeks
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('fault', ['missing', 'timeout', 'changed'])
async def test_missing_external_squad_rejected_before_payment(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.tariff_switch import quote_tariff_switch

    checkout = import_module('app.services.bs_traffic.tariff_switch_checkout')
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        new.external_squad_uuid = str(uuid4())
        await db.commit()
        quote = await quote_tariff_switch(db, **target, new_tariff_id=new.id, evidence=evidence)
        args = dict(
            user_id=sub.user_id,
            subscription_id=sub.id,
            new_tariff_id=new.id,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        await db.commit()
        if fault == 'changed':
            new.external_squad_uuid = str(uuid4())
            await db.commit()
        reader._api.split_external_squad_exists = AsyncMock(
            return_value=fault != 'missing', side_effect=TimeoutError if fault == 'timeout' else None
        )
        with pytest.raises(TimeoutError if fault == 'timeout' else LedgerConflict):
            await checkout.checkout_tariff_switch(reader, **args)
        db.expire_all()
        assert (await db.get(User, args['user_id'])).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_paid_switch_timeout_replays_offline_without_second_charge(postgres_database, monkeypatch):
    assert util.find_spec('app.services.bs_traffic.tariff_switch_checkout') is not None
    checkout = import_module('app.services.bs_traffic.tariff_switch_checkout')
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, _ = await setup_switch(db, monkeypatch)
        await db.commit()
        quote = await checkout.prepare_tariff_switch(
            reader, user_id=target['user_id'], subscription_id=sub.id, new_tariff_id=new.id
        )
        args = dict(
            user_id=target['user_id'],
            subscription_id=sub.id,
            new_tariff_id=new.id,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        delivery = AsyncMock(side_effect=TimeoutError)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', delivery)
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        result = await checkout.checkout_tariff_switch(reader, **args)
        assert result.delivery_pending
        reader._api.get_internal_squad_topology.side_effect = AssertionError('Replay contacted panel')
        repeat = await checkout.checkout_tariff_switch(reader, **args)
        assert repeat.replayed and repeat.transaction_id == result.transaction_id
        assert delivery.await_count == 1
        db.expire_all()
        assert (await db.get(User, target['user_id'])).balance_kopeks == 10000 - quote.price_kopeks
        assert (await db.get(Subscription, target['subscription_id'])).tariff_id == args['new_tariff_id']
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('cancel_at', ['delivery', 'effects'])
async def test_cancelled_paid_switch_is_recoverable_offline(postgres_database, monkeypatch, cancel_at):
    checkout = import_module('app.services.bs_traffic.tariff_switch_checkout')
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, _ = await setup_switch(db, monkeypatch)
        await db.commit()
        quote = await checkout.prepare_tariff_switch(
            reader, user_id=target['user_id'], subscription_id=sub.id, new_tariff_id=new.id
        )
        args = dict(
            user_id=target['user_id'],
            subscription_id=sub.id,
            new_tariff_id=new.id,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        delivery = AsyncMock(
            return_value='pending', side_effect=asyncio.CancelledError if cancel_at == 'delivery' else None
        )
        effects = AsyncMock(side_effect=asyncio.CancelledError if cancel_at == 'effects' else None)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', delivery)
        monkeypatch.setattr(checkout, '_emit_effects', effects)
        with pytest.raises(asyncio.CancelledError):
            await checkout.checkout_tariff_switch(reader, **args)
        reader._api.get_internal_squad_topology.side_effect = AssertionError('Replay contacted panel')
        result = await checkout.checkout_tariff_switch(reader, **args)
        assert result.replayed and result.delivery_pending
        assert delivery.await_count == 1
        assert effects.await_count == (cancel_at == 'effects')
        db.expire_all()
        assert (await db.get(User, args['user_id'])).balance_kopeks == 10000 - quote.price_kopeks
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
