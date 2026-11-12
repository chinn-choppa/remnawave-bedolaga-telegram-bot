"""Split checkout commits money, group allowance and durable delivery together."""

from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    Base,
    BsTrafficAllowance,
    BsTrafficDelivery,
    BsTrafficLedger,
    Subscription,
    Tariff,
    Transaction,
    User,
)
from app.services.bs_traffic import cutover_activation
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.policy import configure_groups
from app.services.bs_traffic.purchase import purchase_package
from tests.database.test_bs_traffic_cutover_activation_postgres import ready
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = list(Base.metadata.sorted_tables)


async def setup(db, monkeypatch, *, with_reader=False):
    reader, keys, plan, operation = await ready(db, monkeypatch)
    await db.commit()
    assert await cutover_activation.activate_cutover(reader, **keys, operation_id=operation) == 'activated'
    await db.rollback()
    state = await db.get(BsTrafficLedger, 1)
    await db.refresh(state)
    await configure_groups(
        db, **keys, ordinary_mode='finite', bs_mode='finite', evidence=plan.evidence, expected_revision=state.revision
    )
    sub = await db.get(Subscription, 1)
    sub.is_trial = False
    item = await db.get(Tariff, sub.tariff_id)
    item.allowed_squads = [ORDINARY]
    item.traffic_limit_gb = 10
    item.bs_traffic_settings = {
        'squad_uuids': [BS],
        'limit_gb': 5,
        'topup_enabled': True,
        'topup_packages': {'10': 2500},
        'max_topup_gb': 20,
    }
    user = await db.get(User, 1)
    user.balance_kopeks = 10000
    await db.commit()
    monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: True)
    args = dict(
        **keys,
        group='bs',
        gb=10,
        expected_price_kopeks=2500,
        operation_key='checkout-1',
        evidence=plan.evidence,
        current_squads=(ORDINARY,),
    )
    return (args, reader) if with_reader else args


async def totals(db):
    return (
        (await db.get(User, 1)).balance_kopeks,
        await db.scalar(select(func.count()).select_from(Transaction)),
        await db.scalar(
            select(func.count()).select_from(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'bs')
        ),
        await db.scalar(select(func.count()).select_from(BsTrafficDelivery)),
    )


async def test_purchase_and_replay_are_one_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        result = await purchase_package(db, **args)
        await db.commit()
        assert await totals(db) == (7500, 1, 1, 1)
        grant = await db.get(BsTrafficAllowance, result.grant_id)
        assert grant.remaining_bytes == 10 * 1024**3
        assert grant.expires_at - grant.starts_at == timedelta(days=30)
        delivery = await db.get(BsTrafficDelivery, result.delivery_id)
        assert set(delivery.squads) == {ORDINARY, BS}
        again = await purchase_package(db, **args)
        assert again.replayed and again.transaction_id == result.transaction_id
        assert await totals(db) == (7500, 1, 1, 1)


@pytest.mark.parametrize(
    'change', [{'expected_price_kopeks': 2400}, {'gb': 11}, {'user_id': 2}, {'account_generation': 'b' * 64}]
)
async def test_invalid_purchase_does_not_debit(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        with pytest.raises(LedgerConflict):
            await purchase_package(db, **(args | change))
        assert await totals(db) == (10000, 0, 0, 0)


async def test_delivery_failure_rolls_back_money_and_allowance(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        monkeypatch.setattr(
            'app.services.bs_traffic.purchase.enqueue',
            AsyncMock(side_effect=LedgerConflict('injected delivery failure')),
        )
        with pytest.raises(LedgerConflict):
            await purchase_package(db, **args)
        await db.commit()  # Savepoint must preserve atomicity even if caller catches failure.
        assert await totals(db) == (10000, 0, 0, 0)


async def test_outer_rollback_leaves_no_purchase(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        await purchase_package(db, **args)
        await db.rollback()
        assert await totals(db) == (10000, 0, 0, 0)


@pytest.mark.parametrize(
    'case', ['balance', 'trial', 'expired', 'blocked', 'restricted', 'grace', 'unlimited', 'squads', 'mode']
)
async def test_ineligible_purchase_keeps_balance(postgres_database, monkeypatch, case):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        sub, user = await db.get(Subscription, 1), await db.get(User, 1)
        item = await db.get(Tariff, sub.tariff_id)
        if case == 'balance':
            user.balance_kopeks = 100
        elif case == 'trial':
            sub.is_trial = True
        elif case == 'expired':
            sub.end_date = await db.scalar(select(func.clock_timestamp())) - timedelta(seconds=1)
        elif case == 'blocked':
            sub.status = 'disabled'
        elif case == 'restricted':
            user.restriction_subscription = True
        elif case == 'grace':
            sub.grace_session_open = True
        elif case == 'unlimited':
            item.bs_traffic_settings = {**item.bs_traffic_settings, 'limit_gb': 0, 'topup_enabled': False}
        elif case == 'squads':
            item.allowed_squads = []
        else:
            sub.traffic_accounting_mode = 'legacy'
        await db.commit()
        before = await totals(db)
        with pytest.raises(LedgerConflict):
            await purchase_package(db, **args)
        assert await totals(db) == before


async def test_concurrent_duplicate_purchases_charge_once(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def purchase():
            async with factory() as conn, conn.begin():
                return await purchase_package(conn, **args)

        first, second = await asyncio.gather(purchase(), purchase())
        assert first.transaction_id == second.transaction_id
        assert first.replayed != second.replayed
        assert await totals(db) == (7500, 1, 1, 1)


async def test_conflicting_replay_cannot_repurpose_paid_operation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        await purchase_package(db, **args)
        await db.commit()
        for change in ({'group': 'ordinary'}, {'gb': 20}, {'expected_price_kopeks': 1}):
            with pytest.raises(LedgerConflict):
                await purchase_package(db, **(args | change))
        assert await totals(db) == (7500, 1, 1, 1)


@pytest.mark.parametrize('pending_group', ['ordinary', 'bs'])
async def test_pending_usage_in_other_group_does_not_block_purchase(postgres_database, monkeypatch, pending_group):
    from app.services.bs_traffic.ledger import open_usage

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        target = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        await open_usage(
            db,
            **target,
            group=pending_group,
            source_key='pending',
            amount_bytes=10,
            interval_start=now - timedelta(seconds=1),
            interval_end=now,
            evidence_kind='raw_pending',
        )
        await db.commit()
        if pending_group == 'bs':
            with pytest.raises(LedgerConflict):
                await purchase_package(db, **args)
            assert await totals(db) == (10000, 0, 0, 0)
        else:
            result = await purchase_package(db, **args)
            assert set((await db.get(BsTrafficDelivery, result.delivery_id)).squads) == {ORDINARY, BS}


@pytest.mark.parametrize('case', ['stale_evidence', 'unknown_squad', 'cap'])
async def test_delivery_and_quota_guards_prevent_charge(postgres_database, monkeypatch, case):
    from dataclasses import replace

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        if case == 'stale_evidence':
            args['evidence'] = replace(
                args['evidence'], valid_until=args['evidence'].observed_at - timedelta(seconds=1)
            )
        elif case == 'unknown_squad':
            args['current_squads'] = ('99999999-9999-4999-8999-999999999999',)
        else:
            sub = await db.get(Subscription, 1)
            item = await db.get(Tariff, sub.tariff_id)
            item.bs_traffic_settings = {**item.bs_traffic_settings, 'max_topup_gb': 9}
            await db.commit()
        with pytest.raises((LedgerConflict, ValueError)):
            await purchase_package(db, **args)
        await db.commit()
        assert await totals(db) == (10000, 0, 0, 0)


async def test_unconfirmed_delivery_prevents_second_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        await purchase_package(db, **args)
        await db.commit()
        with pytest.raises(LedgerConflict, match='Previous delivery'):
            await purchase_package(db, **(args | {'operation_key': 'second'}))
        await db.commit()
        assert await totals(db) == (7500, 1, 1, 1)


async def test_successful_replay_ignores_later_price_change(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        result = await purchase_package(db, **args)
        await db.commit()
        sub = await db.get(Subscription, 1)
        item = await db.get(Tariff, sub.tariff_id)
        item.bs_traffic_settings = {**item.bs_traffic_settings, 'topup_packages': {'10': 9000}}
        await db.commit()
        again = await purchase_package(db, **args)
        assert again.replayed and again.transaction_id == result.transaction_id
        assert await totals(db) == (7500, 1, 1, 1)


async def test_known_old_overflow_is_settled_before_paid_package(postgres_database, monkeypatch):
    from app.database.models import BsTrafficUsage
    from app.services.bs_traffic.intake import attach_cursor
    from app.services.bs_traffic.ledger import grant
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_B

    async with postgres_session(postgres_database, TABLES) as db:
        args, reader = await setup(db, monkeypatch, with_reader=True)
        target = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        now = await db.scalar(select(func.clock_timestamp()))
        await grant(
            db,
            **target,
            group='bs',
            operation_key='base',
            amount_bytes=100,
            starts_at=now - timedelta(hours=1),
            expires_at=now + timedelta(days=30),
        )
        _, day = await observe(db, reader, target, node=NODE_B, amount=0)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        await observe(db, reader, target, node=NODE_B, amount=150)
        await db.commit()
        result = await purchase_package(db, **args)
        assert (await db.get(BsTrafficAllowance, result.grant_id)).remaining_bytes == 10 * 1024**3
        usage = await db.scalar(select(BsTrafficUsage).where(BsTrafficUsage.traffic_group == 'bs'))
        assert usage.charged_bytes == 100 and usage.unallocated_bytes == 50


@pytest.mark.parametrize('lost_response', [False, True])
async def test_committed_purchase_is_delivered_once(postgres_database, monkeypatch, lost_response):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.preparation import delivery_guard
    from app.services.bs_traffic.sender import send_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        args, reader = await setup(db, monkeypatch, with_reader=True)
        result = await purchase_package(db, **args)
        await db.commit()
        target = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        snapshot = reader._api.get_split_grace_snapshot.return_value
        account = SimpleNamespace(
            id=snapshot.id, created_at=snapshot.created_at, active_internal_squads=[{'uuid': ORDINARY}]
        )

        async def patch(user_id, squads):
            async with reader._factory() as check:
                assert (await check.get(User, 1)).balance_kopeks == 7500
                assert (await check.get(BsTrafficDelivery, result.delivery_id)).state == 'attempted'
            account.active_internal_squads = [{'uuid': value} for value in squads]
            if lost_response:
                raise TimeoutError('response lost after applying squads')
            return account

        api = SimpleNamespace(
            get_split_access_snapshot=AsyncMock(side_effect=lambda _: account),
            update_internal_squads_once=AsyncMock(side_effect=patch),
        )
        sending = dict(
            **target,
            operation_id=result.delivery_id,
            evidence=args['evidence'],
            current_guard_hash=delivery_guard(**target, evidence=args['evidence']),
        )
        assert await send_delivery(reader._factory, api, **sending) == 'verified'
        assert await send_delivery(reader._factory, api, **sending) == 'already_verified'
        api.update_internal_squads_once.assert_awaited_once()
        assert {item['uuid'] for item in account.active_internal_squads} == {ORDINARY, BS}
        assert await totals(db) == (7500, 1, 1, 1)


@pytest.mark.parametrize('percent,price', [(20, 2000), (99, 100), (100, 0)])
async def test_server_discount_is_part_of_atomic_price(postgres_database, monkeypatch, percent, price):
    from app.database.models import PromoGroup

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        promo = PromoGroup(
            name='synthetic', traffic_discount_percent=percent, apply_discounts_to_addons=True, is_default=False
        )
        db.add(promo)
        await db.flush()
        (await db.get(User, 1)).promo_group_id = promo.id
        await db.commit()
        result = await purchase_package(db, **(args | {'expected_price_kopeks': price}))
        assert result.price_kopeks == price
        assert await totals(db) == (10000 - price, 1, 1, 1)


async def test_ordinary_purchase_does_not_grant_bs(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        item = await db.get(Tariff, sub.tariff_id)
        item.traffic_limit_gb, item.traffic_topup_enabled = 10, True
        item.traffic_topup_packages, item.max_topup_traffic_gb = {'10': 1500}, 30
        await db.commit()
        result = await purchase_package(db, **(args | {'group': 'ordinary', 'expected_price_kopeks': 1500}))
        assert (await db.get(BsTrafficAllowance, result.grant_id)).traffic_group == 'ordinary'
        assert (await db.get(BsTrafficDelivery, result.delivery_id)).squads == [ORDINARY]
        assert await totals(db) == (8500, 1, 0, 1)


async def test_transaction_insert_failure_rolls_back_delivery_too(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        monkeypatch.setattr(
            'app.services.bs_traffic.purchase.create_transaction',
            AsyncMock(side_effect=RuntimeError('synthetic insert failure')),
        )
        with pytest.raises(RuntimeError, match='synthetic'):
            await purchase_package(db, **args)
        await db.commit()
        assert await totals(db) == (10000, 0, 0, 0)


async def test_other_group_mode_change_requires_reconciliation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        (await db.get(Tariff, sub.tariff_id)).traffic_limit_gb = 0
        await db.commit()
        with pytest.raises(LedgerConflict, match='modes'):
            await purchase_package(db, **args)
        assert await totals(db) == (10000, 0, 0, 0)


async def test_unlimited_ordinary_keeps_access_after_bs_purchase(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        target = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        state = await db.get(BsTrafficLedger, 1)
        await configure_groups(
            db,
            **target,
            ordinary_mode='unlimited',
            bs_mode='finite',
            evidence=args['evidence'],
            expected_revision=state.revision,
        )
        sub = await db.get(Subscription, 1)
        (await db.get(Tariff, sub.tariff_id)).traffic_limit_gb = 0
        await db.commit()
        result = await purchase_package(db, **args)
        assert set((await db.get(BsTrafficDelivery, result.delivery_id)).squads) == {ORDINARY, BS}
