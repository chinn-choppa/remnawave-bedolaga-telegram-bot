from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from app.database.models import Subscription, Tariff, User
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.purchase import purchase_package, quote_package
from tests.database.test_bs_traffic_purchase_postgres import TABLES, setup, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def quote_args(args):
    return {k: v for k, v in args.items() if k not in ('operation_key', 'expected_price_kopeks')}


async def test_quote_matches_purchase_without_writing_money_or_grants(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        before = await totals(db)
        quote = await quote_package(db, **quote_args(args))
        assert (quote.group, quote.gb, quote.period_days, quote.price_kopeks) == ('bs', 10, 30, 2500)
        assert quote.balance_kopeks == 10000 and quote.missing_kopeks == 0
        assert await totals(db) == before
        paid = await purchase_package(db, **(args | {'expected_price_kopeks': quote.price_kopeks}))
        assert paid.price_kopeks == quote.price_kopeks


async def test_quote_shows_missing_balance_but_does_not_allow_unfunded_purchase(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        (await db.get(User, 1)).balance_kopeks = 100
        await db.commit()
        quote = await quote_package(db, **quote_args(args))
        assert quote.missing_kopeks == 2400
        with pytest.raises(LedgerConflict, match='Insufficient balance'):
            await purchase_package(db, **args)
        assert await totals(db) == (100, 0, 0, 0)


async def test_price_change_after_quote_requires_confirmation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        quote = await quote_package(db, **quote_args(args))
        tariff = await db.get(Tariff, (await db.get(Subscription, 1)).tariff_id)
        tariff.bs_traffic_settings = {**tariff.bs_traffic_settings, 'topup_packages': {'10': 3000}}
        await db.commit()
        with pytest.raises(LedgerConflict, match='price changed'):
            await purchase_package(db, **(args | {'expected_price_kopeks': quote.price_kopeks}))
        assert await totals(db) == (10000, 0, 0, 0)
        assert (await quote_package(db, **quote_args(args))).price_kopeks == 3000


@pytest.mark.parametrize('change', [{'user_id': 2}, {'account_generation': 'b' * 64}, {'group': 'bad'}, {'gb': True}])
async def test_quote_rejects_invalid_target_or_terms(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        with pytest.raises(LedgerConflict):
            await quote_package(db, **(quote_args(args) | change))
        assert await totals(db) == (10000, 0, 0, 0)


async def test_expired_topology_cannot_offer_a_purchase(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args = await setup(db, monkeypatch)
        args['evidence'] = replace(args['evidence'], valid_until=datetime.now(UTC) - timedelta(seconds=1))
        with pytest.raises(LedgerConflict):
            await quote_package(db, **quote_args(args))


@pytest.mark.parametrize('percent,price', [(20, 2000), (99, 100), (100, 0)])
async def test_quote_discount_and_minimum_match_checkout(postgres_database, monkeypatch, percent, price):
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
        quote = await quote_package(db, **quote_args(args))
        assert quote.discount_percent == percent and quote.price_kopeks == price
        assert quote.base_price_kopeks == 2500
        result = await purchase_package(db, **(args | {'expected_price_kopeks': quote.price_kopeks}))
        assert result.price_kopeks == price
