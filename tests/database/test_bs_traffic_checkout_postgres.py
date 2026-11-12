from unittest.mock import AsyncMock

import pytest

from app.services.bs_traffic.checkout import checkout_package
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_checkout_commits_before_panel_and_replay_never_recharges(postgres_database, monkeypatch):
    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, account = await prepared_purchase(db, monkeypatch)
        effects = AsyncMock()
        monkeypatch.setattr(checkout, '_emit_effects', effects)

        async def write(user_id, squads):
            async with reader._factory() as other:
                assert (await totals(other))[0] == 7500
            account.active_internal_squads = [{'uuid': s} for s in squads]
            return account

        reader._api.update_internal_squads_once.side_effect = write
        args = dict(
            user_id=1,
            subscription_id=target['subscription_id'],
            group='bs',
            gb=10,
            expected_price_kopeks=2500,
            operation_key='second-checkout',
        )
        first = await checkout_package(reader, **args)
        assert first.delivery_status == 'verified' and not first.purchase.replayed
        reader._api.get_split_access_snapshot = AsyncMock(side_effect=RuntimeError('offline'))
        again = await checkout_package(reader, **args)
        assert again.purchase.replayed and again.purchase.transaction_id == first.purchase.transaction_id
        assert again.delivery_status == 'verified'
        reader._api.get_split_access_snapshot.assert_not_awaited()
        assert reader._api.update_internal_squads_once.await_count == 1
        effects.assert_awaited_once()


async def test_price_change_does_not_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        before = await totals(db)
        with pytest.raises(LedgerConflict, match='price changed'):
            await checkout_package(
                reader,
                user_id=1,
                subscription_id=target['subscription_id'],
                group='bs',
                gb=10,
                expected_price_kopeks=1,
                operation_key='second-checkout',
            )
        db.expire_all()
        assert await totals(db) == before
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_delivery_failure_after_commit_returns_paid_pending(postgres_database, monkeypatch):
    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock(side_effect=RuntimeError('notification unavailable')))
        monkeypatch.setattr(
            checkout, 'advance_purchase_delivery', AsyncMock(side_effect=RuntimeError('panel unavailable'))
        )
        result = await checkout_package(
            reader,
            user_id=1,
            subscription_id=target['subscription_id'],
            group='bs',
            gb=10,
            expected_price_kopeks=2500,
            operation_key='second-checkout',
        )
        assert result.delivery_status == 'pending'
        db.expire_all()
        assert (await totals(db))[:3] == (7500, 1, 1)


async def test_concurrent_checkout_has_one_charge(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        effects = AsyncMock()
        monkeypatch.setattr(checkout, '_emit_effects', effects)
        monkeypatch.setattr(checkout, 'advance_purchase_delivery', AsyncMock(return_value='pending'))
        original = checkout._with_purchase_context
        entered = 0
        ready = asyncio.Event()

        async def together(*args, **kwargs):
            nonlocal entered
            entered += 1
            if entered == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), 5)
            return await original(*args, **kwargs)

        monkeypatch.setattr(checkout, '_with_purchase_context', together)
        args = dict(
            user_id=1,
            subscription_id=target['subscription_id'],
            group='bs',
            gb=10,
            expected_price_kopeks=2500,
            operation_key='concurrent',
        )
        first, second = await asyncio.gather(checkout_package(reader, **args), checkout_package(reader, **args))
        assert first.purchase.transaction_id == second.purchase.transaction_id
        assert first.purchase.replayed != second.purchase.replayed
        effects.assert_awaited_once()
        db.expire_all()
        assert (await totals(db))[:3] == (7500, 1, 1)


async def test_offline_new_purchase_does_not_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        reader._api.get_split_access_snapshot = AsyncMock(side_effect=RuntimeError('offline'))
        before = await totals(db)
        with pytest.raises(RuntimeError, match='offline'):
            await checkout_package(
                reader,
                user_id=1,
                subscription_id=target['subscription_id'],
                group='bs',
                gb=10,
                expected_price_kopeks=2500,
                operation_key='offline',
            )
        db.expire_all()
        assert await totals(db) == before
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_wrong_owner_cannot_quote_or_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        before = await totals(db)
        with pytest.raises(LedgerConflict, match='Owned split subscription required'):
            await checkout_package(
                reader,
                user_id=2,
                subscription_id=target['subscription_id'],
                group='bs',
                gb=10,
                expected_price_kopeks=2500,
                operation_key='wrong-owner',
            )
        assert await totals(db) == before
        reader._api.get_split_access_snapshot.assert_not_awaited()
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_replay_changed_terms_rejected_without_panel(postgres_database, monkeypatch):
    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        monkeypatch.setattr(checkout, 'advance_purchase_delivery', AsyncMock(return_value='pending'))
        args = dict(
            user_id=1,
            subscription_id=target['subscription_id'],
            group='bs',
            gb=10,
            expected_price_kopeks=2500,
            operation_key='receipt',
        )
        await checkout_package(reader, **args)
        reader._api.get_split_access_snapshot.reset_mock()
        with pytest.raises(LedgerConflict, match='original terms'):
            await checkout_package(reader, **(args | {'expected_price_kopeks': 2400}))
        reader._api.get_split_access_snapshot.assert_not_awaited()
        db.expire_all()
        assert (await totals(db))[:3] == (7500, 1, 1)
