"""Telegram confirmations bind one owned subscription, terms and durable UUID."""

import inspect
from contextlib import asynccontextmanager
from importlib import import_module, util
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import Subscription, Transaction, User
from app.handlers.subscription import tariff_purchase
from tests.database.test_bs_tariff_switch_postgres import setup_switch
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    'fault', ['none', 'price', 'owner', 'missing', 'lost_response', 'wrong_uuid', 'generic_conflict']
)
async def test_telegram_switch_fixed_confirmation_and_offline_replay(postgres_database, monkeypatch, fault):
    assert util.find_spec('app.handlers.subscription.split_tariff_switch') is not None
    adapter = import_module('app.handlers.subscription.split_tariff_switch')
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, _ = await setup_switch(db, monkeypatch)
        user = await db.get(User, target['user_id'])
        await db.commit()

        @asynccontextmanager
        async def bound_reader(*args):
            yield reader

        monkeypatch.setattr(adapter, 'purchase_reader', bound_reader)
        if fault == 'lost_response':
            original_checkout = adapter.checkout_tariff_switch

            async def lose_paid_reply(*args, **kwargs):
                await original_checkout(*args, **kwargs)
                raise TimeoutError('Synthetic lost adapter response')

            monkeypatch.setattr(adapter, 'checkout_tariff_switch', lose_paid_reply)
        monkeypatch.setattr(
            'app.services.bs_traffic.tariff_switch_checkout.advance_paid_renewal', AsyncMock(side_effect=TimeoutError)
        )
        monkeypatch.setattr('app.services.bs_traffic.tariff_switch_checkout._emit_effects', AsyncMock())
        saved = {}

        async def clear():
            saved.clear()

        async def update(**values):
            saved.update(values)

        state = SimpleNamespace(get_data=AsyncMock(side_effect=lambda: saved), update_data=update, clear=clear)
        callback = SimpleNamespace(
            data=f'instant_sw_preview:{new.id}',
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )
        assert await adapter.preview_split_tariff_switch(
            callback, user, db, state, subscription=sub, new_tariff_id=new.id
        )
        preview = callback.message.edit_text.await_args.args[0]
        assert 'Обычный трафик: 20 ГБ' in preview
        assert 'БС-трафик: 10 ГБ' in preview
        assert 'Устройства: 4' in preview
        confirmation = saved['split_tariff_switch_confirmation']
        original_key = confirmation['operation_key']
        price = confirmation['price_kopeks']
        assert confirmation['subscription_id'] == sub.id
        assert confirmation['new_tariff_id'] == new.id
        assert len(confirmation['terms_hash']) == 64
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        if fault == 'generic_conflict':
            from app.services.bs_traffic.ledger import LedgerConflict

            monkeypatch.setattr(
                adapter, 'checkout_tariff_switch', AsyncMock(side_effect=LedgerConflict('Synthetic conflict'))
            )
        if fault == 'price':
            confirmation['price_kopeks'] += 1
        elif fault == 'owner':
            confirmation['user_id'] += 1
        elif fault == 'missing':
            saved.clear()
        callback.data = f'instant_sw_confirm:{new.id}'
        legacy_debit = AsyncMock(side_effect=AssertionError('Legacy split switch bypass'))
        monkeypatch.setattr(tariff_purchase, 'subtract_user_balance', legacy_debit)
        monkeypatch.setattr(
            tariff_purchase, '_resolve_switch_subscription_target', AsyncMock(return_value=(sub, sub.id))
        )
        await inspect.unwrap(tariff_purchase.confirm_instant_switch)(callback, user, db, state)
        # The legacy callback has no consent UUID. An old message for the same
        # target cannot approve a newly prepared price/terms snapshot.
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        callback.data = f'swt:{target["subscription_id"]}:' + (
            '00000000-0000-4000-8000-000000000001' if fault == 'wrong_uuid' else original_key
        )
        await adapter.handle_split_tariff_switch_confirm(callback, user, db, state)
        db.expire_all()
        assert (await db.get(User, target['user_id'])).balance_kopeks == (
            10000 - price if fault in ('none', 'lost_response') else 10000
        )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == (
            1 if fault in ('none', 'lost_response') else 0
        )
        legacy_debit.assert_not_awaited()
        if fault == 'generic_conflict':
            assert saved['split_tariff_switch_attempted']
            user = await db.get(User, target['user_id'])
            assert await adapter.preview_split_tariff_switch(
                callback,
                user,
                db,
                state,
                subscription=SimpleNamespace(id=999, traffic_accounting_mode='split'),
                new_tariff_id=confirmation['new_tariff_id'],
            )
            assert saved['split_tariff_switch_confirmation']['subscription_id'] == target['subscription_id']
            assert saved['split_tariff_switch_confirmation']['operation_key'] == original_key
        if fault not in ('none', 'lost_response'):
            return
        if fault == 'lost_response':
            sub = await db.get(Subscription, target['subscription_id'])
            user = await db.get(User, target['user_id'])
            assert saved['split_tariff_switch_attempted']
            assert await adapter.preview_split_tariff_switch(
                callback,
                user,
                db,
                state,
                subscription=sub,
                new_tariff_id=confirmation['new_tariff_id'],
            )
        assert saved['split_tariff_switch_confirmation']['operation_key'] == original_key
        assert saved['split_tariff_switch_attempted']
        sub = await db.get(Subscription, target['subscription_id'])
        user = await db.get(User, target['user_id'])
        with monkeypatch.context() as navigation:
            navigation.setattr(tariff_purchase, '_resolve_switch_subscription', AsyncMock(return_value=(sub, sub.id)))
            navigation.setattr(tariff_purchase, 'get_tariff_by_id', AsyncMock(return_value=None))
            await inspect.unwrap(tariff_purchase.show_instant_switch_list)(callback, user, db, state)
        assert saved['split_tariff_switch_confirmation']['operation_key'] == original_key
        assert saved['split_tariff_switch_attempted']
        keyboard = callback.message.edit_text.await_args.kwargs['reply_markup']
        assert keyboard.inline_keyboard[0][0].callback_data == f'swt:{target["subscription_id"]}:{original_key}'
        assert 'Повторная оплата не нужна' in callback.message.edit_text.await_args.args[0]
        # Receipt remains viewable without current split ownership, availability,
        # balance or any panel request. This is a check, never another switch.
        sub = await db.get(Subscription, target['subscription_id'])
        sub.traffic_accounting_mode = 'managed'
        new.is_active = False
        await db.commit()
        reader._api.get_internal_squad_topology.side_effect = AssertionError('Receipt replay requested topology')
        callback.data = 'swt:' + original_key
        await adapter.handle_split_tariff_switch_confirm(callback, user, db, state)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        db.expire_all()
        assert (await db.get(User, target['user_id'])).balance_kopeks == 10000 - price

        @asynccontextmanager
        async def no_reader(*args):
            raise AssertionError('Receipt recovery opened panel reader')
            yield  # pragma: no cover

        monkeypatch.setattr(adapter, 'purchase_reader', no_reader)
        saved.clear()
        for case in ('owned', 'foreign', 'wrong_subscription', 'unknown_uuid'):
            owner = await db.get(User, 2 if case == 'foreign' else target['user_id'])
            callback.data = 'swt:{}:{}'.format(
                999 if case == 'wrong_subscription' else target['subscription_id'],
                '00000000-0000-4000-8000-000000000001' if case == 'unknown_uuid' else original_key,
            )
            rendered = callback.message.edit_text.await_count
            await adapter.handle_split_tariff_switch_confirm(callback, owner, db, state)
            assert callback.message.edit_text.await_count == rendered + (case == 'owned')
            if case != 'owned':
                assert callback.answer.await_args.kwargs['show_alert']
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1

        # A stored operation alone is insufficient: its payment must verify.
        payment = await db.scalar(select(Transaction))
        payment.amount_kopeks -= 1
        await db.commit()
        owner = await db.get(User, target['user_id'])
        callback.data = f'swt:{target["subscription_id"]}:{original_key}'
        rendered = callback.message.edit_text.await_count
        await adapter.handle_split_tariff_switch_confirm(callback, owner, db, state)
        assert callback.message.edit_text.await_count == rendered
        assert callback.answer.await_args.kwargs['show_alert']
