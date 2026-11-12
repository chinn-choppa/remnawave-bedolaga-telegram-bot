"""Telegram split confirmation uses real PostgreSQL debit, expiry and receipt."""

import inspect
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import Tariff, Transaction, User
from app.handlers.subscription import tariff_purchase
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('fault', [None, 'price', 'missing_confirmation', 'tariff_mismatch', 'balance'])
async def test_telegram_split_shared_checkout_and_replay(postgres_database, monkeypatch, fault):
    monkeypatch.setattr('app.database.crud.transaction.emit_transaction_side_effects', AsyncMock())
    monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
    monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False))
    monkeypatch.setattr('app.services.payment.lava.shift_lava_next_charge_after_manual_extension', AsyncMock())
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        user = await db.get(User, sub.user_id)
        if fault == 'balance':
            user.balance_kopeks = 0
            await db.commit()
        before = sub.end_date
        saved = {
            'subscription_id': sub.id,
            'tariff_id': sub.tariff_id,
            'period_days': 30,
            'price_kopeks': 2001 if fault == 'price' else 2000,
            'operation_key': 'telegram:stable',
        }
        if fault == 'tariff_mismatch':
            saved['tariff_id'] += 1
        state = SimpleNamespace(
            get_data=AsyncMock(
                return_value={} if fault == 'missing_confirmation' else {'split_renewal_confirmation': saved}
            )
        )
        callback = SimpleNamespace(
            data=f'tariff_ext_confirm:{sub.id}:{sub.tariff_id}:30',
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )
        handler = inspect.unwrap(tariff_purchase.confirm_tariff_extend)
        # Legacy money and direct panel paths must remain unreachable.
        debit = AsyncMock(side_effect=AssertionError('legacy debit bypass'))
        monkeypatch.setattr(tariff_purchase, 'subtract_user_balance', debit)
        await handler(callback, user, db, state)
        await db.refresh(user)
        await db.refresh(sub)
        if fault:
            assert user.balance_kopeks == (0 if fault == 'balance' else 10000)
            assert sub.end_date == before
            if fault == 'balance':
                assert 'Недостаточно средств' in callback.answer.await_args.args[0]
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        else:
            assert user.balance_kopeks == 8000
            assert sub.end_date == before + timedelta(days=30)
            from app.config import settings

            keyboard = callback.message.edit_text.await_args.kwargs['reply_markup']
            assert keyboard.inline_keyboard[0][0].callback_data == (
                f'sm:{sub.id}' if settings.is_multi_tariff_enabled() else 'menu_subscription'
            )
            assert 'Повторная оплата не нужна' in callback.message.edit_text.await_args.args[0]
            tariff = await db.get(Tariff, sub.tariff_id)
            tariff.is_active = False
            tariff.period_prices = {'30': 99999}
            user.balance_kopeks = 0
            await db.commit()
            await handler(callback, user, db, state)
            await db.refresh(sub)
            assert sub.end_date == before + timedelta(days=30)
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        debit.assert_not_awaited()


async def test_insufficient_split_renewal_cart_preserves_confirmed_operation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 0
        await db.commit()
        saved = {}

        async def update(**values):
            saved.update(values)

        state = SimpleNamespace(update_data=update, get_data=AsyncMock(side_effect=lambda: saved))
        callback = SimpleNamespace(
            data=f'tariff_extend:{sub.id}:{sub.tariff_id}:30',
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )
        cart = AsyncMock()
        monkeypatch.setattr(tariff_purchase.user_cart_service, 'save_user_cart', cart)
        await inspect.unwrap(tariff_purchase.select_tariff_extend_period)(callback, user, db, state)
        confirmation = saved['split_renewal_confirmation']
        assert confirmation['price_kopeks'] == 2000
        assert cart.await_args.args[1]['renewal_operation_key'] == confirmation['operation_key']
        assert cart.await_args.args[1]['total_price'] == confirmation['price_kopeks']
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
