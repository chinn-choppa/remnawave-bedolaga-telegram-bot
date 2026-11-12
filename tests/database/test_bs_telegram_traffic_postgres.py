from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


class State:
    def __init__(self):
        self.data = {}

    async def get_data(self):
        return self.data.copy()

    async def update_data(self, **values):
        self.data.update(values)


def callback(data):
    return SimpleNamespace(data=data, answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))


@pytest.mark.parametrize('changed', [False, True])
async def test_confirm_real_quote_replay_and_price_change(postgres_database, monkeypatch, changed):
    from app.handlers.subscription import split_traffic as handler
    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)

        @asynccontextmanager
        async def bound(user_id, subscription_id):
            yield reader

        monkeypatch.setattr(handler, 'purchase_reader', bound)
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        monkeypatch.setattr(checkout, 'advance_purchase_delivery', AsyncMock(side_effect=TimeoutError()))
        state = State()
        user = SimpleNamespace(id=1, language='ru')
        quote = callback(f'spq:{target["subscription_id"]}:bs:10')
        await handler.handle_split_quote(quote, user, state)
        intent = state.data['split_traffic_purchase']
        assert intent['expected_price_kopeks'] == 2500
        if changed:
            from app.database.models import Subscription, Tariff

            sub = await db.get(Subscription, target['subscription_id'])
            tariff = await db.get(Tariff, sub.tariff_id)
            tariff.bs_traffic_settings = {**tariff.bs_traffic_settings, 'topup_packages': {'10': 2600}}
            await db.commit()
        confirm = callback('spc:' + intent['operation_key'])
        await handler.handle_split_confirm(confirm, user, state)
        await handler.handle_split_confirm(confirm, user, state)
        db.expire_all()
        assert (await totals(db))[:3] == ((10000, 0, 0) if changed else (7500, 1, 1))
        if not changed:
            assert 'split_traffic_purchase' in state.data
            assert 'оплачен' in confirm.message.edit_text.call_args.args[0].lower()


async def test_foreign_owner_and_stale_callback_cannot_charge(postgres_database, monkeypatch):
    from app.handlers.subscription import split_traffic as handler

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)

        @asynccontextmanager
        async def bound(user_id, subscription_id):
            yield reader

        monkeypatch.setattr(handler, 'purchase_reader', bound)
        state = State()
        await handler.handle_split_quote(
            callback(f'spq:{target["subscription_id"]}:bs:10'), SimpleNamespace(id=2, language='ru'), state
        )
        assert not state.data
        await handler.handle_split_confirm(callback('spc:missing'), SimpleNamespace(id=1, language='ru'), state)
        db.expire_all()
        assert (await totals(db))[:3] == (10000, 0, 0)


@pytest.mark.parametrize('guard', ['trial', 'expired', 'grace'])
async def test_ineligible_subscription_cannot_get_confirm(postgres_database, monkeypatch, guard):
    from datetime import UTC, datetime, timedelta

    from app.database.models import Subscription
    from app.handlers.subscription import split_traffic as handler

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        if guard == 'trial':
            sub.is_trial = True
        elif guard == 'expired':
            sub.end_date = datetime.now(UTC) - timedelta(days=1)
        else:
            sub.grace_session_open = True
        await db.commit()

        @asynccontextmanager
        async def bound(user_id, subscription_id):
            yield reader

        monkeypatch.setattr(handler, 'purchase_reader', bound)
        state = State()
        await handler.handle_split_quote(callback('spq:1:bs:10'), SimpleNamespace(id=1, language='ru'), state)
        assert not state.data
        assert (await totals(db))[:3] == (10000, 0, 0)


async def test_ordinary_group_and_concurrent_confirm(postgres_database, monkeypatch):
    import asyncio

    from app.database.models import Subscription, Tariff
    from app.handlers.subscription import split_traffic as handler
    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.traffic_topup_enabled = True
        tariff.traffic_topup_packages = {'10': 1500}
        await db.commit()

        @asynccontextmanager
        async def bound(user_id, subscription_id):
            yield reader

        monkeypatch.setattr(handler, 'purchase_reader', bound)
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        monkeypatch.setattr(checkout, 'advance_purchase_delivery', AsyncMock(return_value='pending'))
        state = State()
        user = SimpleNamespace(id=1, language='ru')
        await handler.handle_split_quote(callback('spq:1:ordinary:10'), user, state)
        intent = state.data['split_traffic_purchase']
        assert intent['subscription_id'] == 1 and intent['gb'] == 10
        await asyncio.gather(
            *[handler.handle_split_confirm(callback('spc:' + intent['operation_key']), user, state) for _ in range(2)]
        )
        db.expire_all()
        assert (await totals(db))[:3] == (8500, 1, 0)


async def test_menu_owned_scope_runtime_and_group_choice(postgres_database, monkeypatch):
    from app.handlers.subscription import split_traffic as handler
    from app.services.bs_traffic import runtime

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        monkeypatch.setattr(handler, 'AsyncSessionLocal', reader._factory)
        monkeypatch.setattr(runtime, 'runtime_ready', lambda scope: scope == reader.panel_scope)
        user = SimpleNamespace(id=1, language='ru')
        menu = callback('st:1')
        await handler.show_split_packages(menu, user, 1)
        rows = menu.message.edit_text.call_args.kwargs['reply_markup'].inline_keyboard
        assert rows[0][0].callback_data == 'spg:1:bs'
        choice = callback('spg:1:bs')
        await handler.handle_split_group(choice, user)
        rows = choice.message.edit_text.call_args.kwargs['reply_markup'].inline_keyboard
        assert rows[0][0].callback_data == 'spq:1:bs:10'
        foreign = callback('st:1')
        await handler.show_split_packages(foreign, SimpleNamespace(id=2, language='ru'), 1)
        foreign.message.edit_text.assert_not_awaited()
        monkeypatch.setattr(runtime, 'runtime_ready', lambda scope: False)
        stopped = callback('st:1')
        await handler.show_split_packages(stopped, user, 1)
        stopped.message.edit_text.assert_not_awaited()


async def test_ambiguous_timeout_keeps_uuid_before_new_quote(postgres_database, monkeypatch):
    from app.handlers.subscription import split_traffic as handler
    from app.services.bs_traffic import checkout

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)

        @asynccontextmanager
        async def bound(user_id, subscription_id):
            yield reader

        monkeypatch.setattr(handler, 'purchase_reader', bound)
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        monkeypatch.setattr(checkout, 'advance_purchase_delivery', AsyncMock(return_value='pending'))
        state = State()
        user = SimpleNamespace(id=1, language='ru')
        await handler.handle_split_quote(callback('spq:1:bs:10'), user, state)
        key = state.data['split_traffic_purchase']['operation_key']
        real_checkout = handler.checkout_package

        async def lost_response(*args, **kwargs):
            await real_checkout(*args, **kwargs)
            raise TimeoutError('response lost after commit')

        monkeypatch.setattr(handler, 'checkout_package', lost_response)
        await handler.handle_split_confirm(callback('spc:' + key), user, state)
        assert state.data['split_traffic_attempted']
        await handler.handle_split_quote(callback('spq:1:bs:10'), user, state)
        assert state.data['split_traffic_purchase']['operation_key'] == key
        monkeypatch.setattr(handler, 'checkout_package', real_checkout)
        await handler.handle_split_confirm(callback('spc:' + key), user, state)
        assert not state.data['split_traffic_attempted']
        db.expire_all()
        assert (await totals(db))[:3] == (7500, 1, 1)
