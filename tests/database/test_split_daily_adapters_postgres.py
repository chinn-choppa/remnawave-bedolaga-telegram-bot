"""Daily adapters preserve free controls and require immutable paid consent."""

from datetime import UTC, datetime, timedelta
from importlib import import_module, util
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from app.database.models import Transaction, User
from tests.database.test_bs_daily_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('paused', [True, False])
async def test_cabinet_free_daily_control_never_buys_day(postgres_database, monkeypatch, paused):
    from app.cabinet.routes.subscription_modules.daily import toggle_subscription_pause

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.end_date = datetime.now(UTC) + timedelta(hours=8)
        sub.is_daily_paused = not paused
        user = await db.get(User, owner['user_id'])
        user.balance_kopeks = 0
        end, charged = sub.end_date, sub.last_daily_charge_at
        await db.commit()
        result = await toggle_subscription_pause(user=user, db=db, subscription_id=sub.id, paused=paused)
        assert result['is_paused'] is paused
        assert sub.end_date == end and sub.last_daily_charge_at == charged
        assert user.balance_kopeks == 0
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_cabinet_expired_free_request_requires_paid_consent(postgres_database, monkeypatch):
    from app.cabinet.routes.subscription_modules.daily import toggle_subscription_pause

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        user = await db.get(User, owner['user_id'])
        await db.commit()
        with pytest.raises(HTTPException) as error:
            await toggle_subscription_pause(user=user, db=db, subscription_id=sub.id, paused=False)
        assert error.value.status_code == 409
        assert error.value.detail['code'] == 'daily_resume_requires_confirmation'
        await db.rollback()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('data', ['sdr:1:bad', 'sdr:1:00000000-0000-4000-8000-000000000001'])
async def test_telegram_missing_consent_cannot_open_checkout(postgres_database, monkeypatch, data):
    assert util.find_spec('app.handlers.subscription.split_daily') is not None
    adapter = import_module('app.handlers.subscription.split_daily')
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _ = await prepared(db, monkeypatch)
        user = await db.get(User, owner['user_id'])
        callback = SimpleNamespace(data=data, answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
        state = SimpleNamespace(get_data=AsyncMock(return_value={}))
        checkout = AsyncMock(side_effect=AssertionError('Missing consent cannot pay'))
        monkeypatch.setattr(adapter, 'checkout_daily_resume', checkout)
        await adapter.handle_split_daily_confirm(callback, user, db, state)
        checkout.assert_not_awaited()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('fault', ['none', 'price', 'missing_fsm', 'foreign', 'lost_response'])
async def test_telegram_paid_consent_and_durable_check(postgres_database, monkeypatch, fault):
    from contextlib import asynccontextmanager

    from app.handlers.subscription import split_daily as adapter
    from app.services.bs_traffic import daily_control_checkout as checkout

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _ = await prepared(db, monkeypatch)
        uid, sid = owner['user_id'], sub.id
        user = await db.get(User, uid)
        await db.commit()

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(adapter, 'purchase_reader', bound)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(side_effect=TimeoutError))
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        data = {}

        async def update(**values):
            data.update(values)

        state = SimpleNamespace(get_data=AsyncMock(side_effect=lambda: data), update_data=update)
        callback = SimpleNamespace(
            data=f'sdp:{sid}:0', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await adapter.handle_split_daily_control(callback, user, db, state)
        saved = dict(data['split_daily_confirmation'])
        assert saved['subscription_id'] == sid
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        if fault == 'price':
            data['split_daily_confirmation']['price_kopeks'] += 1
        elif fault == 'missing_fsm':
            data.clear()
        elif fault == 'foreign':
            data['split_daily_confirmation']['user_id'] += 999
        elif fault == 'lost_response':
            original = adapter.checkout_daily_resume

            async def lose(*args, **kwargs):
                await original(*args, **kwargs)
                raise TimeoutError('Synthetic lost paid response')

            monkeypatch.setattr(adapter, 'checkout_daily_resume', lose)
        callback.data = f'sdr:{sid}:{saved["operation_key"]}'
        await adapter.handle_split_daily_confirm(callback, await db.get(User, uid), db, state)
        await db.rollback()
        paid = fault in ('none', 'lost_response')
        assert await db.scalar(select(func.count()).select_from(Transaction)) == int(paid)
        assert await db.scalar(select(User.balance_kopeks).where(User.id == uid)) == 10000 - (
            saved['price_kopeks'] if paid else 0
        )
        data.clear()
        monkeypatch.setattr(
            adapter, 'checkout_daily_resume', AsyncMock(side_effect=AssertionError('Result check cannot pay'))
        )
        monkeypatch.setattr(
            adapter, 'purchase_reader', lambda *args: (_ for _ in ()).throw(AssertionError('Offline replay'))
        )
        callback.data = f'sdc:{sid}:{saved["operation_key"]}'
        await adapter.handle_split_daily_confirm(callback, await db.get(User, uid), db, state)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == int(paid)
        if paid:
            assert 'Повторная оплата не нужна' in callback.message.edit_text.await_args.args[0]


async def test_cabinet_paid_quote_confirm_and_offline_result(postgres_database, monkeypatch):
    from contextlib import asynccontextmanager
    from uuid import uuid4

    from app.cabinet.routes.subscription_modules import split_daily as adapter
    from app.services.bs_traffic import daily_control_checkout as checkout

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _ = await prepared(db, monkeypatch)
        uid, sid = owner['user_id'], sub.id
        user = await db.get(User, uid)
        await db.commit()

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(adapter, '_reader', bound)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(side_effect=TimeoutError))
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        quote = await adapter.quote_daily_resume(subscription_id=sid, user=user, db=db)
        request = adapter.DailyResumeConfirmation(
            subscription_id=sid,
            operation_id=uuid4(),
            expected_price_kopeks=quote['price_kopeks'],
            expected_terms_hash=quote['terms_hash'],
        )
        result = await adapter.confirm_daily_resume(request=request, user=user, db=db)
        assert result['delivery_pending'] is True
        assert result['charged_kopeks'] == quote['price_kopeks']
        assert result['is_paused'] is False
        await db.rollback()
        sub = await db.get(type(sub), sid)
        sub.traffic_accounting_mode = 'managed'
        sub.is_daily_paused = True
        await db.commit()
        monkeypatch.setattr(
            adapter, '_reader', lambda *a: (_ for _ in ()).throw(AssertionError('Receipt must be offline'))
        )
        result = await adapter.daily_resume_result(
            operation_id=request.operation_id, subscription_id=sid, user=await db.get(User, uid), db=db
        )
        assert result['replayed'] is True
        assert result['is_paused'] is True
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        with pytest.raises(HTTPException) as missing:
            await adapter.daily_resume_result(
                operation_id=request.operation_id, subscription_id=sid + 999, user=await db.get(User, uid), db=db
            )
        assert missing.value.status_code == 404


async def test_multi_selected_daily_controls_are_explicit_and_owned(postgres_database, monkeypatch):
    from app.handlers.subscription import split_daily as adapter
    from app.handlers.subscription.my_subscriptions import _build_subscription_detail_keyboard

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        uid, sid = owner['user_id'], sub.id
        user = await db.get(User, uid)
        sub.end_date = datetime.now(UTC) + timedelta(hours=8)
        await db.commit()
        keyboard = _build_subscription_detail_keyboard(sub.id, sub=sub, language='ru')
        buttons = [button for row in keyboard.inline_keyboard for button in row]
        assert any(button.callback_data == f'sdp:{sid}:1' for button in buttons)
        data = {}
        state = SimpleNamespace(get_data=AsyncMock(return_value=data))
        callback = SimpleNamespace(
            data=f'sdp:{sid}:1', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await adapter.handle_split_daily_control(callback, user, db, state)
        assert sub.is_daily_paused is True
        await adapter.handle_split_daily_control(callback, user, db, state)
        assert sub.is_daily_paused is True  # Explicit repeated pause is not a toggle.
        callback.data = f'sdp:{sid + 999}:0'
        await adapter.handle_split_daily_control(callback, user, db, state)
        assert sub.is_daily_paused is True
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_http_daily_contract_rejects_unowned_and_invalid_confirmation(postgres_database, monkeypatch):
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
    from app.cabinet.routes.subscription_modules import daily, split_daily as adapter

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        user = await db.get(User, owner['user_id'])
        app = FastAPI()
        app.include_router(daily.router, prefix='/cabinet/subscription')
        app.dependency_overrides[get_cabinet_db] = lambda: db
        app.dependency_overrides[get_current_cabinet_user] = lambda: user
        monkeypatch.setattr(adapter, 'checkout_daily_resume', AsyncMock(side_effect=AssertionError('No consent')))
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
            response = await client.post('/cabinet/subscription/split-daily/confirm', json={'subscription_id': sub.id})
            assert response.status_code == 422
            response = await client.get(
                '/cabinet/subscription/split-daily/result',
                params={'subscription_id': sub.id + 99, 'operation_id': '00000000-0000-4000-8000-000000000001'},
            )
            assert response.status_code == 404
            response = await client.post('/cabinet/subscription/pause', params={'subscription_id': sub.id})
            assert response.status_code == 409, response.text
            assert response.json()['detail']['code'] == 'daily_pause_target_required'
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_completed_receipt_check_releases_same_pending_confirmation(postgres_database, monkeypatch):
    from uuid import uuid4

    from app.handlers.subscription import split_daily as adapter

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        uid, sid = owner['user_id'], sub.id
        user = await db.get(User, uid)
        key = str(uuid4())
        data = {
            'split_daily_confirmation': dict(
                user_id=uid, subscription_id=sid, operation_key=key, price_kopeks=100, terms_hash='a' * 64
            ),
            'split_daily_attempted': True,
        }

        async def update(**values):
            data.update(values)

        state = SimpleNamespace(get_data=AsyncMock(side_effect=lambda: data), update_data=update)
        monkeypatch.setattr(
            adapter,
            'find_daily_resume_by_operation',
            AsyncMock(
                return_value=SimpleNamespace(
                    charged_kopeks=100, new_end_date=datetime.now(UTC) + timedelta(days=1), delivery_pending=False
                )
            ),
        )
        monkeypatch.setattr(adapter, 'checkout_daily_resume', AsyncMock(side_effect=AssertionError('Read only')))
        callback = SimpleNamespace(
            data=f'sdc:{sid}:{key}', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await adapter.handle_split_daily_confirm(callback, user, db, state)
        assert data['split_daily_attempted'] is False
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_unknown_check_only_offers_same_consent_retry(postgres_database, monkeypatch):
    from uuid import uuid4

    from app.handlers.subscription import split_daily as adapter

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        uid, sid = owner['user_id'], sub.id
        key = str(uuid4())
        saved = dict(user_id=uid, subscription_id=sid, operation_key=key, price_kopeks=100, terms_hash='a' * 64)
        state = SimpleNamespace(
            get_data=AsyncMock(return_value={'split_daily_confirmation': saved, 'split_daily_attempted': True})
        )
        checkout = AsyncMock(side_effect=AssertionError('Check cannot pay'))
        monkeypatch.setattr(adapter, 'checkout_daily_resume', checkout)
        callback = SimpleNamespace(
            data=f'sdc:{sid}:{key}', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await adapter.handle_split_daily_confirm(callback, await db.get(User, uid), db, state)
        buttons = callback.message.edit_text.await_args.kwargs['reply_markup'].inline_keyboard
        assert buttons[0][0].callback_data == f'sdr:{sid}:{key}'
        checkout.assert_not_awaited()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
