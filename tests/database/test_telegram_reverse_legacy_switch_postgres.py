"""Old generic Telegram buttons cannot debit after split reverse and lost consent."""

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from app.config import settings
from app.database.models import BsTrafficLedger, BsTrafficOwnershipCycle, Subscription, Tariff, Transaction
from app.handlers.subscription import tariff_purchase as handler
from tests.database.test_multi_tariff_switch_postgres import switch_data


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('daily', [False, True])
@pytest.mark.parametrize('consent', ['lost', 'fresh', 'ordinary', 'user', 'subscription', 'current_tariff', 'target'])
async def test_managed_after_reverse_requires_its_own_fresh_legacy_preview(
    postgres_database, monkeypatch, consent, daily
):
    async with switch_data(postgres_database, monkeypatch) as (db, user):
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', False)
        source = await db.get(Subscription, 11)
        source.traffic_accounting_mode = 'managed'
        target = await db.get(Tariff, 3)
        target.is_daily, target.daily_price_kopeks = daily, 200 if daily else 0
        await db.delete(await db.get(Subscription, 12))
        if consent != 'ordinary':
            db.add(
                BsTrafficLedger(
                    subscription_id=11,
                    panel_scope='00000000-0000-4000-8000-000000000001',
                    panel_user_id=111,
                    account_generation='a' * 64,
                )
            )
            await db.flush()
            db.add(BsTrafficOwnershipCycle(subscription_id=11, ownership_cycle=0, state='managed', forward_receipt={}))
        await db.commit()
        panel = SimpleNamespace(create_remnawave_user=AsyncMock(), update_remnawave_user=AsyncMock())
        monkeypatch.setattr(handler, 'SubscriptionService', lambda: panel)
        monkeypatch.setattr(handler, '_reject_unavailable_checkout_squads', AsyncMock(return_value=False))
        saved = {}

        async def update(**values):
            saved.update(values)

        async def clear():
            saved.clear()

        state = SimpleNamespace(get_data=AsyncMock(side_effect=lambda: saved), update_data=update, clear=clear)
        callback = MagicMock(data='instant_sw_preview:3')
        callback.answer = AsyncMock()
        callback.message.edit_text = AsyncMock()
        if consent not in ('lost', 'ordinary'):
            await inspect.unwrap(handler.preview_instant_switch)(callback, user, db, state)
            if consent != 'fresh':
                marker = saved.get('legacy_tariff_switch_confirmation', {}).copy()
                marker[
                    {
                        'user': 'user_id',
                        'subscription': 'subscription_id',
                        'current_tariff': 'current_tariff_id',
                        'target': 'new_tariff_id',
                    }[consent]
                ] = 999
                saved['legacy_tariff_switch_confirmation'] = marker
        callback.data = 'instant_sw_confirm:3'
        await inspect.unwrap(handler.confirm_instant_switch)(callback, user, db, state)
        await db.refresh(source)
        await db.refresh(user)
        payments = list(await db.scalars(select(Transaction)))
        if consent in ('fresh', 'ordinary'):
            assert source.tariff_id == 3
            assert user.balance_kopeks == (9800 if daily else 9334)
            assert len(payments) == 1
        else:
            assert source.tariff_id == 1
            assert user.balance_kopeks == 10000
            assert payments == []
            assert callback.answer.await_args.kwargs['show_alert']
            panel.create_remnawave_user.assert_not_awaited()
            panel.update_remnawave_user.assert_not_awaited()
