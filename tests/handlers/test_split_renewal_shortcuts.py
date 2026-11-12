from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import settings
from app.handlers import menu
from app.handlers.subscription import purchase, tariff_purchase


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
@pytest.mark.parametrize('entry', ['activate', 'simple'])
async def test_shortcuts_redirect_to_tariff_confirmation_before_pricing(monkeypatch, mode, entry):
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', False)
    user = SimpleNamespace(id=7, language='ru')
    sub = SimpleNamespace(id=42, traffic_accounting_mode=mode)
    callback, db = object(), object()
    screen = AsyncMock()
    monkeypatch.setattr(tariff_purchase, 'show_tariff_extend', screen)
    monkeypatch.setattr('app.database.crud.subscription.get_subscription_by_user_id', AsyncMock(return_value=sub))
    pricing = AsyncMock(side_effect=AssertionError('Shortcut may not price or debit split'))
    monkeypatch.setattr('app.services.pricing_engine.pricing_engine.calculate_renewal_price', pricing)
    if entry == 'activate':
        await menu.handle_activate_button(callback, user, db)
    else:
        await purchase._extend_simple_tariff_subscription(callback, user, db, sub)
    screen.assert_awaited_once_with(callback, user, db, subscription_id=42)
    pricing.assert_not_awaited()


async def test_multi_activate_requires_selection_even_for_expired_split(monkeypatch):
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    user = SimpleNamespace(id=7, language='ru')
    callback = SimpleNamespace(answer=AsyncMock())
    db = SimpleNamespace(scalar=AsyncMock(return_value=42))
    active = AsyncMock(side_effect=AssertionError('Expired split must not become classic purchase'))
    monkeypatch.setattr('app.database.crud.subscription.get_active_subscriptions_by_user_id', active)
    await menu.handle_activate_button(callback, user, db)
    callback.answer.assert_awaited_once()
    active.assert_not_awaited()
    assert 'subscriptions.user_id' in str(db.scalar.await_args.args[0])
