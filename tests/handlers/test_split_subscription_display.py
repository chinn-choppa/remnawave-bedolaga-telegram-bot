from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.handlers.subscription import my_subscriptions


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_selected_card_uses_split_text_and_hides_legacy_traffic(monkeypatch, mode):
    from app.services.bs_traffic import customer_text

    subscription = SimpleNamespace(
        id=42,
        is_pending_trial=False,
        is_daily_tariff=False,
        is_trial=False,
        traffic_accounting_mode=mode,
        tariff=None,
        traffic_limit_gb=100,
        traffic_used_gb=77,
        device_limit=2,
        end_date=None,
        status_display='🟢 Активна',
        actual_status='active',
    )
    monkeypatch.setattr(my_subscriptions, 'get_subscription_by_id_for_user', AsyncMock(return_value=subscription))
    monkeypatch.setattr(my_subscriptions, 'get_display_subscription_link', lambda _: None)
    render = AsyncMock(return_value='ordinary: unlimited\nbs: 0')
    monkeypatch.setattr(customer_text, 'subscription_traffic_text', render)
    callback = SimpleNamespace(data='sm:42', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
    state = SimpleNamespace(update_data=AsyncMock())
    db = object()
    await my_subscriptions.show_subscription_detail(callback, SimpleNamespace(id=7, language='ru'), db, state)
    message = callback.message.edit_text.await_args.args[0]
    assert 'ordinary: unlimited\nbs: 0' in message and '77' not in message
    assert render.await_args.kwargs['user_id'] == 7
    assert render.await_args.kwargs['subscription'].id == 42
    buttons = callback.message.edit_text.await_args.kwargs['reply_markup'].inline_keyboard
    assert any(b.callback_data == 'st:42' for row in buttons for b in row) is (mode == 'split')
    state.update_data.assert_awaited_once_with(active_subscription_id=42)
