"""A depleted BS group must not hide the daily auto-charge pause action."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.handlers.subscription.my_subscriptions import _build_subscription_detail_keyboard
from app.handlers.subscription.split_daily import show_split_daily_menu


@pytest.mark.parametrize('entry', ['single', 'multi'])
@pytest.mark.parametrize(
    'status,paused,expired,resume',
    [
        ('limited', False, False, False),
        ('limited', True, False, True),
        ('limited', False, True, True),
        ('active', False, False, False),
        ('active', False, True, True),
        ('disabled', False, False, True),
        ('expired', False, False, True),
    ],
)
async def test_daily_menu_keeps_pause_available_for_paid_limited(entry, status, paused, expired, resume):
    sub = SimpleNamespace(
        id=42,
        traffic_accounting_mode='split',
        is_daily_tariff=True,
        is_trial=False,
        actual_status=status,
        is_daily_paused=paused,
        end_date=datetime.now(UTC) + timedelta(hours=-1 if expired else 1),
    )
    if entry == 'single':
        callback = SimpleNamespace(answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
        await show_split_daily_menu(callback, SimpleNamespace(language='ru'), sub)
        keyboard = callback.message.edit_text.await_args.kwargs['reply_markup']
    else:
        keyboard = _build_subscription_detail_keyboard(sub.id, sub=sub, language='ru')
    actions = [
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith('sdp:')
    ]
    assert actions == [f'sdp:42:{0 if resume else 1}']
