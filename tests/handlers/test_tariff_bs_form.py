import pytest

from app.handlers.admin.tariff_bs_traffic import parse_packages


@pytest.mark.parametrize('value', ['10=12.345', '0=1', '01=1', '10=-1', '10=NaN', '10=1;10=2', '100001=1'])
def test_invalid_packages_are_not_silently_accepted(value):
    with pytest.raises(ValueError):
        parse_packages(value)


def test_packages_preserve_exact_kopeks():
    assert parse_packages('10=12.34; 50=100\n100=0') == {'10': 1234, '50': 10000, '100': 0}
    assert parse_packages('-') == {}


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'fa', 'zh'])
def test_localized_screen_escapes_names_and_bounds_keyboard(language):
    from types import SimpleNamespace

    from app.handlers.admin.tariff_bs_traffic import render

    form = dict(
        tariff_id=123,
        token='12345678',
        name='<danger>',
        enabled=True,
        page=0,
        field=None,
        ordinary=[],
        catalog=[{'uuid': str(i), 'name': 'N' * 100} for i in range(30)],
        value=dict(squad_uuids=[], limit_gb=0, max_topup_gb=0, topup_enabled=False, topup_packages={}),
    )
    text, keyboard = render(form, SimpleNamespace(language=language))
    assert '<danger>' not in text and '&lt;danger&gt;' in text
    assert 'ADMIN_BS_' not in text and len(text) < 4096
    assert len(keyboard.inline_keyboard) <= 16
    assert all(len(button.callback_data.encode()) <= 64 for row in keyboard.inline_keyboard for button in row)


def test_all_existing_packages_are_visible_before_replacing_list():
    from types import SimpleNamespace

    from app.handlers.admin.tariff_bs_traffic import render

    form = dict(
        tariff_id=1,
        token='12345678',
        name='Test',
        enabled=True,
        page=0,
        field='topup_packages',
        ordinary=[],
        catalog=[],
        value=dict(
            squad_uuids=[],
            limit_gb=100,
            max_topup_gb=0,
            topup_enabled=True,
            topup_packages={str(i): i * 100 for i in range(1, 101)},
        ),
    )
    text, _ = render(form, SimpleNamespace(language='ru'))
    assert '100=100.00' in text
    assert len(text) < 4096


@pytest.mark.parametrize('name', ['start_bs_editor', 'bs_action', 'bs_input'])
async def test_non_admin_never_reaches_fsm_or_database(monkeypatch, name):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from aiogram import types

    from app.config import settings
    from app.handlers.admin import tariff_bs_traffic as ui

    monkeypatch.setattr(type(settings), 'is_admin', lambda self, user_id: False)
    event = MagicMock(spec=types.Message if name == 'bs_input' else types.CallbackQuery)
    event.from_user = SimpleNamespace(id=2, language_code='en')
    event.answer = AsyncMock()
    db, state = AsyncMock(), AsyncMock()
    await getattr(ui, name)(event, SimpleNamespace(language='en'), db, state)
    assert not db.mock_calls and not state.mock_calls
    event.answer.assert_awaited_once()
