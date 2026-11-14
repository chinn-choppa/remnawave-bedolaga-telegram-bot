"""Сторож: всё, что правится в кабинете, правится и в телеграм-редакторе тарифов.

Владелец: «в боте много чего нет, как и в кабинете — настройки разнятся, нужно
исправлять, и не допустить ошибок как в прошлый раз». Поля тарифа заводятся в кабинете
(TariffUpdateRequest), а редактор бота отставал: не знал внешний сквад, продукт Lava,
подарки, произвольные дни, лимиты по серверам — и писал дни триала в колонку, которой
не было в модели.

Покрытие бота снимается с кода: kwargs вызовов update_tariff/create_tariff и присваивания
tariff.<поле> во всех модулях редактора. Поля, которые бот меняет отдельными CRUD-вызовами,
перечислены явно с указанием этих вызовов.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from app.cabinet.schemas.tariffs import TariffUpdateRequest
from app.database.models import Tariff


ROOT = Path(__file__).resolve().parents[2]
EDITOR_MODULES = sorted((ROOT / 'app' / 'handlers' / 'admin').glob('tariff*.py'))

# Поля кабинета, которые бот пишет не через update_tariff, а отдельными CRUD-вызовами.
COVERED_BY_CALL = {
    'promo_group_ids': ('add_promo_group_to_tariff', 'remove_promo_group_from_tariff'),
    'is_trial_available': ('set_trial_tariff', 'clear_trial_tariff'),
}


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding='utf-8'))


def _written_fields() -> set[str]:
    written: set[str] = set()
    for path in EDITOR_MODULES:
        for node in ast.walk(_parse(path)):
            if isinstance(node, ast.Call):
                name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', '')
                if name in ('update_tariff', 'create_tariff'):
                    written.update(kw.arg for kw in node.keywords if kw.arg)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == 'tariff'
                    ):
                        written.add(target.attr)
    return written


def _called_functions() -> set[str]:
    called: set[str] = set()
    for path in EDITOR_MODULES:
        for node in ast.walk(_parse(path)):
            if isinstance(node, ast.Call):
                called.add(node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', ''))
    return called


def test_editor_modules_are_found() -> None:
    names = {path.name for path in EDITOR_MODULES}
    assert 'tariffs.py' in names and 'tariff_custom_traffic.py' in names


def test_every_cabinet_field_is_editable_from_telegram() -> None:
    written = _written_fields()
    called = _called_functions()

    missing = []
    for field in TariffUpdateRequest.model_fields:
        if field in written:
            continue
        if field in COVERED_BY_CALL and any(fn in called for fn in COVERED_BY_CALL[field]):
            continue
        missing.append(field)

    assert not missing, 'кабинет правит эти поля тарифа, а телеграм-редактор — нет: ' + ', '.join(sorted(missing))


def test_telegram_editor_writes_only_real_columns() -> None:
    """Дни триала писались в атрибут, которого не было в модели — молча терялись."""
    columns = set(Tariff.__table__.c.keys())
    phantom = sorted(_written_fields() - columns - set(TariffUpdateRequest.model_fields))
    assert not phantom, f'редактор пишет поля, которых нет ни в модели, ни в кабинете: {phantom}'
    assert 'trial_duration_days' in columns


def test_new_editor_modules_are_registered_from_tariff_router() -> None:
    source = (ROOT / 'app' / 'handlers' / 'admin' / 'tariffs.py').read_text(encoding='utf-8')
    for register in (
        'register_custom_traffic_handlers(dp)',
        'register_custom_days_handlers(dp)',
        'register_panel_settings_handlers(dp)',
        'register_server_limits_handlers(dp)',
    ):
        assert register in source, register


def test_exclusive_family_telegram_input_rejects_invalid_slugs() -> None:
    from app.handlers.admin.tariff_panel_settings import parse_exclusive_family_input

    assert parse_exclusive_family_input('main') == 'main'
    assert parse_exclusive_family_input('  family_2  ') == 'family_2'
    assert parse_exclusive_family_input('-') is None
    for bad in ('', 'Main', '2main', 'a.b', 'a' * 65, 'main family'):
        with pytest.raises(ValueError):
            parse_exclusive_family_input(bad)

def test_exclusive_family_editor_is_reachable_and_shows_current_family() -> None:
    from app.handlers.admin.tariff_panel_settings import get_panel_settings_keyboard

    tariff = Tariff(id=77, name='Basic', exclusive_family='main')
    buttons = get_panel_settings_keyboard(tariff, 'ru').inline_keyboard
    family_buttons = [
        button for row in buttons for button in row if button.callback_data == 'admin_tariff_edit_family:77'
    ]
    assert len(family_buttons) == 1
    assert 'main' in family_buttons[0].text
