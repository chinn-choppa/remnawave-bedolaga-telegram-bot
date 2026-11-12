import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage

from app.database.models import Tariff
from app.handlers.admin import tariff_bs_traffic as ui
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
A = '00000000-0000-4000-8000-000000000001'
B = '00000000-0000-4000-8000-000000000002'
USER = SimpleNamespace(id=1, language='ru')


def callback(data):
    return SimpleNamespace(data=data, message=SimpleNamespace(message_id=20, edit_text=AsyncMock()), answer=AsyncMock())


async def opened(db, monkeypatch):
    # Editor state tests isolate the separately PostgreSQL-tested panel preflight.
    from app.services.bs_traffic import operator_binding

    monkeypatch.setattr(operator_binding, 'prepare_operator_settings', AsyncMock())
    db.add(Tariff(id=1, name='<Test>', period_prices={'30': 100}, allowed_squads=[A]))
    await db.commit()
    monkeypatch.setattr(
        ui,
        'get_all_server_squads',
        AsyncMock(
            return_value=(
                [
                    SimpleNamespace(squad_uuid=A, display_name='A'),
                    SimpleNamespace(squad_uuid=B, display_name='B'),
                ],
                2,
            )
        ),
    )
    state = FSMContext(storage=MemoryStorage(), key=StorageKey(bot_id=1, chat_id=1, user_id=1))
    await inspect.unwrap(ui.start_bs_editor)(callback('admin_tariff_bs:1'), USER, db, state)
    return state


async def action(db, state, name, *, message_id=20, token=None):
    form = (await state.get_data())['bs_form']
    cb = callback(f'bsf:1:{name}:{token or form["token"]}')
    cb.message.message_id = message_id
    await inspect.unwrap(ui.bs_action)(cb, USER, db, state)
    return cb


async def test_selection_is_only_saved_on_explicit_save(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None
        await action(db, state, 'save')
        assert (await db.get(Tariff, 1)).bs_traffic_settings['squad_uuids'] == [B]
        assert (await db.get(Tariff, 1)).allowed_squads == [A]
        assert await state.get_state() is None


async def test_changed_tariff_is_not_overwritten(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        (await db.get(Tariff, 1)).name = 'Changed'
        await db.commit()
        cb = await action(db, state, 'save')
        assert cb.answer.call_args.kwargs['show_alert'] is True
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None


async def test_old_message_and_token_do_not_modify_form(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        before = await state.get_data()
        await action(db, state, 'enabled', message_id=19)
        await action(db, state, 'enabled', token='old')
        assert await state.get_data() == before
        await action(db, state, 'cancel')
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None
        assert await state.get_state() is None


async def test_ordinary_squad_cannot_be_selected(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 's0')
        assert (await state.get_data())['bs_form']['value']['squad_uuids'] == []


async def test_input_changes_only_fsm_and_zero_disables_topup(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        for field, text in [('limit_gb', '100'), ('topup_packages', '10=12.34'), ('max_topup_gb', '500')]:
            await action(db, state, field)
            message = SimpleNamespace(
                text=text,
                answer=AsyncMock(),
                chat=SimpleNamespace(id=1),
                bot=SimpleNamespace(edit_message_text=AsyncMock()),
            )
            await inspect.unwrap(ui.bs_input)(message, USER, db, state)
        await action(db, state, 'topup')
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None
        await action(db, state, 'save')
        value = (await db.get(Tariff, 1)).bs_traffic_settings
        assert value == dict(
            enabled=True,
            squad_uuids=[B],
            limit_gb=100,
            topup_enabled=True,
            topup_packages={'10': 1234},
            max_topup_gb=500,
        )
        await inspect.unwrap(ui.start_bs_editor)(callback('admin_tariff_bs:1'), USER, db, state)
        await action(db, state, 'limit_gb')
        message.text = '0'
        await inspect.unwrap(ui.bs_input)(message, USER, db, state)
        assert (await state.get_data())['bs_form']['value']['topup_enabled'] is False
        await action(db, state, 'enabled')
        await action(db, state, 'save')
        assert {
            key: item for key, item in (await db.get(Tariff, 1)).bs_traffic_settings.items() if key != 'disabled_at'
        } == value | {'enabled': False}


async def test_invalid_save_keeps_form_for_correction(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        await action(db, state, 'topup')
        cb = await action(db, state, 'save')
        assert cb.answer.call_args.kwargs['show_alert'] is True
        assert (await state.get_data())['bs_form']['saving'] is False
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None


async def test_removed_squad_is_rechecked_on_save(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        monkeypatch.setattr(ui, 'get_all_server_squads', AsyncMock(return_value=([], 0)))
        cb = await action(db, state, 'save')
        assert cb.answer.call_args.kwargs['show_alert'] is True
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None


async def test_disabled_editor_reopens_off_and_preserves_terms(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        await action(db, state, 'save')
        await inspect.unwrap(ui.start_bs_editor)(callback('admin_tariff_bs:1'), USER, db, state)
        await action(db, state, 'enabled')
        await action(db, state, 'save')
        await inspect.unwrap(ui.start_bs_editor)(callback('admin_tariff_bs:1'), USER, db, state)
        form = (await state.get_data())['bs_form']
        assert form['enabled'] is False
        assert form['value']['squad_uuids'] == [B]
        await action(db, state, 'enabled')
        await action(db, state, 'save')
        assert (await db.get(Tariff, 1)).bs_traffic_settings['enabled'] is True


async def test_panel_preflight_failure_keeps_unsaved_editor(postgres_database, monkeypatch):
    from app.services.bs_traffic import operator_binding

    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        monkeypatch.setattr(
            operator_binding, 'prepare_operator_settings', AsyncMock(side_effect=ValueError('Invalid topology'))
        )
        cb = await action(db, state, 'save')
        assert cb.answer.call_args.kwargs['show_alert'] is True
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None
        assert (await state.get_data())['bs_form']['saving'] is False


@pytest.mark.parametrize(
    'code',
    [
        'bs_topology_invalid',
        'bs_usage_config_invalid',
        'bs_panel_unavailable',
        'bs_binding_mismatch',
        'bs_uncertain_policy_required',
    ],
)
async def test_operator_preflight_error_preserves_form(postgres_database, monkeypatch, code):
    from app.services.bs_traffic import operator_binding

    async with postgres_session(postgres_database, [Tariff.__table__]) as db:
        state = await opened(db, monkeypatch)
        await action(db, state, 'enabled')
        await action(db, state, 's1')
        before = await state.get_data()
        monkeypatch.setattr(
            operator_binding,
            'prepare_operator_settings',
            AsyncMock(side_effect=operator_binding.OperatorSettingsError(code)),
        )
        cb = await action(db, state, 'save')
        assert await state.get_data() == before
        assert await state.get_state() == ui.BsEditor.editing.state
        assert (await db.get(Tariff, 1)).bs_traffic_settings is None
        assert cb.answer.call_args.kwargs['show_alert'] is True
        assert cb.answer.call_args.args[0] != ui._t(USER, 'INVALID')
        assert code not in cb.answer.call_args.args[0]
