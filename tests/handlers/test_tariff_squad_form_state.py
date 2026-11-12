"""Server selection stays in the existing FSM until explicit save."""

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage

from app.database.models import Tariff
from app.handlers.admin import tariffs as handlers
from app.services.subscription_service import SubscriptionService
from tests.services.test_traffic_accounting_store import database


A = '00000000-0000-4000-8000-000000000001'
B = '00000000-0000-4000-8000-000000000002'


def callback(data):
    return SimpleNamespace(
        data=data, message=SimpleNamespace(message_id=20, edit_text=AsyncMock(), answer=AsyncMock()), answer=AsyncMock()
    )


def state():
    return FSMContext(storage=MemoryStorage(), key=StorageKey(bot_id=1, chat_id=1, user_id=1))


async def invoke(name, cb, db, fsm):
    form = (await fsm.get_data()).get('tariff_squad_form')
    if form and name != 'start_edit_tariff_squads':
        cb.data += ':' + form['token']
    return await inspect.unwrap(getattr(handlers, name))(cb, SimpleNamespace(id=1, language='ru'), db, fsm)


async def prepare(monkeypatch, db, fsm):
    db.add(Tariff(id=1, name='Test', period_prices={'30': 100}, allowed_squads=[A]))
    await db.commit()
    monkeypatch.setattr(
        handlers,
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
    monkeypatch.setattr(handlers, 'view_tariff', AsyncMock())
    propagate = AsyncMock(return_value=SimpleNamespace(failed_ids=[], total=0))
    monkeypatch.setattr(SubscriptionService, 'propagate_tariff_squads', propagate)
    await invoke('start_edit_tariff_squads', callback('admin_tariff_edit_squads:1'), db, fsm)
    return propagate


async def test_toggle_and_bulk_actions_do_not_change_live_tariff_before_save(monkeypatch):
    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        await invoke('clear_tariff_squads', callback('admin_tariff_clear_squads:1'), db, fsm)
        await invoke('select_all_tariff_squads', callback('admin_tariff_select_all_squads:1'), db, fsm)
        assert (await db.get(Tariff, 1)).allowed_squads == [A]
        propagate.assert_not_awaited()
        await invoke('save_tariff_squads', callback('admin_tariff_save_squads:1'), db, fsm)
        assert set((await db.get(Tariff, 1)).allowed_squads) == {A, B}
        assert await fsm.get_state() is None
        assert propagate.await_count == 1
        await invoke('save_tariff_squads', callback('admin_tariff_save_squads:1'), db, fsm)
        assert propagate.await_count == 1


async def test_conflicting_cabinet_edit_does_not_get_overwritten(monkeypatch):
    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        tariff = await db.get(Tariff, 1)
        tariff.name = 'Changed elsewhere'
        await db.commit()
        cb = callback('admin_tariff_save_squads:1')
        await invoke('save_tariff_squads', cb, db, fsm)
        assert (await db.get(Tariff, 1)).allowed_squads == [A]
        propagate.assert_not_awaited()
        assert cb.answer.call_args.kwargs['show_alert'] is True


async def test_cancel_and_old_message_cannot_change_current_selection(monkeypatch):
    async with database(monkeypatch) as db:
        fsm = state()
        await prepare(monkeypatch, db, fsm)
        before = await fsm.get_data()
        cb = callback(f'trf_sq:1:{B}')
        cb.message.message_id = 19
        await invoke('toggle_tariff_squad', cb, db, fsm)
        assert await fsm.get_data() == before
        monkeypatch.setattr(handlers, 'view_tariff', AsyncMock())
        await invoke('cancel_tariff_squads', callback('admin_tariff_cancel_squads:1'), db, fsm)
        assert await fsm.get_state() is None
        assert (await db.get(Tariff, 1)).allowed_squads == [A]


async def test_saved_selection_propagates_even_when_telegram_render_fails(monkeypatch):
    import pytest

    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        monkeypatch.setattr(handlers, 'view_tariff', AsyncMock(side_effect=RuntimeError('Telegram unavailable')))
        with pytest.raises(RuntimeError, match='Telegram unavailable'):
            await invoke('save_tariff_squads', callback('admin_tariff_save_squads:1'), db, fsm)
        assert set((await db.get(Tariff, 1)).allowed_squads) == {A, B}
        propagate.assert_awaited_once()


async def test_unchanged_selection_does_not_propagate(monkeypatch):
    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('save_tariff_squads', callback('admin_tariff_save_squads:1'), db, fsm)
        propagate.assert_not_awaited()
        assert await fsm.get_state() is None


async def test_failed_write_keeps_selection_for_retry(monkeypatch):
    import pytest

    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        before = await fsm.get_data()
        original = handlers.update_tariff
        monkeypatch.setattr(handlers, 'update_tariff', AsyncMock(side_effect=RuntimeError('write failed')))
        with pytest.raises(RuntimeError, match='write failed'):
            await invoke('save_tariff_squads', callback('admin_tariff_save_squads:1'), db, fsm)
        assert await fsm.get_data() == before
        assert (await db.get(Tariff, 1)).allowed_squads == [A]
        propagate.assert_not_awaited()
        monkeypatch.setattr(handlers, 'update_tariff', original)
        await invoke('save_tariff_squads', callback('admin_tariff_save_squads:1'), db, fsm)
        propagate.assert_awaited_once()


async def test_deleted_server_rejects_save_without_losing_selection(monkeypatch):
    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        before = await fsm.get_data()
        monkeypatch.setattr(
            handlers, 'get_all_server_squads', AsyncMock(return_value=([SimpleNamespace(squad_uuid=A)], 1))
        )
        cb = callback('admin_tariff_save_squads:1')
        await invoke('save_tariff_squads', cb, db, fsm)
        assert await fsm.get_data() == before
        assert (await db.get(Tariff, 1)).allowed_squads == [A]
        propagate.assert_not_awaited()
        assert cb.answer.call_args.kwargs['show_alert']


async def test_old_menu_token_cannot_save_reopened_form(monkeypatch):
    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        old = (await fsm.get_data())['tariff_squad_form']['token']
        await invoke('start_edit_tariff_squads', callback('admin_tariff_edit_squads:1'), db, fsm)
        before = await fsm.get_data()
        cb = callback(f'admin_tariff_save_squads:1:{old}')
        await inspect.unwrap(handlers.save_tariff_squads)(cb, SimpleNamespace(id=1, language='ru'), db, fsm)
        assert await fsm.get_data() == before
        propagate.assert_not_awaited()
        assert cb.answer.call_args.kwargs['show_alert']


async def test_parallel_save_claims_form_once(monkeypatch):
    import asyncio

    async with database(monkeypatch) as db:
        fsm = state()
        propagate = await prepare(monkeypatch, db, fsm)
        await invoke('toggle_tariff_squad', callback(f'trf_sq:1:{B}'), db, fsm)
        token = (await fsm.get_data())['tariff_squad_form']['token']
        entered, release = asyncio.Event(), asyncio.Event()
        original = handlers.get_tariff_by_id

        async def blocked_read(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(handlers, 'get_tariff_by_id', blocked_read)
        save = inspect.unwrap(handlers.save_tariff_squads)
        user = SimpleNamespace(id=1, language='ru')
        first = asyncio.create_task(save(callback(f'admin_tariff_save_squads:1:{token}'), user, db, fsm))
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            second = callback(f'admin_tariff_save_squads:1:{token}')
            await save(second, user, db, fsm)
            assert second.answer.call_args.kwargs['show_alert']
        finally:
            release.set()
            await first
        propagate.assert_awaited_once()


async def test_keyboard_callback_lengths_and_all_locales():
    for language in ('ru', 'en', 'ua', 'zh', 'fa'):
        cb = callback('unused')
        await handlers._render_tariff_squad_form(
            cb,
            SimpleNamespace(language=language),
            {'tariff_id': 2147483647, 'token': '12345678', 'selected': [A], 'name': '<Test>'},
            [SimpleNamespace(squad_uuid=A, display_name='A')],
        )
        args = cb.message.edit_text.call_args
        assert 'ADMIN_TARIFF_' not in args.args[0]
        assert '&lt;Test&gt;' in args.args[0]
        for row in args.kwargs['reply_markup'].inline_keyboard:
            for button in row:
                assert len(button.callback_data.encode()) <= 64
                assert 'ADMIN_TARIFF_' not in button.text
