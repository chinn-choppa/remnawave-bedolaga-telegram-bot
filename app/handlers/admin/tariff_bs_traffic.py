"""BS tariff settings in the existing administrator FSM; save is explicit."""

import html
import re
import secrets
from copy import deepcopy
from decimal import Decimal

from aiogram import Dispatcher, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.crud.server_squad import get_all_server_squads
from app.database.crud.tariff import get_tariff_by_id, update_tariff
from app.database.models import User
from app.localization.texts import get_texts
from app.services.bs_traffic.operator_binding import OperatorSettingsError
from app.services.bs_traffic.tariff_settings import BsTrafficSettings, normalize_bs_settings
from app.services.tariff_edit_version import tariff_edit_version
from app.utils.decorators import admin_required, error_handler
from app.utils.fsm_selection import mutate_fsm_if_current


class BsEditor(StatesGroup):
    editing = State()


def parse_packages(text: str) -> dict[str, int]:
    if text.strip() == '-':
        return {}
    packages = {}
    for part in re.split(r'[;\n]', text):
        match = re.fullmatch(r'\s*([1-9][0-9]{0,5})\s*=\s*([0-9]+(?:[.,][0-9]{1,2})?)\s*', part)
        if not match or match[1] in packages:
            raise ValueError('Invalid package')
        packages[match[1]] = int(Decimal(match[2].replace(',', '.')) * 100)
    BsTrafficSettings(squad_uuids=['00000000-0000-4000-8000-000000000001'], limit_gb=0, topup_packages=packages)
    return packages


def _t(user, key):
    return get_texts(user.language).t('ADMIN_BS_' + key)


def render(form, user):
    value = form['value']

    def button(label, action):
        return InlineKeyboardButton(text=label, callback_data=f'bsf:{form["tariff_id"]}:{action}:{form["token"]}')

    rows = [[button(('✅ ' if form['enabled'] else '⬜ ') + _t(user, 'ENABLE'), 'enabled')]]
    for index, item in enumerate(form['catalog'][form['page'] * 8 : form['page'] * 8 + 8], form['page'] * 8):
        selected = item['uuid'] in value['squad_uuids']
        prefix = '✅ ' if selected else ('🔒 ' if item['uuid'] in form['ordinary'] else '⬜ ')
        rows.append([button(prefix + item['name'][:48], f's{index}')])
    nav = []
    if form['page']:
        nav.append(button('‹', f'p{form["page"] - 1}'))
    if (form['page'] + 1) * 8 < len(form['catalog']):
        nav.append(button('›', f'p{form["page"] + 1}'))
    if nav:
        rows.append(nav)
    for key in ('limit_gb', 'max_topup_gb', 'topup_packages'):
        rows.append([button(_t(user, key.upper()), key)])
    rows.append([button(('✅ ' if value['topup_enabled'] else '⬜ ') + _t(user, 'TOPUP'), 'topup')])
    rows.append([button(_t(user, 'SAVE'), 'save'), button(_t(user, 'CANCEL'), 'cancel')])
    packages = '; '.join(f'{gb}={price / 100:.2f}' for gb, price in value['topup_packages'].items()) or '—'
    text = _t(user, 'SUMMARY').format(
        name=html.escape(form['name']),
        count=len(value['squad_uuids']),
        limit=value['limit_gb'],
        maximum=value['max_topup_gb'],
        packages=html.escape(packages),
    )
    if form['field']:
        text += '\n\n' + _t(user, 'INPUT_PACKAGES' if form['field'] == 'topup_packages' else 'INPUT_NUMBER')
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def _expired(callback, user):
    await callback.answer(_t(user, 'EXPIRED'), show_alert=True)


@admin_required
@error_handler
async def start_bs_editor(callback: types.CallbackQuery, db_user: User, db: AsyncSession, state: FSMContext):
    old_state, data = await state.get_state(), await state.get_data()
    tariff = await get_tariff_by_id(db, int(callback.data.split(':')[1]))
    if tariff is None:
        return await _expired(callback, db_user)
    squads, _ = await get_all_server_squads(db, limit=10000)
    catalog = [{'uuid': s.squad_uuid, 'name': s.display_name or s.squad_uuid} for s in squads if s.squad_uuid]
    value = (
        deepcopy(tariff.bs_traffic_settings)
        if tariff.bs_traffic_settings
        else dict(
            squad_uuids=[],
            limit_gb=0,
            topup_enabled=False,
            topup_packages={},
            max_topup_gb=0,
        )
    )
    present = {s['uuid'] for s in catalog}
    catalog += [{'uuid': uid, 'name': '⚠ ' + uid} for uid in value['squad_uuids'] if uid not in present]
    form = dict(
        tariff_id=tariff.id,
        name=tariff.name,
        token=secrets.token_hex(4),
        message_id=callback.message.message_id,
        version=tariff_edit_version(tariff),
        enabled=bool(tariff.bs_traffic_settings) and tariff.bs_traffic_settings.get('enabled', True),
        value=value,
        ordinary=tariff.allowed_squads or [],
        catalog=catalog,
        page=0,
        field=None,
        saving=False,
    )
    if not await mutate_fsm_if_current(state, old_state, data, updates={'bs_form': form}, new_state=BsEditor.editing):
        return await _expired(callback, db_user)
    text, keyboard = render(form, db_user)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()
    return None


async def _save(callback, user, db, state, data, form):
    claimed = {**form, 'saving': True}
    if not await mutate_fsm_if_current(state, BsEditor.editing.state, data, updates={'bs_form': claimed}):
        return await _expired(callback, user)
    claimed_data = {**data, 'bs_form': claimed}
    committed = False
    try:
        tariff = await get_tariff_by_id(db, form['tariff_id'], for_update=True)
        if tariff is None or tariff_edit_version(tariff) != form['version']:
            await db.rollback()
            await callback.answer(_t(user, 'CONFLICT'), show_alert=True)
            return None
        value = normalize_bs_settings(
            dict(form['value'], enabled=True) if form['enabled'] else None, tariff.allowed_squads
        )
        squads, _ = await get_all_server_squads(db, limit=10000)
        if value and not set(value['squad_uuids']).issubset({s.squad_uuid for s in squads}):
            raise ValueError('Unavailable squad')
        from app.services.bs_traffic.operator_binding import prepare_operator_settings

        await prepare_operator_settings(db, settings=value, ordinary_squads=tariff.allowed_squads)
        await update_tariff(db, tariff, bs_traffic_settings=value)
        committed = True
    except OperatorSettingsError as error:
        await db.rollback()
        key = {
            'bs_topology_invalid': 'TOPOLOGY_INVALID',
            'bs_usage_config_invalid': 'USAGE_CONFIG_INVALID',
            'bs_panel_unavailable': 'PANEL_UNAVAILABLE',
            'bs_binding_mismatch': 'BINDING_MISMATCH',
            'bs_uncertain_policy_required': 'UNCERTAIN_POLICY_REQUIRED',
        }[error.code]
        await callback.answer(_t(user, key), show_alert=True)
        return None
    except ValueError:
        await db.rollback()
        await callback.answer(_t(user, 'INVALID'), show_alert=True)
        return None
    except Exception:
        await db.rollback()
        raise
    finally:
        await mutate_fsm_if_current(
            state,
            BsEditor.editing.state,
            claimed_data,
            **({'clear': True} if committed else {'updates': {'bs_form': form}}),
        )
    await _finish(callback, user, form, 'SAVED')
    return None


async def _finish(callback, user, form, key):
    await callback.message.edit_text(
        _t(user, key),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=_t(user, 'BACK'), callback_data=f'admin_tariff_view:{form["tariff_id"]}')]
            ]
        ),
    )
    await callback.answer()


@admin_required
@error_handler
async def bs_action(callback: types.CallbackQuery, db_user: User, db: AsyncSession, state: FSMContext):
    data = await state.get_data()
    form = data.get('bs_form')
    parts = callback.data.split(':')
    if (
        len(parts) != 4
        or await state.get_state() != BsEditor.editing.state
        or not isinstance(form, dict)
        or form['saving']
        or parts[1] != str(form['tariff_id'])
        or parts[3] != form['token']
        or callback.message.message_id != form['message_id']
    ):
        return await _expired(callback, db_user)
    action = parts[2]
    if action == 'save':
        return await _save(callback, db_user, db, state, data, form)
    if action == 'cancel':
        if not await mutate_fsm_if_current(state, BsEditor.editing.state, data, clear=True):
            return await _expired(callback, db_user)
        return await _finish(callback, db_user, form, 'CANCELLED')
    changed = deepcopy(form)
    changed['field'] = None
    if action == 'enabled':
        changed['enabled'] = not form['enabled']
    elif action == 'topup':
        changed['value']['topup_enabled'] = not form['value']['topup_enabled']
    elif action in ('limit_gb', 'max_topup_gb', 'topup_packages'):
        changed['field'] = action
    elif re.fullmatch(r'p[0-9]+', action) and int(action[1:]) * 8 < len(form['catalog']):
        changed['page'] = int(action[1:])
    elif re.fullmatch(r's[0-9]+', action) and int(action[1:]) < len(form['catalog']):
        uid = form['catalog'][int(action[1:])]['uuid']
        selected = set(form['value']['squad_uuids'])
        if uid in form['ordinary'] and uid not in selected:
            return await _expired(callback, db_user)
        selected.symmetric_difference_update({uid})
        changed['value']['squad_uuids'] = sorted(selected)
    else:
        return await _expired(callback, db_user)
    if not await mutate_fsm_if_current(state, BsEditor.editing.state, data, updates={'bs_form': changed}):
        return await _expired(callback, db_user)
    text, keyboard = render(changed, db_user)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()
    return None


@admin_required
@error_handler
async def bs_input(message: types.Message, db_user: User, db: AsyncSession, state: FSMContext):
    data = await state.get_data()
    form = data.get('bs_form')
    if not isinstance(form, dict) or form['saving'] or not form['field']:
        return
    changed = deepcopy(form)
    field = form['field']
    try:
        if field == 'topup_packages':
            value = parse_packages(message.text or '')
        else:
            raw = (message.text or '').strip()
            if not re.fullmatch(r'[0-9]{1,7}', raw) or int(raw) > 1_000_000:
                raise ValueError('Invalid limit')
            value = int(raw)
        changed['value'][field] = value
        if field == 'limit_gb' and value == 0:
            changed['value']['topup_enabled'] = False
    except ValueError:
        await message.answer(_t(db_user, 'INVALID_INPUT'))
        return
    changed['field'] = None
    if not await mutate_fsm_if_current(state, BsEditor.editing.state, data, updates={'bs_form': changed}):
        await message.answer(_t(db_user, 'EXPIRED'))
        return
    text, keyboard = render(changed, db_user)
    await message.bot.edit_message_text(
        chat_id=message.chat.id, message_id=form['message_id'], text=text, reply_markup=keyboard, parse_mode='HTML'
    )


def register_bs_handlers(dp: Dispatcher):
    dp.callback_query.register(start_bs_editor, F.data.startswith('admin_tariff_bs:'))
    dp.callback_query.register(bs_action, F.data.startswith('bsf:'))
    dp.message.register(bs_input, BsEditor.editing)
