"""Telegram consent and offline receipt replay for the shared split switch."""

from html import escape
from uuid import UUID, uuid4

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
from app.database.models import Tariff
from app.handlers.subscription.split_traffic import _UNAVAILABLE, purchase_reader
from app.localization.texts import get_texts
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.renewal import RenewalBalanceError
from app.services.bs_traffic.tariff_switch import SwitchQuoteChanged, find_tariff_switch
from app.services.bs_traffic.tariff_switch_checkout import checkout_tariff_switch, prepare_tariff_switch
from app.services.bs_traffic.tariff_switch_receipt import find_tariff_switch_by_operation
from app.utils.formatting import format_price_kopeks


_KEY = 'split_tariff_switch_confirmation'


def _valid(saved, user_id):
    if not isinstance(saved, dict):
        return False
    try:
        return (
            saved.get('user_id') == user_id
            and type(saved.get('subscription_id')) is int
            and saved['subscription_id'] > 0
            and type(saved.get('new_tariff_id')) is int
            and saved['new_tariff_id'] > 0
            and type(saved.get('price_kopeks')) is int
            and saved['price_kopeks'] >= 0
            and isinstance(saved.get('terms_hash'), str)
            and len(saved['terms_hash']) == 64
            and all(c in '0123456789abcdef' for c in saved['terms_hash'])
            and str(UUID(saved['operation_key'])) == saved['operation_key']
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _keyboard(saved, texts, *, confirm=False, pending=False):
    rows = []
    if confirm or pending:
        rows.append(
            [
                InlineKeyboardButton(
                    text=texts.t('SPLIT_SWITCH_CONFIRM' if confirm else 'SPLIT_SWITCH_CHECK'),
                    callback_data=f'swt:{saved["subscription_id"]}:{saved["operation_key"]}',
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=texts.t('MY_SUBSCRIPTION_BUTTON', '📱 Моя подписка'),
                callback_data=f'sm:{saved["subscription_id"]}'
                if settings.is_multi_tariff_enabled()
                else 'menu_subscription',
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def preview_split_tariff_switch(callback, user, db, state, *, subscription, new_tariff_id):
    if subscription is None:
        return False
    texts = get_texts(user.language)
    user_id, subscription_id = user.id, subscription.id
    data = await state.get_data()
    previous = data.get(_KEY)
    if data.get('split_tariff_switch_attempted') and _valid(previous, user_id):
        await callback.message.edit_text(
            texts.t('SPLIT_SWITCH_RETRY'),
            reply_markup=_keyboard(previous, texts, pending=True),
        )
        await callback.answer()
        return True
    if getattr(subscription, 'traffic_accounting_mode', None) not in ('split', 'split_pending'):
        # Explicit new ordinary consent replaces a resolved split confirmation.
        # Unknown/pending operations were retained by the guard above.
        if _valid(previous, user_id) and previous['subscription_id'] == subscription_id:
            await state.update_data(**{_KEY: None, 'split_tariff_switch_attempted': False})
        return False
    try:
        tariff = await db.get(Tariff, new_tariff_id)
        name = tariff.name if tariff else ''
        await db.commit()
        async with purchase_reader(user_id, subscription_id) as reader:
            quote = await prepare_tariff_switch(
                reader,
                user_id=user_id,
                subscription_id=subscription_id,
                new_tariff_id=new_tariff_id,
            )
        saved = dict(
            user_id=user_id,
            subscription_id=subscription_id,
            new_tariff_id=new_tariff_id,
            price_kopeks=quote.price_kopeks,
            terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        await state.update_data(**{_KEY: saved, 'split_tariff_switch_attempted': False})
        await callback.message.edit_text(
            texts.t('SPLIT_DAILY_SWITCH_PREVIEW' if quote.billing_kind == 'daily' else 'SPLIT_SWITCH_PREVIEW').format(
                tariff=escape(name),
                amount=format_price_kopeks(quote.price_kopeks),
                date=quote.expected_end_date.strftime('%d.%m.%Y %H:%M:%S UTC')
                if quote.billing_kind == 'daily'
                else quote.expected_end_date.strftime('%d.%m.%Y'),
                next_price=format_price_kopeks(quote.target_daily_rate_kopeks or 0),
                ordinary=texts.format_traffic(quote.ordinary_limit_gb, is_limit=quote.ordinary_mode == 'unlimited'),
                bs=texts.format_traffic(quote.bs_limit_gb, is_limit=quote.bs_mode == 'unlimited'),
                devices=texts.format_device_limit(quote.device_limit),
            ),
            reply_markup=_keyboard(saved, texts, confirm=True),
            parse_mode='HTML',
        )
        await callback.answer()
    except (LedgerConflict, *_UNAVAILABLE):
        await db.rollback()
        await callback.answer(texts.t('SPLIT_SWITCH_UNAVAILABLE'), show_alert=True)
    return True


async def _confirm(callback, user, db, state, saved):
    user_id = user.id
    texts = get_texts(user.language)
    if not _valid(saved, user_id):
        await callback.answer(texts.t('SPLIT_SWITCH_EXPIRED'), show_alert=True)
        return
    request = dict(
        user_id=user_id,
        subscription_id=saved['subscription_id'],
        new_tariff_id=saved['new_tariff_id'],
        expected_price_kopeks=saved['price_kopeks'],
        expected_terms_hash=saved['terms_hash'],
        operation_key=saved['operation_key'],
    )
    try:
        # Owned immutable receipt precedes current mode, tariff and panel gates.
        result = await find_tariff_switch(db, **request)
        await db.commit()
        if result is None:
            await state.update_data(split_tariff_switch_attempted=True)
            async with purchase_reader(user_id, saved['subscription_id']) as reader:
                result = await checkout_tariff_switch(reader, **request)
    except RenewalBalanceError:
        await db.rollback()
        await state.update_data(split_tariff_switch_attempted=False)
        await callback.answer(
            texts.t('MINIAPP_PURCHASE_STATUS_INSUFFICIENT', 'Недостаточно средств на балансе'), show_alert=True
        )
        return
    except SwitchQuoteChanged:
        await db.rollback()
        await state.update_data(split_tariff_switch_attempted=False)
        await callback.answer(texts.t('SPLIT_SWITCH_EXPIRED'), show_alert=True)
        return
    except LedgerConflict:
        await db.rollback()
        await state.update_data(split_tariff_switch_attempted=True)
        await callback.message.edit_text(
            texts.t('SPLIT_SWITCH_RETRY'),
            reply_markup=_keyboard(saved, texts, pending=True),
        )
        await callback.answer()
        return
    except Exception:
        # An unknown outcome retains the same UUID; retry resolves its receipt.
        await db.rollback()
        await callback.message.edit_text(
            texts.t('SPLIT_SWITCH_RETRY'), reply_markup=_keyboard(saved, texts, pending=True)
        )
        await callback.answer()
        return
    await state.update_data(split_tariff_switch_attempted=result.delivery_pending)
    await _render_result(callback, texts, saved, result)


async def _render_result(callback, texts, saved, result):
    message = texts.t('SPLIT_SWITCH_SUCCESS').format(
        amount=format_price_kopeks(result.charged_kopeks),
        date=result.new_end_date.strftime('%d.%m.%Y'),
    )
    if result.delivery_pending:
        message += '\n\n' + texts.t('SPLIT_SWITCH_PENDING')
    try:
        await callback.answer()
    except Exception:
        pass
    await callback.message.edit_text(
        message,
        reply_markup=_keyboard(saved, texts, pending=result.delivery_pending),
        parse_mode='HTML',
    )


async def handle_split_tariff_switch_confirm(callback, db_user, db, state):
    saved = (await state.get_data()).get(_KEY)
    texts = get_texts(db_user.language)
    try:
        parts = callback.data.split(':')
        if len(parts) == 2 and _valid(saved, db_user.id) and parts[1] == saved['operation_key']:
            # Earlier UUID-only buttons are supported while their FSM survives.
            subscription_id, operation_key = saved['subscription_id'], parts[1]
        elif len(parts) == 3:
            subscription_id, operation_key = int(parts[1]), parts[2]
            if str(subscription_id) != parts[1] or not 0 < subscription_id <= 2_147_483_647:
                raise ValueError
        else:
            raise ValueError
        if parts[0] != 'swt' or str(UUID(operation_key)) != operation_key:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        await callback.answer(texts.t('SPLIT_SWITCH_EXPIRED'), show_alert=True)
        return
    if (
        _valid(saved, db_user.id)
        and saved['subscription_id'] == subscription_id
        and saved['operation_key'] == operation_key
    ):
        await _confirm(callback, db_user, db, state, saved)
        return
    # Without exact consent, this branch can only read an existing payment.
    # It never opens a panel reader or attempts a fresh checkout.
    try:
        result = await find_tariff_switch_by_operation(
            db,
            user_id=db_user.id,
            subscription_id=subscription_id,
            operation_key=operation_key,
        )
        await db.commit()
    except LedgerConflict:
        await db.rollback()
        result = None
    if result is None:
        await callback.answer(texts.t('SPLIT_SWITCH_EXPIRED'), show_alert=True)
        return
    await _render_result(
        callback,
        texts,
        dict(subscription_id=subscription_id, operation_key=operation_key),
        result,
    )


async def clear_switch_navigation(state):
    data = await state.get_data()
    saved, attempted = data.get(_KEY), data.get('split_tariff_switch_attempted', False)
    await state.clear()
    if isinstance(saved, dict):
        await state.update_data(**{_KEY: saved, 'split_tariff_switch_attempted': attempted})


async def reject_legacy_split_confirmation(callback, user, subscription, state, *, new_tariff_id):
    saved = (await state.get_data()).get(_KEY)
    if getattr(subscription, 'traffic_accounting_mode', None) not in ('split', 'split_pending') and not (
        _valid(saved, user.id)
        and saved['subscription_id'] == subscription.id
        and saved['new_tariff_id'] == new_tariff_id
    ):
        return False
    # Older generic buttons have no consent UUID, including after reverse.
    # Explicit new ordinary preview clears resolved consent before reaching here.
    await callback.answer(get_texts(user.language).t('SPLIT_SWITCH_EXPIRED'), show_alert=True)
    return True


async def daily_switch_discovery(callback, user, db, state, subscription):
    """Daily split candidates never advertise the legacy zero/full-day calculation."""
    if getattr(subscription, 'traffic_accounting_mode', None) not in ('split', 'split_pending'):
        return False
    current_tariff = await db.get(Tariff, subscription.tariff_id) if subscription.tariff_id else None
    if current_tariff is None or not current_tariff.is_daily:
        return False
    from app.database.crud.subscription import get_active_subscriptions_by_user_id
    from app.database.crud.tariff import get_tariffs_for_user

    texts = get_texts(user.language)
    candidates = await get_tariffs_for_user(db, getattr(user, 'promo_group_id', None))
    owned = {subscription.tariff_id}
    if settings.is_multi_tariff_enabled():
        owned.update(item.tariff_id for item in await get_active_subscriptions_by_user_id(db, user.id))
    buttons = []
    if settings.TARIFF_SWITCH_UPGRADE_ENABLED or settings.TARIFF_SWITCH_DOWNGRADE_ENABLED:
        for tariff in candidates:
            if tariff.is_daily and tariff.id not in owned:
                buttons.append(
                    [InlineKeyboardButton(text=tariff.name, callback_data=f'instant_sw_preview:{tariff.id}')]
                )
    buttons.append(
        [
            InlineKeyboardButton(
                text=texts.t('MY_SUBSCRIPTION_BUTTON'),
                callback_data=f'sm:{subscription.id}' if settings.is_multi_tariff_enabled() else 'menu_subscription',
            )
        ]
    )
    await state.update_data(active_subscription_id=subscription.id)
    await callback.message.edit_text(
        texts.t('SPLIT_DAILY_SWITCH_SELECT'),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode='HTML',
    )
    await callback.answer()
    return True
