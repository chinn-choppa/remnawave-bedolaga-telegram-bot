"""Selected-subscription daily controls with explicit, durable paid consent."""

from datetime import UTC, datetime
from html import escape
from uuid import UUID, uuid4

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import select

from app.config import settings
from app.database.models import BsTrafficLedger, Subscription
from app.handlers.subscription.split_traffic import _UNAVAILABLE, purchase_reader
from app.localization.texts import get_texts
from app.services.bs_traffic.daily_control import (
    DailyResumeConsentRequired,
    DailyResumeQuoteChanged,
    find_daily_resume,
    find_daily_resume_by_operation,
    set_daily_pause,
)
from app.services.bs_traffic.daily_control_checkout import checkout_daily_resume, prepare_daily_resume
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.renewal import RenewalBalanceError
from app.utils.formatting import format_price_kopeks


_KEY = 'split_daily_confirmation'
_ATTEMPT = 'split_daily_attempted'


def _valid(saved, user_id):
    try:
        return (
            isinstance(saved, dict)
            and saved['user_id'] == user_id
            and type(saved['subscription_id']) is int
            and 0 < saved['subscription_id'] <= 2_147_483_647
            and type(saved['price_kopeks']) is int
            and saved['price_kopeks'] >= 0
            and len(saved['terms_hash']) == 64
            and all(c in '0123456789abcdef' for c in saved['terms_hash'])
            and str(UUID(saved['operation_key'])) == saved['operation_key']
        )
    except (KeyError, ValueError, TypeError, AttributeError):
        return False


def _keyboard(subscription_id, texts, *, saved=None, confirm=False, retry=False):
    rows = []
    if saved:
        rows.append(
            [
                InlineKeyboardButton(
                    text=texts.t(
                        'SPLIT_DAILY_RETRY_CONFIRM'
                        if retry
                        else 'SPLIT_DAILY_CONFIRM'
                        if confirm
                        else 'SPLIT_DAILY_CHECK'
                    ),
                    callback_data=f'{"sdr" if confirm else "sdc"}:{subscription_id}:{saved["operation_key"]}',
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=texts.t('MY_SUBSCRIPTION_BUTTON', '📱 Моя подписка'),
                callback_data=f'sm:{subscription_id}' if settings.is_multi_tariff_enabled() else 'menu_subscription',
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def daily_resume_needed(subscription):
    """LIMITED alone says nothing about the paid daily window or its pause flag."""
    end = subscription.end_date
    if end is not None and end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    return bool(
        subscription.is_daily_paused
        or subscription.actual_status in ('expired', 'disabled')
        or (end is not None and end <= datetime.now(UTC))
    )


async def show_split_daily_menu(callback, user, subscription):
    texts = get_texts(user.language)
    resume = daily_resume_needed(subscription)
    await callback.message.edit_text(
        texts.t('SPLIT_DAILY_MENU'),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=texts.t('SPLIT_DAILY_RESUME_BUTTON' if resume else 'SPLIT_DAILY_PAUSE_BUTTON'),
                        callback_data=f'sdp:{subscription.id}:{0 if resume else 1}',
                    )
                ]
            ]
        ),
    )
    await callback.answer()


async def handle_split_daily_control(callback, db_user, db, state):
    texts = get_texts(db_user.language)
    try:
        prefix, raw_id, action = callback.data.split(':')
        sid = int(raw_id)
        if prefix != 'sdp' or str(sid) != raw_id or not 0 < sid <= 2_147_483_647 or action not in ('0', '1'):
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        await callback.answer(texts.t('SPLIT_DAILY_EXPIRED'), show_alert=True)
        return
    sub = await db.scalar(select(Subscription).where(Subscription.id == sid, Subscription.user_id == db_user.id))
    if sub is None or sub.traffic_accounting_mode not in ('split', 'split_pending'):
        await callback.answer(texts.t('SPLIT_DAILY_UNAVAILABLE'), show_alert=True)
        return
    await show_split_daily_control(callback, db_user, db, state, subscription=sub, paused=action == '1')


async def show_split_daily_control(callback, user, db, state, *, subscription, paused=False):
    texts = get_texts(user.language)
    uid, sid = user.id, subscription.id
    data = await state.get_data()
    previous = data.get(_KEY)
    if not paused and data.get(_ATTEMPT) and _valid(previous, uid):
        await callback.message.edit_text(
            texts.t('SPLIT_DAILY_RETRY'), reply_markup=_keyboard(previous['subscription_id'], texts, saved=previous)
        )
        await callback.answer()
        return
    generation = await db.scalar(
        select(BsTrafficLedger.account_generation).where(BsTrafficLedger.subscription_id == sid)
    )
    try:
        result = await set_daily_pause(
            db, user_id=uid, subscription_id=sid, account_generation=generation, paused=paused
        )
        await db.commit()
        await callback.message.edit_text(
            texts.t('SPLIT_DAILY_PAUSED' if result.is_paused else 'SPLIT_DAILY_UNPAUSED'),
            reply_markup=_keyboard(sid, texts),
        )
        await callback.answer()
        return
    except DailyResumeConsentRequired:
        await db.rollback()
    except LedgerConflict:
        await db.rollback()
        await callback.answer(texts.t('SPLIT_DAILY_UNAVAILABLE'), show_alert=True)
        return
    try:
        async with purchase_reader(uid, sid) as reader:
            quote = await prepare_daily_resume(reader, user_id=uid, subscription_id=sid)
        saved = dict(
            user_id=uid,
            subscription_id=sid,
            price_kopeks=quote.price_kopeks,
            terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        await state.update_data(**{_KEY: saved, _ATTEMPT: False})
        await callback.message.edit_text(
            texts.t('SPLIT_DAILY_PREVIEW').format(
                tariff=escape(quote.tariff_name), amount=format_price_kopeks(quote.price_kopeks)
            ),
            reply_markup=_keyboard(sid, texts, saved=saved, confirm=True),
            parse_mode='HTML',
        )
        await callback.answer()
    except (LedgerConflict, *_UNAVAILABLE):
        await callback.answer(texts.t('SPLIT_DAILY_UNAVAILABLE'), show_alert=True)


async def _render(callback, texts, saved, result):
    text = texts.t('SPLIT_DAILY_PAID').format(
        amount=format_price_kopeks(result.charged_kopeks), date=result.new_end_date.strftime('%d.%m.%Y %H:%M')
    )
    if result.delivery_pending:
        text += '\n\n' + texts.t('SPLIT_DAILY_PENDING')
    try:
        await callback.answer()
    except Exception:
        pass
    await callback.message.edit_text(
        text,
        reply_markup=_keyboard(saved['subscription_id'], texts, saved=saved if result.delivery_pending else None),
        parse_mode='HTML',
    )


async def handle_split_daily_confirm(callback, db_user, db, state):
    uid = db_user.id
    texts = get_texts(db_user.language)
    try:
        prefix, raw_id, key = callback.data.split(':')
        sid = int(raw_id)
        if prefix not in ('sdr', 'sdc') or str(sid) != raw_id or not 0 < sid <= 2_147_483_647 or str(UUID(key)) != key:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        await callback.answer(texts.t('SPLIT_DAILY_EXPIRED'), show_alert=True)
        return
    saved = (await state.get_data()).get(_KEY)
    exact = prefix == 'sdr' and _valid(saved, uid) and saved['subscription_id'] == sid and saved['operation_key'] == key
    if not exact:
        try:
            result = await find_daily_resume_by_operation(db, user_id=uid, subscription_id=sid, operation_key=key)
            await db.commit()
        except LedgerConflict:
            await db.rollback()
            result = None
        if result is None:
            if (
                prefix == 'sdc'
                and _valid(saved, uid)
                and saved['subscription_id'] == sid
                and saved['operation_key'] == key
            ):
                # A read never pays. An explicit retry retains the original consent UUID.
                await callback.message.edit_text(
                    texts.t('SPLIT_DAILY_RETRY'),
                    reply_markup=_keyboard(sid, texts, saved=saved, confirm=True, retry=True),
                )
                await callback.answer()
            else:
                await callback.answer(texts.t('SPLIT_DAILY_EXPIRED'), show_alert=True)
            return
        if _valid(saved, uid) and saved['subscription_id'] == sid and saved['operation_key'] == key:
            await state.update_data(**{_ATTEMPT: result.delivery_pending})
        await _render(callback, texts, dict(subscription_id=sid, operation_key=key), result)
        return
    args = dict(
        user_id=uid,
        subscription_id=sid,
        operation_key=key,
        expected_price_kopeks=saved['price_kopeks'],
        expected_terms_hash=saved['terms_hash'],
    )
    try:
        result = await find_daily_resume(db, **args)
        await db.commit()
        if result is None:
            await state.update_data(**{_ATTEMPT: True})
            async with purchase_reader(uid, sid) as reader:
                result = await checkout_daily_resume(reader, **args)
    except (DailyResumeQuoteChanged, RenewalBalanceError) as error:
        await db.rollback()
        await state.update_data(**{_ATTEMPT: False, _KEY: None})
        await callback.answer(
            texts.t(
                'MINIAPP_PURCHASE_STATUS_INSUFFICIENT'
                if isinstance(error, RenewalBalanceError)
                else 'SPLIT_DAILY_EXPIRED'
            ),
            show_alert=True,
        )
        return
    except Exception:
        await db.rollback()
        await state.update_data(**{_ATTEMPT: True})
        await callback.message.edit_text(texts.t('SPLIT_DAILY_RETRY'), reply_markup=_keyboard(sid, texts, saved=saved))
        await callback.answer()
        return
    await state.update_data(**{_ATTEMPT: result.delivery_pending})
    await _render(callback, texts, saved, result)
