"""Telegram adapter for the shared owned split-traffic quote and checkout."""

from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from aiogram import types
from sqlalchemy import select

from app.database.database import AsyncSessionLocal
from app.database.models import BsTrafficLedger, Subscription, Tariff
from app.external.remnawave_api import RemnaWaveAPIError
from app.localization.texts import get_texts
from app.services.bs_traffic.checkout import checkout_package
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import bind_reader
from app.services.bs_traffic.quote_preparation import prepare_package_quote
from app.services.bs_traffic.tariff_settings import BsTrafficSettings
from app.services.remnawave_service import RemnaWaveConfigurationError, RemnaWaveService


_UNAVAILABLE = (RemnaWaveAPIError, RemnaWaveConfigurationError, TimeoutError)


async def _scope(db, user_id, subscription_id):
    scope = await db.scalar(
        select(BsTrafficLedger.panel_scope)
        .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            Subscription.traffic_accounting_mode == 'split',
        )
    )
    if scope is None:
        raise LedgerConflict('Owned split subscription required')
    from app.services.bs_traffic.runtime import runtime_ready

    if not runtime_ready(scope):
        raise TimeoutError('Split delivery unavailable')
    return scope


@asynccontextmanager
async def purchase_reader(user_id, subscription_id):
    async with AsyncSessionLocal() as db:
        scope = await _scope(db, user_id, subscription_id)
    # No SQL transaction is held across panel HTTP.
    async with RemnaWaveService().get_api_client() as api:
        yield await bind_reader(AsyncSessionLocal, api, panel_scope=scope)


def _keyboard(rows, texts):
    return types.InlineKeyboardMarkup(
        inline_keyboard=rows + [[types.InlineKeyboardButton(text=texts.BACK, callback_data='back_to_menu')]]
    )


async def show_split_packages(callback, db_user, subscription_id, group=None):
    texts = get_texts(db_user.language)
    try:
        async with AsyncSessionLocal() as db:
            await _scope(db, db_user.id, subscription_id)
            tariff = await db.scalar(
                select(Tariff)
                .join(Subscription, Subscription.tariff_id == Tariff.id)
                .where(Subscription.id == subscription_id, Subscription.user_id == db_user.id)
            )
            packages = {'ordinary': [], 'bs': []}
            if tariff is not None:
                if tariff.traffic_topup_enabled and tariff.traffic_limit_gb:
                    packages['ordinary'] = [
                        gb for gb, price in tariff.get_traffic_topup_packages().items() if price > 0
                    ]
                if tariff.bs_traffic_settings:
                    bs = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
                    if bs.enabled and bs.topup_enabled and bs.limit_gb:
                        packages['bs'] = [int(gb) for gb, price in bs.topup_packages.items() if price > 0]
        rows = []
        if group is None:
            for name, sizes in packages.items():
                if sizes:
                    rows.append(
                        [
                            types.InlineKeyboardButton(
                                text=texts.t('SPLIT_BUY_' + name.upper()), callback_data=f'spg:{subscription_id}:{name}'
                            )
                        ]
                    )
        else:
            for gb in sorted(packages.get(group, [])):
                rows.append(
                    [
                        types.InlineKeyboardButton(
                            text=texts.format_traffic(gb), callback_data=f'spq:{subscription_id}:{group}:{gb}'
                        )
                    ]
                )
        await callback.message.edit_text(
            texts.t('SPLIT_BUY_SELECT' if rows else 'SPLIT_BUY_UNAVAILABLE'), reply_markup=_keyboard(rows, texts)
        )
        await callback.answer()
    except (LedgerConflict, *_UNAVAILABLE):
        await callback.answer(texts.t('SPLIT_BUY_UNAVAILABLE'), show_alert=True)


async def handle_split_group(callback, db_user):
    try:
        _, sub_id, group = callback.data.split(':')
        if group not in ('ordinary', 'bs') or int(sub_id) <= 0:
            raise ValueError
    except (ValueError, AttributeError):
        await callback.answer()
        return
    await show_split_packages(callback, db_user, int(sub_id), group)


async def handle_split_quote(callback, db_user, state):
    texts = get_texts(db_user.language)
    saved = await state.get_data()
    previous = saved.get('split_traffic_purchase')
    if saved.get('split_traffic_attempted') and previous and previous.get('user_id') == db_user.id:
        # An ambiguous earlier debit must be resolved before issuing another consent UUID.
        rows = [
            [
                types.InlineKeyboardButton(
                    text=texts.t('SPLIT_BUY_CONFIRM'), callback_data='spc:' + previous['operation_key']
                )
            ]
        ]
        await callback.message.edit_text(texts.t('SPLIT_BUY_RETRY'), reply_markup=_keyboard(rows, texts))
        await callback.answer()
        return
    try:
        _, sub_id, group, gb = callback.data.split(':')
        async with purchase_reader(db_user.id, int(sub_id)) as reader:
            quote = await prepare_package_quote(
                reader, user_id=db_user.id, subscription_id=int(sub_id), group=group, gb=int(gb)
            )
        intent = dict(
            user_id=db_user.id,
            subscription_id=int(sub_id),
            group=group,
            gb=int(gb),
            expected_price_kopeks=quote.price_kopeks,
            operation_key=uuid4().hex,
        )
        # Save before displaying confirmation. A failed Telegram edit cannot create an unrecorded debit.
        await state.update_data(split_traffic_purchase=intent)
        rows = [
            [
                types.InlineKeyboardButton(
                    text=texts.t('SPLIT_BUY_CONFIRM'), callback_data='spc:' + intent['operation_key']
                )
            ]
        ]
        if quote.missing_kopeks:
            rows = [[types.InlineKeyboardButton(text=texts.BALANCE_TOP_UP, callback_data='balance_topup')]]
        await callback.message.edit_text(
            texts.t('SPLIT_BUY_QUOTE').format(
                group=texts.t('SPLIT_BUY_' + group.upper()),
                gb=int(gb),
                price=texts.format_price(quote.price_kopeks),
                balance=texts.format_price(quote.balance_kopeks),
            ),
            reply_markup=_keyboard(rows, texts),
        )
        await callback.answer()
    except (LedgerConflict, ValueError):
        await callback.answer(texts.t('SPLIT_BUY_CHANGED'), show_alert=True)
    except _UNAVAILABLE:
        await callback.answer(texts.t('SPLIT_BUY_UNAVAILABLE'), show_alert=True)


async def handle_split_confirm(callback, db_user, state):
    texts = get_texts(db_user.language)
    intent = (await state.get_data()).get('split_traffic_purchase')
    if not intent or intent.get('user_id') != db_user.id or callback.data != 'spc:' + intent['operation_key']:
        await callback.answer(texts.t('SPLIT_BUY_CHANGED'), show_alert=True)
        return
    try:
        UUID(intent['operation_key'])
        await state.update_data(split_traffic_attempted=True)
        async with purchase_reader(db_user.id, intent['subscription_id']) as reader:
            result = await checkout_package(reader, **intent)
    except (LedgerConflict, ValueError):
        await state.update_data(split_traffic_attempted=False)
        await callback.answer(texts.t('SPLIT_BUY_CHANGED'), show_alert=True)
        return
    except _UNAVAILABLE:
        # Keep the exact consent and UUID: an ambiguous timeout may already have committed.
        await callback.answer(texts.t('SPLIT_BUY_RETRY'), show_alert=True)
        return
    await callback.message.edit_text(
        texts.t('SPLIT_BUY_PAID' if result.delivery_status == 'verified' else 'SPLIT_BUY_PENDING'),
        reply_markup=_keyboard([], texts),
    )
    await callback.answer()
    await state.update_data(split_traffic_attempted=False)
    # Retain receipt for duplicate callbacks and lost Telegram responses. Never mint a retry UUID here.
