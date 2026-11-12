"""Telegram adapter for the shared, idempotent split renewal coordinator."""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import select

from app.config import settings
from app.database.models import PaymentMethod, Subscription
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.renewal import find_balance_renewal
from app.services.pricing_engine import RenewalPricing
from app.services.subscription_renewal_service import SubscriptionRenewalChargeError, SubscriptionRenewalService
from app.utils.formatting import format_price_kopeks


async def confirm_split_renewal(callback, user, db, state, *, subscription_id, tariff_id, period_days, texts):
    """Return False only for a legacy target; never debit through both paths."""
    subscription = await db.scalar(
        select(Subscription).where(Subscription.id == subscription_id, Subscription.user_id == user.id)
    )
    if subscription is None or subscription.traffic_accounting_mode not in ('split', 'split_pending'):
        return False
    saved = (await state.get_data()).get('split_renewal_confirmation')
    if (
        not isinstance(saved, dict)
        or saved.get('subscription_id') != subscription_id
        or saved.get('tariff_id') != tariff_id
        or saved.get('period_days') != period_days
        or type(saved.get('price_kopeks')) is not int
        or saved['price_kopeks'] < 0
        or not isinstance(saved.get('operation_key'), str)
    ):
        await callback.answer(
            texts.t('TARIFF_RENEW_SESSION_EXPIRED', 'Сессия устарела, откройте продление заново'), show_alert=True
        )
        return True
    price = saved['price_kopeks']
    try:
        # Receipt lookup precedes tariff availability, promo and balance checks.
        receipt = await find_balance_renewal(
            db,
            user_id=user.id,
            subscription_id=subscription_id,
            period_days=period_days,
            operation_key=saved['operation_key'],
            expected_price_kopeks=price,
        )
        if receipt is not None:
            await db.commit()
            pending, end_date = receipt.delivery_pending, receipt.new_end_date
        else:
            if subscription.tariff_id != tariff_id:
                raise LedgerConflict('Tariff changed')
            # Confirmed price is input only: the coordinator reprices under locks.
            pricing = RenewalPricing(0, 0, 0, 0, 0, 0, price, period_days, True)
            result = await SubscriptionRenewalService().finalize(
                db,
                user,
                subscription,
                pricing,
                operation_key=saved['operation_key'],
                payment_method=PaymentMethod.BALANCE,
            )
            pending, end_date = result.delivery_pending, result.subscription.end_date
    except SubscriptionRenewalChargeError:
        await db.rollback()
        await callback.answer(
            texts.t('MINIAPP_PURCHASE_STATUS_INSUFFICIENT', 'Недостаточно средств на балансе'), show_alert=True
        )
        return True
    except LedgerConflict:
        await db.rollback()
        await callback.answer(
            texts.t('TARIFF_RENEW_SESSION_EXPIRED', 'Сессия устарела, откройте продление заново'), show_alert=True
        )
        return True
    # Keep the confirmation receipt in FSM: duplicate callbacks remain replayable.
    # finalize owns all financial and panel side effects; this adapter only renders.
    try:
        await callback.answer()
    except Exception:
        pass  # A paid receipt remains valid after Telegram expires the query.
    message = texts.t(
        'SUBSCRIPTION_UI_RENEW_SUCCESS_DETAILS',
        '✅ Подписка успешно продлена!\n\n⏰ Добавлено: {days} дней\nДействует до: {date}\n\n💰 Списано: {amount}',
    ).format(days=period_days, date=end_date.strftime('%d.%m.%Y'), amount=format_price_kopeks(price))
    if pending:
        message += '\n\n' + texts.t(
            'SPLIT_RENEWAL_DELIVERY_PENDING',
            'Продление оплачено и сохранено. Применение настроек ещё выполняется. Повторная оплата не нужна.',
        )
    await callback.message.edit_text(
        message,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=texts.t('MY_SUBSCRIPTION_BUTTON', '📱 Моя подписка'),
                        callback_data=f'sm:{subscription_id}'
                        if settings.is_multi_tariff_enabled()
                        else 'menu_subscription',
                    )
                ]
            ]
        ),
        parse_mode='HTML',
    )
    return True
