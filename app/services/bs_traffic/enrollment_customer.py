"""Truthful purchase acknowledgements while funded initial delivery is pending."""

import structlog

from app.localization.texts import get_texts
from app.services.bs_traffic.enrollment import pending_enrollment


logger = structlog.get_logger(__name__)


def pending_message(language):
    return get_texts(language).t('SPLIT_ENROLLMENT_PENDING')


async def notify_pending_if_needed(db, user, subscription_id, *, bot=None):
    if user is None:
        return False
    job = await pending_enrollment(db, subscription_id)
    if job is None or job.user_id != user.id:
        return False
    from app.services.notification_delivery_service import NotificationType, notification_delivery_service

    try:
        await notification_delivery_service.send_notification(
            user=user,
            notification_type=NotificationType.SUBSCRIPTION_PENDING,
            context={'delivery_pending': True, 'subscription_id': subscription_id},
            telegram_message=pending_message(user.language),
            bot=bot,
        )
    except Exception as error:
        logger.warning('Pending purchase acknowledgement failed', error_type=type(error).__name__)
    return True


async def show_pending_if_needed(callback, db, subscription_id, language):
    if await pending_enrollment(db, subscription_id) is None:
        return False
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    texts = get_texts(language)
    await callback.message.edit_text(
        pending_message(language),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=texts.t('MY_SUBSCRIPTION_BUTTON'), callback_data='menu_subscription')]
            ]
        ),
    )
    return True
