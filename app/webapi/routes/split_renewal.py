"""Authenticated legacy MiniApp adapter for split balance renewal."""

from fastapi import HTTPException

from app.config import settings
from app.database.models import PaymentMethod
from app.localization.texts import get_texts
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.renewal import find_balance_renewal
from app.services.pricing_engine import RenewalPricing
from app.services.subscription_renewal_service import SubscriptionRenewalChargeError, SubscriptionRenewalService
from app.webapi.schemas.miniapp import MiniAppSubscriptionRenewalResponse


async def submit_split_renewal(db, user, subscription, payload, *, period_days):
    if payload.operation_id is None or payload.expected_price_kopeks is None:
        raise HTTPException(
            409, detail={'code': 'renewal_confirmation_required', 'message': 'Refresh and confirm renewal price'}
        )
    price = payload.expected_price_kopeks
    try:
        receipt = await find_balance_renewal(
            db,
            user_id=user.id,
            subscription_id=subscription.id,
            period_days=period_days,
            operation_key=str(payload.operation_id),
            expected_price_kopeks=price,
        )
        if receipt is not None:
            await db.commit()
            end_date, pending = receipt.new_end_date, receipt.delivery_pending
        else:
            pricing = RenewalPricing(0, 0, 0, 0, 0, 0, price, period_days, True)
            result = await SubscriptionRenewalService().finalize(
                db,
                user,
                subscription,
                pricing,
                payment_method=PaymentMethod.BALANCE,
                operation_key=str(payload.operation_id),
            )
            end_date, pending = result.subscription.end_date, result.delivery_pending
    except SubscriptionRenewalChargeError:
        await db.rollback()
        raise HTTPException(
            402, detail={'code': 'insufficient_funds', 'message': 'Top up your balance and retry the same confirmation'}
        ) from None
    except LedgerConflict:
        await db.rollback()
        raise HTTPException(
            409, detail={'code': 'renewal_confirmation_changed', 'message': 'Refresh and confirm renewal price'}
        ) from None
    await db.refresh(user)
    texts = get_texts(user.language)
    message = (
        texts.t('SPLIT_RENEWAL_DELIVERY_PENDING')
        if pending
        else texts.t('SUBSCRIPTION_UI_RENEW_SUCCESS_DETAILS').format(
            days=period_days, date=end_date.strftime('%d.%m.%Y'), amount=settings.format_price(price)
        )
    )
    return MiniAppSubscriptionRenewalResponse(
        subscription_id=subscription.id,
        renewed_until=end_date,
        delivery_pending=pending,
        balance_kopeks=user.balance_kopeks,
        balance_label=settings.format_price(user.balance_kopeks),
        message=message,
    )
