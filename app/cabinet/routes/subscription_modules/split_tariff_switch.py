"""Cabinet adapter for confirmed split switches and offline receipt retries."""

from contextlib import asynccontextmanager

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.cabinet.schemas.subscription import DailyTariffSwitchMetadata
from app.config import settings
from app.database.crud.subscription import get_subscription_by_id_for_user
from app.database.models import BsTrafficLedger, BsTrafficRenewalIntent, Subscription, Tariff, User
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import bind_reader
from app.services.bs_traffic.renewal import RenewalBalanceError
from app.services.bs_traffic.tariff_switch import SwitchQuoteChanged, find_tariff_switch
from app.services.bs_traffic.tariff_switch_checkout import checkout_tariff_switch, prepare_tariff_switch
from app.services.remnawave_service import RemnaWaveService
from app.services.traffic_accounting_service import traffic_accounting_summary

from .helpers import _subscription_to_response


def _error(error):
    if isinstance(error, (RemnaWaveAPIError, TimeoutError)):
        return HTTPException(
            503,
            detail={
                'code': 'split_switch_temporarily_unavailable',
                'message': 'Панель временно недоступна. Повторите запрос с тем же подтверждением.',
            },
        )
    if isinstance(error, SwitchQuoteChanged):
        return HTTPException(
            409,
            detail={
                'code': 'split_switch_quote_changed',
                'message': 'Условия изменились. Подтвердите новую стоимость.',
            },
        )
    if isinstance(error, RenewalBalanceError):
        return HTTPException(402, detail={'code': 'insufficient_funds', 'message': 'Недостаточно средств на балансе.'})
    return HTTPException(
        409,
        detail={
            'code': 'split_switch_unavailable',
            'message': 'Смена тарифа сейчас недоступна. Повторите запрос с тем же подтверждением.',
        },
    )


@asynccontextmanager
async def _reader(db, user_id, subscription_id):
    scope = await db.scalar(
        select(BsTrafficLedger.panel_scope)
        .join(Subscription)
        .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
    )
    if scope is None:
        raise LedgerConflict('Owned split ledger required')
    factory = async_sessionmaker(db.bind, expire_on_commit=False)
    await db.rollback()
    async with RemnaWaveService().get_api_client() as api:
        yield await bind_reader(factory, api, panel_scope=scope)


def _request(user_id, subscription_id, request):
    if (
        getattr(request, 'operation_id', None) is None
        or getattr(request, 'expected_price_kopeks', None) is None
        or getattr(request, 'expected_terms_hash', None) is None
    ):
        raise HTTPException(
            409,
            detail={
                'code': 'split_switch_confirmation_required',
                'message': 'Сначала подтвердите стоимость смены тарифа.',
            },
        )
    return dict(
        user_id=user_id,
        subscription_id=subscription_id,
        new_tariff_id=request.tariff_id,
        expected_price_kopeks=request.expected_price_kopeks,
        expected_terms_hash=request.expected_terms_hash,
        operation_key=str(request.operation_id),
    )


async def _response(db, *, user_id, subscription_id, result):
    await db.rollback()
    sub = await get_subscription_by_id_for_user(db, subscription_id, user_id)
    if sub is None:
        raise HTTPException(404, detail='Subscription not found')
    user = await db.get(User, user_id, populate_existing=True)
    receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
    terms = receipt.parameters['switch_terms']
    old = await db.get(Tariff, terms['current_tariff_id'])
    new = await db.get(Tariff, terms['tariff_id'])
    accounting = await traffic_accounting_summary(db, subscription_id)
    return {
        'success': True,
        'message': 'Тариф изменён.',
        'subscription': _subscription_to_response(sub, user=user, traffic_accounting=accounting),
        'old_tariff_name': old.name if old else '',
        'new_tariff_id': terms['tariff_id'],
        'new_tariff_name': new.name if new else '',
        'charged_kopeks': result.charged_kopeks,
        'balance_kopeks': user.balance_kopeks,
        'balance_label': settings.format_price(user.balance_kopeks),
        'delivery_pending': result.delivery_pending,
        'replayed': result.replayed,
    }


async def historical_response(db, *, user_id, subscription_id, request):
    if (
        getattr(request, 'operation_id', None) is None
        or getattr(request, 'expected_price_kopeks', None) is None
        or getattr(request, 'expected_terms_hash', None) is None
    ):
        return None
    try:
        result = await find_tariff_switch(db, **_request(user_id, subscription_id, request))
    except LedgerConflict as error:
        raise _error(error) from error
    if result is None:
        return None
    return await _response(db, user_id=user_id, subscription_id=subscription_id, result=result)


async def preview(db, *, user_id, subscription_id, new_tariff_id):
    try:
        await db.rollback()
        async with _reader(db, user_id, subscription_id) as reader:
            quote = await prepare_tariff_switch(
                reader, user_id=user_id, subscription_id=subscription_id, new_tariff_id=new_tariff_id
            )
    except (LedgerConflict, RemnaWaveAPIError, TimeoutError) as error:
        raise _error(error) from error
    user = await db.get(User, user_id, populate_existing=True)
    old = await db.get(Tariff, quote.current_tariff_id)
    new = await db.get(Tariff, new_tariff_id)
    balance = user.balance_kopeks or 0
    missing = max(0, quote.price_kopeks - balance)
    return {
        **DailyTariffSwitchMetadata(
            billing_kind=quote.billing_kind,
            source_daily_rate_kopeks=quote.source_daily_rate_kopeks,
            target_daily_rate_kopeks=quote.target_daily_rate_kopeks,
            paid_window_end=quote.paid_window_end,
        ).model_dump(mode='json'),
        'can_switch': not missing,
        'current_tariff_id': quote.current_tariff_id,
        'current_tariff_name': old.name if old else '',
        'new_tariff_id': new_tariff_id,
        'new_tariff_name': new.name if new else '',
        'remaining_days': quote.remaining_days,
        'upgrade_cost_kopeks': quote.price_kopeks,
        'upgrade_cost_label': settings.format_price(quote.price_kopeks) if quote.price_kopeks else 'Бесплатно',
        'balance_kopeks': balance,
        'balance_label': settings.format_price(balance, round_kopeks=False),
        'has_enough_balance': not missing,
        'missing_amount_kopeks': missing,
        'missing_amount_label': settings.format_price(missing, round_kopeks=False) if missing else '',
        'is_upgrade': quote.is_upgrade,
        'traffic_accounting_mode': 'split',
        # Frozen quote values: zero denotes unlimited only with that mode.
        'split_limits': {
            'ordinary': {'mode': quote.ordinary_mode, 'limit_gb': quote.ordinary_limit_gb},
            'bs': {'mode': quote.bs_mode, 'limit_gb': quote.bs_limit_gb},
        },
        'device_limit': quote.device_limit,
        'expected_terms_hash': quote.terms_hash,
        'expected_end_date': quote.expected_end_date.isoformat(),
    }


async def execute(db, *, user_id, subscription_id, request):
    parameters = _request(user_id, subscription_id, request)
    try:
        await db.rollback()
        async with _reader(db, user_id, subscription_id) as reader:
            result = await checkout_tariff_switch(reader, **parameters)
    except (LedgerConflict, RemnaWaveAPIError, TimeoutError) as error:
        raise _error(error) from error
    return await _response(db, user_id=user_id, subscription_id=subscription_id, result=result)
