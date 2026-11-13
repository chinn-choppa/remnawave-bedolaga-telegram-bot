"""Explicit daily split consent and owned historical receipt reads."""

from dataclasses import asdict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.models import BsTrafficLedger, Subscription, User
from app.external.remnawave_api import RemnaWaveAPIError
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
from app.services.remnawave_service import RemnaWaveConfigurationError

from ...dependencies import get_cabinet_db, get_current_cabinet_user
from .split_tariff_switch import _reader


_UNAVAILABLE = (RemnaWaveAPIError, RemnaWaveConfigurationError, TimeoutError)

router = APIRouter()


class DailyResumeConfirmation(BaseModel):
    subscription_id: int = Field(gt=0, le=2_147_483_647, strict=True)
    operation_id: UUID
    expected_price_kopeks: int = Field(ge=0, le=2_147_483_647, strict=True)
    expected_terms_hash: str = Field(pattern=r'^[0-9a-f]{64}$')


def _error(error):
    if isinstance(error, DailyResumeConsentRequired):
        return HTTPException(409, detail={'code': 'daily_resume_requires_confirmation'})
    if isinstance(error, DailyResumeQuoteChanged):
        return HTTPException(409, detail={'code': 'daily_resume_quote_changed'})
    if isinstance(error, RenewalBalanceError):
        return HTTPException(402, detail={'code': 'insufficient_balance'})
    if isinstance(error, _UNAVAILABLE):
        return HTTPException(503, detail={'code': 'daily_resume_temporarily_unavailable'})
    return HTTPException(409, detail={'code': 'daily_resume_unavailable'})


async def free_control(db, *, user_id, subscription_id, paused):
    if type(paused) is not bool:
        raise HTTPException(409, detail={'code': 'daily_pause_target_required'})
    generation = await db.scalar(
        select(BsTrafficLedger.account_generation)
        .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
        .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
    )
    try:
        result = await set_daily_pause(
            db, user_id=user_id, subscription_id=subscription_id, account_generation=generation, paused=paused
        )
        await db.commit()
    except LedgerConflict as error:
        await db.rollback()
        raise _error(error) from error
    user = await db.get(User, user_id, populate_existing=True)
    return dict(
        success=True,
        is_paused=result.is_paused,
        balance_kopeks=user.balance_kopeks,
        balance_label=settings.format_price(user.balance_kopeks),
        delivery_pending=False,
    )


async def _response(db, user_id, subscription_id, result):
    user = await db.get(User, user_id, populate_existing=True)
    sub = await db.scalar(
        select(Subscription)
        .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
        .execution_options(populate_existing=True)
    )
    if sub is None:
        raise HTTPException(404, detail='Subscription not found')
    return dict(
        success=True,
        is_paused=bool(sub.is_daily_paused),
        charged_kopeks=result.charged_kopeks,
        new_end_date=result.new_end_date,
        delivery_pending=result.delivery_pending,
        replayed=result.replayed,
        balance_kopeks=user.balance_kopeks,
    )


@router.get('/split-daily/quote')
async def quote_daily_resume(
    subscription_id: int = Query(gt=0, le=2_147_483_647),
    user: User = Depends(get_current_cabinet_user),
    db: AsyncSession = Depends(get_cabinet_db),
):
    user_id = user.id
    try:
        async with _reader(db, user_id, subscription_id) as reader:
            quote = await prepare_daily_resume(reader, user_id=user_id, subscription_id=subscription_id)
    except (LedgerConflict, *_UNAVAILABLE) as error:
        await db.rollback()
        raise _error(error) from error
    current = await db.get(User, user_id, populate_existing=True)
    return {**asdict(quote), 'subscription_id': subscription_id, 'balance_kopeks': current.balance_kopeks}


@router.post('/split-daily/confirm')
async def confirm_daily_resume(
    request: DailyResumeConfirmation,
    user: User = Depends(get_current_cabinet_user),
    db: AsyncSession = Depends(get_cabinet_db),
):
    user_id = user.id
    args = dict(
        user_id=user_id,
        subscription_id=request.subscription_id,
        operation_key=str(request.operation_id),
        expected_price_kopeks=request.expected_price_kopeks,
        expected_terms_hash=request.expected_terms_hash,
    )
    try:
        result = await find_daily_resume(db, **args)
        await db.commit()
        if result is None:
            async with _reader(db, user_id, request.subscription_id) as reader:
                result = await checkout_daily_resume(reader, **args)
        return await _response(db, user_id, request.subscription_id, result)
    except (LedgerConflict, *_UNAVAILABLE) as error:
        await db.rollback()
        raise _error(error) from error


@router.get('/split-daily/result')
async def daily_resume_result(
    operation_id: UUID,
    subscription_id: int = Query(gt=0, le=2_147_483_647),
    user: User = Depends(get_current_cabinet_user),
    db: AsyncSession = Depends(get_cabinet_db),
):
    user_id = user.id
    try:
        result = await find_daily_resume_by_operation(
            db, user_id=user_id, subscription_id=subscription_id, operation_key=str(operation_id)
        )
        if result is None:
            raise HTTPException(404, detail={'code': 'daily_resume_receipt_not_found'})
        return await _response(db, user_id, subscription_id, result)
    except LedgerConflict as error:
        await db.rollback()
        raise _error(error) from error
