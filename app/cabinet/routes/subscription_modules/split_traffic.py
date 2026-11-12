"""Read-only split quote adapter; does not activate accounting or take payment."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.database.database import AsyncSessionLocal
from app.database.models import BsTrafficLedger, Subscription, Tariff, User
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.checkout import checkout_package
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import bind_reader
from app.services.bs_traffic.quote_preparation import prepare_package_quote
from app.services.bs_traffic.tariff_settings import BsTrafficSettings
from app.services.remnawave_service import RemnaWaveConfigurationError, RemnaWaveService


router = APIRouter()


class SplitTrafficQuoteRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    group: Literal['ordinary', 'bs']
    gb: int = Field(strict=True, ge=1, le=100_000)


class SplitTrafficQuoteResponse(BaseModel):
    subscription_id: int
    group: Literal['ordinary', 'bs']
    gb: int
    period_days: int
    base_price_kopeks: int
    price_kopeks: int
    discount_percent: int
    balance_kopeks: int
    missing_kopeks: int
    valid_until: datetime


async def get_quote_reader(
    subscription_id: int = Query(..., gt=0),
    user: User = Depends(get_current_cabinet_user),
    db: AsyncSession = Depends(get_cabinet_db),
):
    # Scope is trusted persisted state, never a request-supplied endpoint/identity.
    scope = await db.scalar(
        select(BsTrafficLedger.panel_scope)
        .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user.id,
            Subscription.traffic_accounting_mode == 'split',
        )
    )
    if scope is None:
        raise HTTPException(404, detail='Split subscription not found')
    try:
        async with RemnaWaveService().get_api_client() as api:
            reader = await bind_reader(AsyncSessionLocal, api, panel_scope=scope)
            yield reader
    except (LedgerConflict, RemnaWaveAPIError, RemnaWaveConfigurationError, TimeoutError):
        raise HTTPException(503, detail='Traffic quote temporarily unavailable') from None


@router.post('/traffic/split-quote', response_model=SplitTrafficQuoteResponse)
async def get_split_traffic_quote(
    request: SplitTrafficQuoteRequest,
    subscription_id: int = Query(..., gt=0),
    user: User = Depends(get_current_cabinet_user),
    reader=Depends(get_quote_reader),
):
    try:
        return await prepare_package_quote(
            reader, user_id=user.id, subscription_id=subscription_id, group=request.group, gb=request.gb
        )
    except LedgerConflict:
        raise HTTPException(409, detail='Traffic quote changed or package unavailable') from None
    except (RemnaWaveAPIError, TimeoutError):
        raise HTTPException(503, detail='Traffic quote temporarily unavailable') from None


class SplitTrafficGroupSummary(BaseModel):
    mode: Literal['disabled', 'finite', 'unlimited']
    remaining_bytes: str | None = Field(pattern=r'^\d+$')
    pending_bytes: str = Field(pattern=r'^\d+$')
    pending_operations: int = Field(ge=0)
    needs_review: bool


class SplitTrafficGroups(BaseModel):
    ordinary: SplitTrafficGroupSummary
    bs: SplitTrafficGroupSummary


class SplitTrafficPackage(BaseModel):
    group: Literal['ordinary', 'bs']
    gb: int


async def split_purchase_runtime_ready(
    subscription_id: int = Query(..., gt=0),
    user: User = Depends(get_current_cabinet_user),
    db: AsyncSession = Depends(get_cabinet_db),
) -> bool:
    from app.services.bs_traffic.runtime import runtime_ready, split_traffic_runtime

    if not split_traffic_runtime.enabled:
        return False
    scope = await db.scalar(
        select(BsTrafficLedger.panel_scope)
        .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user.id,
            Subscription.traffic_accounting_mode == 'split',
        )
    )
    return scope is not None and runtime_ready(scope)


class SplitTrafficSummaryResponse(BaseModel):
    subscription_id: int
    calculated_at: datetime
    grace_active: bool
    purchase_available: bool = False
    purchase_packages: list[SplitTrafficPackage] = Field(default_factory=list)
    groups: SplitTrafficGroups


@router.get('/traffic/split-summary', response_model=SplitTrafficSummaryResponse)
async def get_split_traffic_summary(
    subscription_id: int = Query(..., gt=0),
    user: User = Depends(get_current_cabinet_user),
    db: AsyncSession = Depends(get_cabinet_db),
    runtime_ready: bool = Depends(split_purchase_runtime_ready),
):
    from app.services.bs_traffic.customer_summary import SplitSubscriptionNotFound, customer_summary

    try:
        result = await customer_summary(db, user_id=user.id, subscription_id=subscription_id)
        packages = []
        if runtime_ready and not result['grace_active']:
            tariff = await db.scalar(
                select(Tariff)
                .join(Subscription, Subscription.tariff_id == Tariff.id)
                .where(Subscription.id == subscription_id, Subscription.user_id == user.id)
            )
            if tariff is not None:
                if tariff.traffic_topup_enabled and tariff.traffic_limit_gb:
                    packages.extend(
                        {'group': 'ordinary', 'gb': gb}
                        for gb, price in sorted(tariff.get_traffic_topup_packages().items())
                        if price > 0
                    )
                if tariff.bs_traffic_settings:
                    config = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
                    if config.enabled and config.topup_enabled and config.limit_gb:
                        packages.extend(
                            {'group': 'bs', 'gb': int(gb)}
                            for gb, price in sorted(config.topup_packages.items(), key=lambda item: int(item[0]))
                            if price > 0
                        )
        return {**result, 'purchase_available': bool(runtime_ready and packages), 'purchase_packages': packages}
    except SplitSubscriptionNotFound:
        raise HTTPException(404, detail='Split subscription not found') from None
    except LedgerConflict:
        raise HTTPException(409, detail='Traffic accounting temporarily unavailable') from None


class SplitTrafficPurchaseRequest(SplitTrafficQuoteRequest):
    expected_price_kopeks: int = Field(strict=True, ge=0, le=2_147_483_647)
    operation_key: UUID


class SplitTrafficPurchaseResponse(BaseModel):
    subscription_id: int
    price_kopeks: int
    delivery_status: Literal['verified', 'pending']
    replayed: bool


async def require_split_purchase_runtime(ready: bool = Depends(split_purchase_runtime_ready)):
    if not ready:
        raise HTTPException(503, detail='Traffic purchase temporarily unavailable')


@router.post(
    '/traffic/split-purchase',
    response_model=SplitTrafficPurchaseResponse,
    dependencies=[Depends(require_split_purchase_runtime)],
)
async def purchase_split_traffic(
    request: SplitTrafficPurchaseRequest,
    subscription_id: int = Query(..., gt=0),
    user: User = Depends(get_current_cabinet_user),
    reader=Depends(get_quote_reader),
):
    try:
        result = await checkout_package(
            reader,
            user_id=user.id,
            subscription_id=subscription_id,
            group=request.group,
            gb=request.gb,
            expected_price_kopeks=request.expected_price_kopeks,
            operation_key=str(request.operation_key),
        )
        return SplitTrafficPurchaseResponse(
            subscription_id=subscription_id,
            price_kopeks=result.purchase.price_kopeks,
            delivery_status=result.delivery_status,
            replayed=result.purchase.replayed,
        )
    except LedgerConflict:
        raise HTTPException(409, detail='Traffic purchase terms changed or package unavailable') from None
    except (RemnaWaveAPIError, TimeoutError):
        raise HTTPException(503, detail='Traffic purchase temporarily unavailable') from None
