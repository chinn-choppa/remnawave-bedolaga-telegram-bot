"""Do not offer a new checkout while a paid squad change is unresolved."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.database.models import Subscription, User
from tests.database.test_bs_topology_renewal_guard_postgres import pending_switch
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('surface', ['options', 'confirm', 'telegram_options', 'telegram_period'])
async def test_unresolved_change_blocks_new_quote_and_payment_cart(postgres_database, monkeypatch, surface):
    from app.cabinet.routes.subscription_modules import helpers, renewal
    from app.cabinet.schemas.subscription import RenewalRequest
    from app.handlers.subscription import tariff_purchase

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _ = await pending_switch(db, monkeypatch)
        sub = await db.scalar(
            select(Subscription)
            .where(Subscription.id == owner['subscription_id'])
            .options(selectinload(Subscription.tariff))
        )
        user = await db.get(User, owner['user_id'])
        user.balance_kopeks = 0
        await db.commit()
        request = RenewalRequest(
            subscription_id=sub.id,
            period_days=30,
            operation_id=uuid4(),
            expected_end_date=sub.end_date,
            expected_price_kopeks=4000,
        )
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        pricing = AsyncMock(side_effect=AssertionError('An unresolved change must not offer a new payment'))
        monkeypatch.setattr(renewal.pricing_engine, 'calculate_renewal_price', pricing)
        monkeypatch.setattr(renewal.pricing_engine, 'calculate_tariff_purchase_price', pricing)
        cart = AsyncMock(side_effect=AssertionError('An unresolved change must not create a payment cart'))
        monkeypatch.setattr(renewal.user_cart_service, 'save_user_cart', cart)
        if surface in ('options', 'confirm'):
            with pytest.raises(HTTPException) as caught:
                if surface == 'options':
                    await renewal.get_renewal_options(user=user, db=db, subscription_id=sub.id)
                else:
                    await renewal.renew_subscription(request, user=user, db=db, subscription_id=sub.id)
            assert caught.value.status_code == 409
            assert caught.value.detail['code'] == 'split_renewal_not_ready'
        else:
            callback = SimpleNamespace(
                data=f'tariff_extend:{sub.id}:{sub.tariff_id}:30',
                answer=AsyncMock(),
                message=SimpleNamespace(edit_text=AsyncMock()),
            )
            if surface == 'telegram_options':
                await tariff_purchase.show_tariff_extend(callback, user, db, subscription_id=sub.id)
            else:
                await tariff_purchase.select_tariff_extend_period(callback, user, db, AsyncMock())
            assert callback.answer.await_args.kwargs['show_alert'] is True
            callback.message.edit_text.assert_not_awaited()
        pricing.assert_not_awaited()
        cart.assert_not_awaited()


@pytest.mark.parametrize('surface', ['options', 'telegram_period'])
async def test_verified_change_keeps_normal_renewal_preview(postgres_database, monkeypatch, surface):
    from app.cabinet.routes.subscription_modules import helpers, renewal
    from app.handlers.subscription import tariff_purchase
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, receipt = await pending_switch(db, monkeypatch)
        operation_id = receipt.id
        await db.commit()
        assert await advance_renewal_delivery(reader, **owner, operation_id=operation_id) == 'verified'
        db.expire_all()
        sub = await db.scalar(
            select(Subscription)
            .where(Subscription.id == owner['subscription_id'])
            .options(selectinload(Subscription.tariff))
        )
        user = await db.get(User, owner['user_id'])
        user.balance_kopeks = 10000
        await db.commit()
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        price = SimpleNamespace(final_total=4000, original_total=4000, promo_group_discount=0, promo_offer_discount=0)
        monkeypatch.setattr(renewal.pricing_engine, 'calculate_renewal_price', AsyncMock(return_value=price))
        monkeypatch.setattr(renewal.pricing_engine, 'calculate_tariff_purchase_price', AsyncMock(return_value=price))
        if surface == 'options':
            options = await renewal.get_renewal_options(user=user, db=db, subscription_id=sub.id)
            assert [(item.period_days, item.price_kopeks) for item in options] == [(30, 4000)]
        else:
            callback = SimpleNamespace(
                data=f'tariff_extend:{sub.id}:{sub.tariff_id}:30',
                answer=AsyncMock(),
                message=SimpleNamespace(edit_text=AsyncMock()),
            )
            await tariff_purchase.select_tariff_extend_period(callback, user, db, AsyncMock())
            callback.message.edit_text.assert_awaited_once()
