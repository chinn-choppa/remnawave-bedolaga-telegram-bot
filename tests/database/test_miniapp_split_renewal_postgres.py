"""The older MiniApp API cannot bypass split confirmation and receipt rules."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from app.database.models import Tariff, Transaction, User
from app.webapi.routes import miniapp
from app.webapi.schemas.miniapp import MiniAppSubscriptionRenewalRequest
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('fault', [None, 'missing_confirmation', 'price', 'balance'])
async def test_miniapp_owned_split_renewal_and_replay(postgres_database, monkeypatch, fault):
    monkeypatch.setattr('app.database.crud.transaction.emit_transaction_side_effects', AsyncMock())
    monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
    monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False))
    monkeypatch.setattr('app.services.payment.lava.shift_lava_next_charge_after_manual_extension', AsyncMock())
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        user = await db.get(User, sub.user_id)
        if fault == 'balance':
            user.balance_kopeks = 0
            await db.commit()
        await db.refresh(user, ['subscriptions'])
        monkeypatch.setattr(miniapp, '_authorize_miniapp_user', AsyncMock(return_value=user))
        before = sub.end_date
        kwargs = (
            {}
            if fault == 'missing_confirmation'
            else {
                'operation_id': '00000000-0000-4000-8000-000000000001',
                'expected_price_kopeks': 2001 if fault == 'price' else 2000,
            }
        )
        payload = MiniAppSubscriptionRenewalRequest(
            init_data='synthetic', subscription_id=sub.id, period_days=30, **kwargs
        )
        if fault:
            with pytest.raises(HTTPException) as error:
                await miniapp.submit_subscription_renewal_endpoint(payload, db)
            assert error.value.status_code == (402 if fault == 'balance' else 409)
            await db.refresh(user)
            await db.refresh(sub)
            assert user.balance_kopeks == (0 if fault == 'balance' else 10000)
            assert sub.end_date == before
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        else:
            response = await miniapp.submit_subscription_renewal_endpoint(payload, db)
            assert response.delivery_pending
            assert user.balance_kopeks == 8000
            assert response.renewed_until == before + timedelta(days=30)
            tariff = await db.get(Tariff, sub.tariff_id)
            tariff.period_prices = {'30': 99999}
            tariff.is_active = False
            user.balance_kopeks = 0
            await db.commit()
            again = await miniapp.submit_subscription_renewal_endpoint(payload, db)
            assert again.renewed_until == response.renewed_until
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
