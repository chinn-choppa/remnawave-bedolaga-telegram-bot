"""Cabinet top-up recovery preserves both ordinary and split renewal consent."""

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from app.cabinet.routes.subscription_modules import helpers
from app.cabinet.routes.subscription_modules.renewal import renew_subscription
from app.cabinet.schemas.subscription import RenewalRequest
from app.config import settings
from app.database.models import BsTrafficRenewalIntent, SubscriptionRenewalOperation, Transaction, User
from app.services import subscription_auto_purchase_service as auto
from app.services.user_cart_service import user_cart_service
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('price', [2000, 1999, None])
async def test_saved_split_cabinet_renewal_preserves_consent_and_replay(postgres_database, monkeypatch, price):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        await db.refresh(sub, ['tariff'])
        user = await db.get(User, sub.user_id)
        before = sub.end_date
        user.balance_kopeks = 0
        await db.commit()
        monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
        monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        save = AsyncMock()
        monkeypatch.setattr(user_cart_service, 'save_user_cart', save)
        request = RenewalRequest(
            period_days=30,
            operation_id=uuid4(),
            expected_end_date=before,
            expected_price_kopeks=2000,
            subscription_id=sub.id,
        )
        with pytest.raises(HTTPException) as error:
            await renew_subscription(request, user=user, db=db, subscription_id=sub.id)
        assert error.value.status_code == 402
        cart = dict(save.call_args.args[1])
        assert cart['renewal_operation_id'] == str(request.operation_id)
        assert cart['renewal_expected_end_date'] == before.isoformat()
        assert cart['total_price'] == 2000
        cart['total_price'] = price
        user.balance_kopeks = 10000
        await db.commit()
        delete = AsyncMock()
        monkeypatch.setattr(auto, '_delete_cart_for_subscription', delete)
        monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False))
        monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
        result = await auto._process_single_cart(db, user, cart)
        assert result is (price == 2000)
        await db.refresh(user)
        await db.refresh(sub)
        assert user.balance_kopeks == (8000 if result else 10000)
        assert sub.end_date == before + timedelta(days=30 if result else 0)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == int(result)
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == int(result)
        assert await db.scalar(select(func.count()).select_from(SubscriptionRenewalOperation)) == int(result)
        if result:
            assert await auto._process_single_cart(db, user, cart)
            response = await renew_subscription(request, user=user, db=db, subscription_id=sub.id)
            assert response['amount_paid_kopeks'] == 2000
            await db.refresh(user)
            assert user.balance_kopeks == 8000
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        else:
            delete.assert_not_awaited()
