"""A payment receipt stays immutable while its delivery can finish later."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.cabinet.routes.subscription_modules import helpers
from app.cabinet.routes.subscription_modules.renewal import renew_subscription
from app.cabinet.schemas.subscription import RenewalRequest
from app.config import settings
from app.database.models import BsTrafficRenewalIntent, SubscriptionRenewalOperation, Transaction, User
from tests.database.test_bs_paid_renewal_postgres import TABLES, prepared
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def verified(intent):
    now = datetime.now(UTC)
    intent.state = 'verified'
    intent.attempt_token = str(uuid4())
    intent.attempted_at = intent.verified_at = now
    intent.send_deadline = intent.valid_until = now + timedelta(seconds=30)
    intent.target = intent.expected_source = {}
    intent.target_hash = intent.source_hash = intent.guard_hash = '0' * 64
    intent.ledger_revision = 1


@pytest.mark.parametrize('immediate', [False, True])
async def test_completed_delivery_clears_pending_without_new_payment(postgres_database, monkeypatch, immediate):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        await db.refresh(sub, ['tariff'])
        user = await db.get(User, sub.user_id)
        monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
        monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())

        async def deliver(session, intent_id):
            if immediate:
                intent = await session.get(BsTrafficRenewalIntent, intent_id)
                verified(intent)
                await session.commit()
            return immediate

        monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', deliver)
        request = RenewalRequest(
            period_days=30, operation_id=uuid4(), expected_price_kopeks=2000, expected_end_date=sub.end_date
        )
        first = await renew_subscription(request, user=user, db=db, subscription_id=sub.id)
        assert first['delivery_pending'] is not immediate
        paid_end = sub.end_date
        intent = await db.scalar(select(BsTrafficRenewalIntent))
        verified(intent)
        await db.commit()
        repeated = await renew_subscription(request, user=user, db=db, subscription_id=sub.id)
        assert repeated['delivery_pending'] is False
        assert repeated['new_end_date'] == first['new_end_date']
        assert repeated['amount_paid_kopeks'] == 2000
        assert sub.end_date == paid_end and user.balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        operation = await db.get(SubscriptionRenewalOperation, (user.id, str(request.operation_id)))
        assert operation.response['delivery_pending'] is False
