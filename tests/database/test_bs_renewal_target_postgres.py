"""A changed sales mode must not redirect an explicit paid confirmation."""

from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from app.cabinet.routes.subscription_modules.renewal import renew_subscription
from app.cabinet.schemas.subscription import RenewalRequest
from app.config import settings
from app.database.models import Subscription, Tariff, Transaction, User
from tests.database.test_bs_paid_renewal_postgres import TABLES, prepared
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_single_mode_cannot_charge_different_subscription(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        user = await db.get(User, sub.user_id)
        db.add(Tariff(id=200, name='Synthetic second tariff', period_prices={'30': 2000}))
        await db.flush()
        other = Subscription(
            id=200,
            user_id=user.id,
            tariff_id=200,
            status='active',
            is_trial=False,
            end_date=sub.end_date,
            device_limit=2,
            remnawave_short_id='synthetic-other',
        )
        db.add(other)
        await db.commit()
        monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: False)
        await db.refresh(user, ['subscriptions'])
        chosen = other if user.subscription.id == sub.id else sub
        before = sub.end_date, other.end_date
        with pytest.raises(HTTPException) as error:
            await renew_subscription(
                RenewalRequest(
                    period_days=30, operation_id=uuid4(), expected_price_kopeks=2000, expected_end_date=chosen.end_date
                ),
                user=user,
                db=db,
                subscription_id=chosen.id,
            )
        assert error.value.status_code == 409
        assert error.value.detail == 'Selected subscription changed. Refresh the renewal options'
        await db.commit()
        await db.refresh(user)
        assert user.balance_kopeks == 10000
        assert (sub.end_date, other.end_date) == before
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
