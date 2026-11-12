"""Confirmed provider charges fund split rights once, without wallet debit."""

from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficRenewalIntent, PaymentMethod, Transaction, User
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    'method', [PaymentMethod.LAVA, PaymentMethod.PLATEGA, PaymentMethod.CASHERA, PaymentMethod.CISPAY]
)
async def test_confirmed_recurrent_charge_preserves_wallet_and_provider_identity(
    postgres_database, monkeypatch, method
):
    from app.services.bs_traffic.provider_renewal import fulfill_recurrent_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        result = await fulfill_recurrent_renewal(
            db,
            user_id=sub.user_id,
            subscription_id=sub.id,
            period_days=30,
            amount_kopeks=2000,
            payment_method=method,
            charge_id='charge-1',
        )
        await db.commit()
        tx = await db.get(Transaction, result.transaction_id)
        assert tx.external_id == 'charge-1' and tx.payment_method == method.value
        assert tx.amount_kopeks == -2000
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert sub.end_date == old_end + timedelta(days=30)
        again = await fulfill_recurrent_renewal(
            db,
            user_id=sub.user_id,
            subscription_id=sub.id,
            period_days=30,
            amount_kopeks=2000,
            payment_method=method,
            charge_id='charge-1',
        )
        assert again.replayed and again.intent_id == result.intent_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 1
        assert set(result.grants) == {'ordinary', 'bs'}
