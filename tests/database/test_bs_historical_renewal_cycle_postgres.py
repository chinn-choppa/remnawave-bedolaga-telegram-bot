"""A paid UUID remains a receipt after real return to ordinary ownership."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy import func, select

from app.database.crud.tariff import update_tariff
from app.database.models import BsTrafficOwnershipCycle, Subscription, Tariff, Transaction, User
from app.services.bs_traffic.renewal import renew_from_balance
from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
from app.services.bs_traffic.reverse_delivery import advance_reverse
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
OPERATION = '12345678-1234-4234-8234-123456789012'


@pytest.mark.parametrize('entry', ['cabinet', 'shared', 'cart'])
async def test_paid_replay_after_real_reverse_never_debits_again(postgres_database, monkeypatch, entry):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, sub = await prepared(db, monkeypatch)
        sub.end_date = datetime.now(UTC) - timedelta(days=1)
        await db.flush()
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        paid = await renew_from_balance(db, **keys, period_days=30, expected_price_kopeks=2000, operation_key=OPERATION)
        await db.commit()
        assert set(paid.grants) == {'ordinary'}
        remote = {'state': reader._api.get_split_grace_snapshot.return_value}
        original = reader._api.get_split_transfer_snapshot.return_value
        reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])
        reader._api.get_split_transfer_snapshot = AsyncMock(
            side_effect=lambda _: replace(
                original,
                used_bytes=remote['state'].used_traffic_bytes,
                traffic_limit_bytes=remote['state'].traffic_limit_bytes,
            )
        )

        async def write(target):
            remote['state'] = target

        reader._api.restore_split_user_once = AsyncMock(side_effect=write)
        reader._api.return_split_to_managed_once = AsyncMock(side_effect=write)
        assert await advance_renewal_delivery(reader, **keys, operation_id=paid.intent_id) == 'verified'
        assert await advance_reverse(reader, **keys) == 'managed'
        await db.rollback()
        sub = await db.get(Subscription, 1, populate_existing=True)
        user = await db.get(User, 1, populate_existing=True)
        assert sub.traffic_accounting_mode == 'managed'
        assert (await db.get(BsTrafficOwnershipCycle, (1, 0))).reverse_attempt['state'] == 'verified'
        before, balance = sub.end_date, user.balance_kopeks
        assert balance == 8000
        # Mutable prerequisites no longer authorize repricing an acknowledged payment.
        user.restriction_subscription = True
        (await db.get(Tariff, sub.tariff_id)).period_prices = {'30': 9000}
        await db.commit()
        if entry == 'cabinet':
            from app.cabinet.routes.subscription_modules.renewal import renew_subscription
            from app.cabinet.schemas.subscription import RenewalRequest

            result = await renew_subscription(
                RenewalRequest(period_days=30, operation_id=UUID(OPERATION), expected_price_kopeks=2000),
                user=user,
                db=db,
                subscription_id=sub.id,
            )
            assert result['amount_paid_kopeks'] == 2000
        elif entry == 'shared':
            from app.services.subscription_renewal_service import SubscriptionRenewalService

            result = await SubscriptionRenewalService().finalize(
                db, user, sub, SimpleNamespace(final_total=2000, period_days=30), operation_key=OPERATION
            )
            assert result.transaction.id == paid.transaction_id
        else:
            import app.services.subscription_auto_purchase_service as cart

            monkeypatch.setattr(cart, '_delete_cart_for_subscription', AsyncMock())
            monkeypatch.setattr(cart, 'clear_subscription_checkout_draft', AsyncMock())
            assert await cart._auto_extend_subscription(
                db,
                user,
                dict(subscription_id=sub.id, period_days=30, renewal_operation_key=OPERATION, total_price=2000),
            )
        await db.refresh(sub)
        await db.refresh(user)
        assert sub.end_date == before and user.balance_kopeks == balance
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        reader._api.return_split_to_managed_once.assert_awaited_once()
