"""A failed legacy switch must not leave a committed debit or transaction."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Delete

from app.database.models import Subscription, Transaction, User
from app.handlers.subscription import traffic
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('fail', [True, False])
async def test_switch_money_is_atomic_and_owner_stays_locked(postgres_database, monkeypatch, fail):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        sub.traffic_accounting_mode = 'legacy'
        sub.traffic_limit_gb = 10
        sub.purchased_traffic_gb = 0
        sub.end_date = datetime.now(UTC) + timedelta(days=30)
        await db.commit()
        user = await db.get(User, target['user_id'])
        user_id, sub_id = user.id, sub.id
        before_balance = user.balance_kopeks
        before_count = await db.scalar(select(func.count()).select_from(Transaction))
        monkeypatch.setattr(traffic, '_resolve_subscription', AsyncMock(return_value=(sub, sub_id)))
        monkeypatch.setattr(type(traffic.settings), 'get_traffic_price', lambda self, gb: gb * 100)
        monkeypatch.setattr(traffic.PricingEngine, 'get_addon_discount_percent', lambda *args: 0)
        from app.database.crud import transaction as transaction_crud

        effects = AsyncMock()
        monkeypatch.setattr(transaction_crud, 'emit_transaction_side_effects', effects)
        monkeypatch.setattr(traffic, 'reactivate_subscription', AsyncMock())
        monkeypatch.setattr(
            traffic,
            'SubscriptionService',
            lambda: SimpleNamespace(
                update_remnawave_user=AsyncMock(),
                enable_remnawave_user=AsyncMock(),
            ),
        )
        original_execute = db.execute

        async def fail_delete(statement, *args, **kwargs):
            if isinstance(statement, Delete):
                # A competing transition cannot lock the owner between debit and limit update.
                async with AsyncSession(bind=db.bind) as concurrent:
                    with pytest.raises(DBAPIError) as error:
                        await concurrent.execute(select(User.id).where(User.id == user_id).with_for_update(nowait=True))
                    assert error.value.orig.sqlstate == '55P03'
                    await concurrent.rollback()
                if fail:
                    raise RuntimeError('injected package cleanup failure')
            return await original_execute(statement, *args, **kwargs)

        monkeypatch.setattr(db, 'execute', fail_delete)
        callback = SimpleNamespace(
            data='execute_switch_traffic_20',
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )
        await traffic.execute_switch_traffic(callback, user, db)
        balance = await db.scalar(select(User.balance_kopeks).where(User.id == user_id))
        assert balance == before_balance - (0 if fail else 1000)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == before_count + (0 if fail else 1)
        assert await db.scalar(select(Subscription.traffic_limit_gb).where(Subscription.id == sub_id)) == (
            10 if fail else 20
        )
        if fail:
            effects.assert_not_awaited()
        else:
            effects.assert_awaited_once()
