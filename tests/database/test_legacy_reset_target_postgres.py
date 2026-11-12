"""Missing or foreign panel targets cannot be charged for a legacy reset."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select

from app.database.models import Subscription, Transaction, User
from app.handlers.subscription import traffic
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('multi', [False, True])
@pytest.mark.parametrize('case', ['missing', 'unresolved', 'deleted', 'mismatch', 'foreign', 'timeout', 'adopt'])
async def test_reset_target_is_verified_before_charge(postgres_database, monkeypatch, case, multi):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        sub.traffic_accounting_mode = 'legacy'
        sub.remnawave_id = 9001 if case in {'deleted', 'mismatch'} else None
        sub.remnawave_short_uuid = '' if case == 'missing' else 'masked-target'
        sub.traffic_used_gb = 3
        user = await db.get(User, target['user_id'])
        user.remnawave_id = None
        if case == 'foreign':
            other = await db.get(User, 2)
            other.remnawave_id = 9001
            if multi:
                db.add(
                    Subscription(id=9001, user_id=other.id, remnawave_id=9001, end_date=sub.end_date, status='active')
                )
        await db.commit()
        user_id, sub_id, balance = user.id, sub.id, user.balance_kopeks
        count = await db.scalar(select(func.count()).select_from(Transaction))
        monkeypatch.setattr(traffic, '_resolve_subscription', AsyncMock(return_value=(sub, sub_id)))
        monkeypatch.setattr(traffic, '_calculate_traffic_reset_price', lambda sub: 1000)
        monkeypatch.setattr(type(traffic.settings), 'is_multi_tariff_enabled', lambda self: multi)
        monkeypatch.setattr(type(traffic.settings), 'is_traffic_topup_blocked', lambda self: False)
        found = SimpleNamespace(id=9001, short_uuid='masked-wrong' if case == 'mismatch' else 'masked-target')
        api = SimpleNamespace(
            get_user_by_id=AsyncMock(return_value=found if case == 'mismatch' else None),
            get_user_by_short_uuid=AsyncMock(return_value=found if case in {'foreign', 'adopt'} else None),
            reset_user_traffic=AsyncMock(),
        )
        if case == 'timeout':
            api.get_user_by_short_uuid.side_effect = TimeoutError('injected lookup timeout')
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=api)
        context.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(traffic, 'RemnaWaveService', lambda: SimpleNamespace(get_api_client=lambda: context))
        callback = SimpleNamespace(answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
        await traffic.confirm_reset_traffic(
            callback, user, db, SimpleNamespace(get_data=AsyncMock(return_value={'active_subscription_id': sub_id}))
        )
        await db.rollback()
        assert await db.scalar(select(User.balance_kopeks).where(User.id == user_id)) == balance - (
            1000 if case == 'adopt' else 0
        )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == count + (1 if case == 'adopt' else 0)
        assert await db.scalar(select(Subscription.traffic_used_gb).where(Subscription.id == sub_id)) == (
            0 if case == 'adopt' else 3
        )
        if case == 'adopt':
            api.reset_user_traffic.assert_awaited_once_with(9001)
        else:
            api.reset_user_traffic.assert_not_awaited()
