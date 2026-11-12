"""Single-bucket revival must not overwrite an existing split account."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm.attributes import set_committed_value

from app.database.crud.subscription import _revive_paid_subscription
from app.database.models import Subscription, User
from app.services.traffic_accounting import AccountingConflict
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_revive_rejects_persisted_split_before_mutation(postgres_database, mode):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        db.add(User(id=1))
        await db.flush()
        before = datetime.now(UTC) - timedelta(days=1)
        sub = Subscription(
            user_id=1,
            status='expired',
            start_date=before - timedelta(days=30),
            end_date=before,
            traffic_limit_gb=10,
            traffic_accounting_mode=mode,
        )
        db.add(sub)
        await db.commit()
        set_committed_value(sub, 'traffic_accounting_mode', 'legacy')
        with pytest.raises(AccountingConflict):
            await _revive_paid_subscription(
                db,
                sub,
                duration_days=30,
                traffic_limit_gb=999,
                device_limit=5,
                connected_squads=[],
                update_server_counters=False,
                commit=False,
            )
        await db.commit()
        await db.refresh(sub)
        assert sub.end_date == before
        assert sub.traffic_limit_gb == 10
        assert sub.status == 'expired'
