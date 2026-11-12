"""The main subscription contract must not label split accounting as legacy."""

from datetime import UTC, datetime, timedelta

import pytest

from app.cabinet.schemas.subscription import TrafficAccountingData
from app.database.models import Base, Subscription, TrafficAccountingState, TrafficAllowance, User
from app.services.traffic_accounting_service import traffic_accounting_summary
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
@pytest.mark.parametrize('phase', [None, 'ready', 'transferred'])
async def test_split_mode_survives_main_summary_without_old_allowance(postgres_database, mode, phase):
    async with postgres_session(postgres_database, list(Base.metadata.sorted_tables)) as db:
        now = datetime.now(UTC)
        db.add(User(id=1))
        await db.flush()
        db.add(
            Subscription(
                id=1,
                user_id=1,
                start_date=now,
                end_date=now + timedelta(days=30),
                traffic_accounting_mode=mode,
                traffic_limit_gb=100,
            )
        )
        await db.flush()
        if phase:
            db.add(
                TrafficAccountingState(
                    subscription_id=1,
                    phase=phase,
                    panel_identity='synthetic',
                    lifetime_bytes=0,
                    used_bytes=0,
                    business_strategy='NO_RESET',
                    calendar_timezone='UTC',
                    calendar_anchor=now,
                    next_refresh_at=now + timedelta(days=1),
                )
            )
            await db.flush()
            db.add_all(
                [
                    TrafficAllowance(
                        subscription_id=1,
                        operation_key=key,
                        granted_bytes=123,
                        remaining_bytes=123,
                        expires_at=now + timedelta(days=1),
                    )
                    for key in ('base:old', 'purchase:1')
                ]
            )
            await db.flush()
        result = await traffic_accounting_summary(db, 1)
        assert result['traffic_accounting_mode'] == mode
        assert result['traffic_remaining_gb'] is None
        assert result['base_traffic_remaining_gb'] is None
        assert result['purchase_remaining'] == {}
        assert result['next_traffic_refresh_at'] is None
        assert TrafficAccountingData.model_validate(result).traffic_accounting_mode == mode
