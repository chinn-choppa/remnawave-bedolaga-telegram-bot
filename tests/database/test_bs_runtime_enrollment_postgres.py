"""Durable, scope-bounded funded enrollment scheduling without live provisioning."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from app.database.models import BsTrafficEnrollment, BsTrafficFleetLane
from tests.database.test_bs_fleet_round_postgres import bulk_fleet
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('failure', ['blocked', 'exception', 'timeout'])
async def test_failed_enrollment_advances_durable_cursor_without_other_scope(postgres_database, monkeypatch, failure):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        await bulk_fleet(db, 4)
        for identifier, subscription_id in enumerate((1, 10, 11), 1):
            db.add(
                BsTrafficEnrollment(
                    id=identifier,
                    subscription_id=subscription_id,
                    user_id=subscription_id,
                    tariff_id=1,
                    source_kind='admin',
                    actor_id=1,
                    terms={},
                    terms_hash='a' * 64,
                    state='pending',
                    panel_scope=reader.panel_scope if identifier < 3 else '00000000-0000-4000-8000-000000000003',
                )
            )
        await db.commit()
        called = []

        async def resume(bound, *, enrollment_id):
            assert bound is reader
            called.append(enrollment_id)
            if enrollment_id == 1:
                if failure == 'exception':
                    raise ValueError('synthetic deferred')
                if failure == 'timeout':
                    await asyncio.Event().wait()
                return 'blocked'
            return 'verified'

        monkeypatch.setattr('app.services.bs_traffic.enrollment_service.resume_enrollment_from_service', resume)
        monkeypatch.setattr('app.services.bs_traffic.runtime.ENROLLMENT_STEP_SECONDS', 0.02)
        first = runtime_for(reader)
        await first._enrollments(reader)
        db.add(
            BsTrafficEnrollment(
                id=4,
                subscription_id=12,
                user_id=12,
                tariff_id=1,
                source_kind='admin',
                actor_id=1,
                terms={},
                terms_hash='b' * 64,
                state='pending',
                panel_scope=reader.panel_scope,
            )
        )
        await db.commit()
        # New receipts cannot extend this finite high-water window.
        # New object proves progress is not hidden in an in-memory cursor.
        second = runtime_for(reader)
        await second._enrollments(reader)
        await second._enrollments(reader)
        assert called == [1, 2]
        row = await db.get(BsTrafficFleetLane, (reader.panel_scope, 'enrollment'))
        assert row.completed_at is not None and row.lease_token is None and row.upper_id == 0
        assert reader.panel_scope in first._scope_failures


async def test_enrollment_lane_runs_after_independent_maintenance_errors(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        runtime = runtime_for(reader)
        for name in ('_recover', '_renewals', '_drain'):
            monkeypatch.setattr(runtime, name, AsyncMock(side_effect=ValueError('synthetic')))
        callback = AsyncMock()
        monkeypatch.setattr(runtime, '_enrollments', callback)
        await runtime._maintenance(reader)
        callback.assert_not_awaited()
        await runtime._enrollment_pass(reader)
        callback.assert_awaited_once_with(reader)


async def test_stale_enrollment_lag_prevents_false_readiness(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from app.database.models import BsTrafficFleetRound

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        now = datetime.now(UTC)
        db.add(
            BsTrafficFleetRound(
                panel_scope=reader.panel_scope,
                round_id=str(uuid4()),
                day=now.date(),
                upper_id=0,
                cursor=0,
                phase='complete',
                started_at=now,
                completed_at=now,
                last_success_at=now,
                error_count=0,
            )
        )
        await db.commit()
        runtime = runtime_for(reader)
        assert await runtime._health(reader) is not None
        job = BsTrafficEnrollment(
            id=1,
            subscription_id=1,
            user_id=1,
            tariff_id=1,
            source_kind='admin',
            actor_id=1,
            terms={},
            terms_hash='a' * 64,
            state='pending',
            panel_scope=None,
            created_at=now - timedelta(minutes=2),
        )
        db.add(job)
        await db.commit()
        assert await runtime._health(reader) is None
        job.state = 'verified'
        await db.commit()
        assert await runtime._health(reader) is not None
