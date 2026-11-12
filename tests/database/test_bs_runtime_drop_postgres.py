"""Restart-safe drop scheduling never retries committed unknown outcomes."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.config import settings
from app.database.models import BsTrafficDelivery, BsTrafficFleetLane, BsTrafficFleetRound
from app.services.bs_traffic.preparation import prepare_delivery
from app.services.bs_traffic.sender import send_delivery
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def ready_drop(db, monkeypatch):
    _, reader, target, account = await prepared_purchase(db, monkeypatch)
    account.active_internal_squads = [{'uuid': ORDINARY}, {'uuid': BS}]
    monkeypatch.setattr(settings, 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED', True)
    reader._api.request_user_connection_drop = AsyncMock()
    prepared = await prepare_delivery(
        reader._factory, reader, **target, operation_key='runtime-exhaustion', settle_usage=False
    )
    assert (
        await send_delivery(
            reader._factory,
            reader._api,
            **target,
            operation_id=prepared.operation_id,
            evidence=prepared.evidence,
            current_guard_hash=prepared.guard_hash,
            bound_reader=reader,
        )
        == 'verified'
    )
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
    return reader, prepared.operation_id


@pytest.mark.parametrize('unknown', [False, True])
async def test_runtime_drop_sends_pending_once_and_never_replays_unknown(postgres_database, monkeypatch, unknown):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, operation = await ready_drop(db, monkeypatch)
        if unknown:
            reader._api.request_user_connection_drop.side_effect = TimeoutError('synthetic unknown')
        runtime = runtime_for(reader)
        await runtime._drops(reader)
        reader._api.request_user_connection_drop.assert_awaited_once()
        db.expire_all()
        row = await db.get(BsTrafficDelivery, operation)
        assert row.connection_drop['state'] == ('attempted' if unknown else 'accepted')
        assert await runtime._health(reader) is not None
        assert runtime._drop_warnings.get(reader.panel_scope) == ('connection_drop_unknown' if unknown else None)
        lane = await db.get(BsTrafficFleetLane, (reader.panel_scope, 'drop'))
        lane.due_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        # A new runtime object after a due retry still cannot select attempted.
        await runtime_for(reader)._drops(reader)
        reader._api.request_user_connection_drop.assert_awaited_once()


async def test_stale_drop_lag_is_visible_only_when_operator_enabled(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, operation = await ready_drop(db, monkeypatch)
        row = await db.get(BsTrafficDelivery, operation)
        row.created_at = datetime.now(UTC) - timedelta(minutes=2)
        await db.commit()
        runtime = runtime_for(reader)
        assert await runtime._health(reader) is not None
        assert runtime._drop_warnings[reader.panel_scope] == 'connection_drop_delayed'
        monkeypatch.setattr(settings, 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED', False)
        await runtime._drops(reader)
        reader._api.request_user_connection_drop.assert_not_awaited()
        assert await runtime._health(reader) is not None

        assert reader.panel_scope not in runtime._drop_warnings
