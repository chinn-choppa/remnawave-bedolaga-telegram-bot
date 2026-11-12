"""Real PostgreSQL runtime: persisted ownership, queue, accounting and delivery."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficPanelBinding
from app.services.bs_traffic.intake import attach_cursor
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from app.services.bs_traffic.runtime import SplitTrafficRuntime
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import NODE_B, ORDINARY
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def runtime_for(reader):
    @asynccontextmanager
    async def client():
        yield reader._api

    return SplitTrafficRuntime(
        enabled=True,
        session_factory=reader._factory,
        service_factory=lambda: SimpleNamespace(is_configured=True, get_api_client=client),
    )


async def test_runtime_collects_spends_and_delivers_without_recharging(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        amount = (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes
        before = await totals(db)
        await db.commit()

        async def fetch(nodes, requested_day, **kwargs):
            return {
                'nodes': [
                    {'uuid': node, 'users': [{'id': 12, 'totalBytes': 100 + amount if node == NODE_B else 100}]}
                    for node in nodes
                ]
            }

        reader._api.get_daily_nodes_usage = AsyncMock(side_effect=fetch)
        runtime = runtime_for(reader)
        await runtime.run_once()
        assert reader.panel_scope in runtime._healthy
        assert runtime._has_bindings is True
        db.expire_all()
        assert (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes == 0
        assert (await totals(db))[:3] == before[:3]
        assert account.active_internal_squads == [{'uuid': ORDINARY}]
        await db.rollback()
        await runtime.run_once()
        assert (await totals(db))[:3] == before[:3]


async def test_unbound_endpoint_never_enrolls_or_reads_panel(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await purchased(db, monkeypatch)
        reader._api.base_url = 'https://other.example'
        reader._api.get_split_access_snapshot.reset_mock()
        runtime = runtime_for(reader)
        await runtime.run_once()
        assert runtime._healthy == {}
        assert runtime._has_bindings is False
        reader._api.get_split_access_snapshot.assert_not_awaited()
        assert await db.scalar(select(func.count()).select_from(BsTrafficPanelBinding)) == 1


async def test_runtime_resumes_paid_expiry_receipt_without_second_patch(postgres_database, monkeypatch):
    from app.database.models import BsTrafficRenewalIntent
    from tests.database.test_bs_paid_renewal_delivery_postgres import prepared

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, end = await prepared(db, monkeypatch)
        before = remote['state'].used_traffic_bytes
        await runtime_for(reader)._renewals(reader)
        assert remote['state'].expire_at == end
        assert remote['state'].used_traffic_bytes == before
        db.expire_all()
        assert (await db.get(BsTrafficRenewalIntent, keys['operation_id'])).state == 'verified'
        await db.rollback()
        await runtime_for(reader)._renewals(reader)
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_local_unchanged_rights_do_not_fetch_account_again(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from app.database.models import BsTrafficFleetWork

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id)
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        await db.commit()
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {
                'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 100}]} for node in nodes]
            }
        )
        runtime = runtime_for(reader)
        await runtime.run_once()
        reader._api.get_split_access_snapshot.reset_mock()
        work = await db.get(BsTrafficFleetWork, target['subscription_id'])
        assert work.error is None and work.applied_squads is not None
        work.due_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        await runtime.run_once()
        reader._api.get_split_access_snapshot.assert_not_awaited()
        db.expire_all()
        assert (await db.get(BsTrafficFleetWork, target['subscription_id'])).error is None


async def test_missing_sample_does_not_declare_fleet_healthy(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await purchased(db, monkeypatch)
        reader._api.get_daily_nodes_usage = AsyncMock(
            side_effect=lambda nodes, day, **kw: {'nodes': [{'uuid': node, 'users': []} for node in nodes]}
        )
        runtime = runtime_for(reader)
        await runtime.run_once()
        assert reader.panel_scope not in runtime._healthy
