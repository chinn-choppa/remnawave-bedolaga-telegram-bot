"""Bounded drain snapshots use independent real PostgreSQL transactions."""

from datetime import datetime

import pytest
from sqlalchemy import select

from app.database.crud.tariff import update_tariff
from app.database.models import BsTrafficLedger, Subscription, Tariff
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('disabled', [False, True])
async def test_page_captures_only_disabled_tariff_and_replay_is_noop(postgres_database, monkeypatch, disabled):
    from app.services.bs_traffic.drain_worker import process_drain_page

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        before = sub.end_date
        if disabled:
            await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        await db.commit()
        first = await process_drain_page(reader._factory, panel_scope=reader.panel_scope, limit=1)
        assert first.scanned == 1
        assert first.changed == int(disabled)
        assert first.deferred == 0
        state = await db.get(BsTrafficLedger, sub.id, populate_existing=True)
        revision = state.revision
        if disabled:
            assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == before
        else:
            assert 'bs_drain_until' not in state.access_policy
        await db.rollback()
        replay = await process_drain_page(reader._factory, panel_scope=reader.panel_scope, limit=1)
        assert replay.changed == 0
        assert await db.scalar(select(BsTrafficLedger.revision)) == revision


async def test_wrong_panel_and_cursor_do_not_touch_subscription(postgres_database, monkeypatch):
    from app.services.bs_traffic.drain_worker import process_drain_page

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        await db.commit()
        wrong = await process_drain_page(reader._factory, panel_scope='99999999-9999-4999-8999-999999999999')
        assert wrong.scanned == 0 and wrong.changed == 0
        past = await process_drain_page(
            reader._factory,
            panel_scope=reader.panel_scope,
            after_id=target['subscription_id'],
            upper_id=target['subscription_id'],
        )
        assert past.scanned == 0


async def test_locked_owner_is_deferred_without_changes(postgres_database, monkeypatch):
    from app.database.models import User
    from app.services.bs_traffic.drain_worker import process_drain_page

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        await db.execute(select(User.id).where(User.id == sub.user_id).with_for_update())
        result = await process_drain_page(reader._factory, panel_scope=reader.panel_scope, limit=1)
        assert result.deferred == 1 and result.changed == 0
        await db.rollback()
        state = await db.get(BsTrafficLedger, target['subscription_id'], populate_existing=True)
        assert 'bs_drain_until' not in state.access_policy
        retried = await process_drain_page(reader._factory, panel_scope=reader.panel_scope, limit=1)
        assert retried.changed == 1


async def test_page_bound_and_failed_neighbor_preserve_committed_snapshot(postgres_database, monkeypatch):
    from app.services.bs_traffic.drain_worker import process_drain_page
    from app.services.bs_traffic.ledger import initialize

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        other = await db.get(Subscription, 2)
        other.traffic_accounting_mode = 'split_pending'
        await initialize(
            db,
            user_id=other.user_id,
            subscription_id=other.id,
            account_generation='b' * 64,
            panel_scope=reader.panel_scope,
            panel_user_id=other.remnawave_id,
        )
        await db.commit()
        first = await process_drain_page(reader._factory, panel_scope=reader.panel_scope, limit=1)
        assert first.scanned == 1 and first.changed == 1 and first.next_after == sub.id
        second = await process_drain_page(
            reader._factory,
            panel_scope=reader.panel_scope,
            limit=1,
            after_id=first.next_after,
            upper_id=first.upper_id,
        )
        assert second.scanned == 1 and second.deferred == 1 and second.next_after is None
        state = await db.get(BsTrafficLedger, target['subscription_id'], populate_existing=True)
        assert state.access_policy['bs_purchase_enabled'] is False
        await db.rollback()
        whole = await process_drain_page(reader._factory, panel_scope=reader.panel_scope)
        assert whole.scanned == 2 and whole.changed == 0 and whole.deferred == 1


async def test_overlapping_workers_capture_once(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.drain_worker import process_drain_page

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        before = sub.end_date
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        await db.commit()
        results = await asyncio.gather(
            *(process_drain_page(reader._factory, panel_scope=reader.panel_scope) for _ in range(2))
        )
        assert sum(result.changed for result in results) == 1
        assert sum(result.deferred for result in results) == 0
        state = await db.get(BsTrafficLedger, sub.id, populate_existing=True)
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == before
