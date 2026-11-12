import asyncio
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, func, select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficCutoverIntent,
    BsTrafficLedger,
    BsTrafficUsageCursor,
    Subscription,
)
from app.services.bs_traffic import cutover_activation as activation
from app.services.bs_traffic.cutover_sender import send_cutover
from app.services.bs_traffic.delivery_store import _local_guard
from app.services.bs_traffic.ledger import LedgerConflict, grant
from tests.database.test_bs_traffic_cutover_baseline_postgres import TABLES, observe
from tests.database.test_bs_traffic_cutover_sender_postgres import setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def ready(db, monkeypatch, *, baseline=True):
    reader, keys, plan, op = await setup(db, monkeypatch)
    assert await send_cutover(reader, **keys, operation_id=op) == 'verified'
    if not baseline:
        await db.execute(delete(BsTrafficUsageCursor).where(BsTrafficUsageCursor.subscription_id == 1))
        await db.commit()
    monkeypatch.setattr(activation, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
    return reader, keys, plan, op


async def test_activation_is_atomic_and_replay_does_not_recredit(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await ready(db, monkeypatch)
        before = (await db.get(BsTrafficLedger, 1)).revision
        await db.commit()
        assert await activation.activate_cutover(reader, **keys, operation_id=op) == 'activated'
        async with reader._factory() as check, check.begin():
            row = await check.get(BsTrafficCutoverIntent, op)
            assert row.state == 'activated' and row.activated_at is not None
            assert (await check.get(Subscription, 1)).traffic_accounting_mode == 'split'
            assert (await check.get(BsTrafficLedger, 1)).revision == before + 1
            # This fixture predates tariff-backed delivery guards. Complete its
            # tariff configuration before asserting delivery eligibility.
            from app.database.models import Tariff

            policy = (await check.get(BsTrafficLedger, 1)).access_policy
            tariff = await check.get(Tariff, policy['tariff_id'])
            tariff.allowed_squads = policy['topology']['ordinary_squads']
            tariff.bs_traffic_settings = {'squad_uuids': policy['topology']['bs_squads'], 'limit_gb': 0}
            await check.flush()
            now = await check.scalar(select(func.clock_timestamp()))
            assert await _local_guard(check, 1, 1, now)
            await grant(
                check,
                **keys,
                group='ordinary',
                operation_key='new-paid-package',
                amount_bytes=50,
                starts_at=now,
                expires_at=now + timedelta(days=1),
            )
        assert await activation.activate_cutover(reader, **keys, operation_id=op) == 'activated'
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 850


@pytest.mark.parametrize('change', ['missing_baseline', 'panel', 'topology'])
async def test_activation_requires_current_evidence(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await ready(db, monkeypatch, baseline=change != 'missing_baseline')
        if change == 'panel':
            reader._api.get_split_grace_snapshot.return_value = replace(plan.target, hwid_device_limit=99)
        elif change == 'topology':
            activation.collect_topology_evidence.return_value = replace(plan.evidence, fingerprint='b' * 64)
        result = await activation.activate_cutover(reader, **keys, operation_id=op)
        assert result in ('obsolete', 'unconfirmed')
        async with reader._factory() as check:
            assert (await check.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
            assert (await check.get(BsTrafficCutoverIntent, op)).activated_at is None


async def test_concurrent_activations_change_revision_once(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await ready(db, monkeypatch)
        before = (await db.get(BsTrafficLedger, 1)).revision
        await db.commit()
        result = await asyncio.gather(*(activation.activate_cutover(reader, **keys, operation_id=op) for _ in range(2)))
        assert result == ['activated', 'activated']
        async with reader._factory() as check:
            assert (await check.get(BsTrafficLedger, 1)).revision == before + 1


async def test_mode_string_alone_cannot_authorize_money_or_panel_writer(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await ready(db, monkeypatch)
        (await db.get(Subscription, 1)).traffic_accounting_mode = 'split'
        await db.commit()
        now = await db.scalar(select(func.clock_timestamp()))
        with pytest.raises(LedgerConflict, match='no confirmed activation'):
            await grant(
                db,
                **keys,
                group='ordinary',
                operation_key='not-authorized',
                amount_bytes=50,
                starts_at=now,
                expires_at=now + timedelta(days=1),
            )
        with pytest.raises(LedgerConflict, match='no confirmed activation'):
            await _local_guard(db, 1, 1, now)


async def test_new_raw_counter_during_http_prevents_activation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await ready(db, monkeypatch)

        async def read(_):
            async with reader._factory() as other, other.begin():
                await observe(other, reader, keys, amount=150)
            return plan.target

        reader._api.get_split_grace_snapshot.side_effect = read
        assert await activation.activate_cutover(reader, **keys, operation_id=op) == 'obsolete'
        async with reader._factory() as check:
            assert (await check.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
            assert await check.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800


@pytest.mark.parametrize('change', ['binding', 'source'])
async def test_activated_owner_is_revalidated_for_new_grants(postgres_database, monkeypatch, change):
    from app.database.models import TrafficAccountingState
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await ready(db, monkeypatch)
        assert await activation.activate_cutover(reader, **keys, operation_id=op) == 'activated'
        async with reader._factory() as other, other.begin():
            if change == 'binding':
                await retire_binding(other, panel_scope=reader.panel_scope)
            else:
                (await other.get(TrafficAccountingState, 1)).used_bytes += 1
        now = await db.scalar(select(func.clock_timestamp()))
        with pytest.raises(LedgerConflict, match='no confirmed activation'):
            await grant(
                db,
                **keys,
                group='ordinary',
                operation_key='stale-owner',
                amount_bytes=50,
                starts_at=now,
                expires_at=now + timedelta(days=1),
            )
