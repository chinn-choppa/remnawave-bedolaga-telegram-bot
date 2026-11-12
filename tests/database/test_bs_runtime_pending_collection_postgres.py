"""Funded transferred accounts receive raw coverage before activation, not debits."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficAllowance, BsTrafficFleetWork, BsTrafficObservation, Transaction
from app.services.bs_traffic.enrollment import stage_paid_enrollment
from app.services.bs_traffic.enrollment_delivery import _transfer, advance_enrollment
from app.services.bs_traffic.enrollment_source import prepare_ordinary_source
from tests.database.test_bs_enrollment_postgres import BS, ORDINARY, TABLES, bound_panel, paid
from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    ('missing', 'fault'),
    [
        (False, None),
        (True, None),
        (False, 'funding'),
        (False, 'generation'),
        (False, 'cycle'),
        (False, 'scope'),
        (False, 'whole_pass'),
    ],
)
async def test_runtime_collects_pending_funded_enrollment_before_activation(
    postgres_database, monkeypatch, missing, fault
):
    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        user, _, sub, transaction = await paid(db, monkeypatch)
        job = await stage_paid_enrollment(db, sub, transaction=transaction)
        reader, remote = await bound_panel(db, sub, monkeypatch, used=12 * 1024**3)
        reader._api.get_system_configuration = AsyncMock(
            return_value={
                'service': {'disableUserUsageRecords': False, 'cleanUsageHistory': False},
                'misc': {'userUsageIgnoreBelowBytes': 0},
            }
        )
        reader._api.get_internal_squad_topology = AsyncMock(
            side_effect=lambda squad: {
                'squadUuid': squad,
                'accessibleNodes': [
                    {
                        'uuid': NODE_A if squad == ORDINARY else NODE_B,
                        'configProfileUuid': '66666666-6666-4666-8666-666666666666',
                        'activeInbounds': ['ordinary' if squad == ORDINARY else 'bs'],
                    }
                ],
            }
        )
        await prepare_ordinary_source(reader, job.id)
        await _transfer(reader, job.id)
        assert await advance_enrollment(reader, enrollment_id=job.id) == 'accounting_pending'
        await db.refresh(job)
        await db.refresh(sub)
        assert job.state == 'transferred' and sub.traffic_accounting_mode == 'split_pending'
        amount_before = await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes)))
        panel_id = sub.remnawave_id
        await db.rollback()

        async def daily(nodes, day, **kwargs):
            return {
                'nodes': []
                if missing
                else [{'uuid': node, 'users': [{'id': panel_id, 'totalBytes': 123}]} for node in nodes]
            }

        reader._api.get_daily_nodes_usage = AsyncMock(side_effect=daily)
        runtime = runtime_for(reader)
        if fault == 'whole_pass':
            import asyncio

            collected = asyncio.Event()
            original = runtime._collection

            async def collect(bound):
                await asyncio.sleep(0.02)
                await original(bound)
                assert await db.scalar(select(func.count()).select_from(BsTrafficFleetWork)) == 0
                assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == amount_before
                await db.rollback()
                collected.set()

            async def resume(bound, *, enrollment_id):
                assert collected.is_set()
                return await advance_enrollment(bound, enrollment_id=enrollment_id)

            monkeypatch.setattr(runtime, '_collection', collect)
            monkeypatch.setattr('app.services.bs_traffic.enrollment_service.resume_enrollment_from_service', resume)
            await runtime.run_once()
            await db.refresh(job)
            await db.refresh(sub)
            assert job.state == 'verified' and sub.traffic_accounting_mode == 'split'
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
            return
        if fault:
            await db.refresh(job)
            if fault == 'funding':
                await db.refresh(transaction)
                transaction.is_completed = False
            elif fault == 'generation':
                job.account_generation = 'f' * 64
            elif fault == 'cycle':
                job.ownership_cycle += 1
            else:
                job.panel_scope = '00000000-0000-4000-8000-000000000099'
            await db.commit()
        await runtime._collection(reader)
        if fault:
            reader._api.get_daily_nodes_usage.assert_not_awaited()
            assert await db.scalar(select(func.count()).select_from(BsTrafficFleetWork)) == 0
            assert await db.scalar(select(func.count()).select_from(BsTrafficObservation)) == 0
            return
        assert reader._api.get_daily_nodes_usage.await_count == 2
        assert await db.scalar(select(func.count()).select_from(BsTrafficFleetWork)) == 0
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == amount_before
        assert await db.scalar(select(func.count()).select_from(BsTrafficObservation)) == (0 if missing else 2)
        await db.rollback()

        # Actual enrollment driver through the bounded runtime adapter lane.
        async def resume(bound, *, enrollment_id):
            return await advance_enrollment(bound, enrollment_id=enrollment_id)

        monkeypatch.setattr('app.services.bs_traffic.enrollment_service.resume_enrollment_from_service', resume)
        await runtime._enrollments(reader)
        await db.refresh(job)
        await db.refresh(sub)
        assert job.state == 'verified'
        assert sub.traffic_accounting_mode == 'split'
        await db.refresh(user)
        assert user.balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        if not missing:
            assert {item['uuid'] for item in remote['state'].active_internal_squads} == {ORDINARY, BS}
