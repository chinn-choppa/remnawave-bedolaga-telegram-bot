import asyncio

import pytest

from tests.services.test_traffic_accounting_store import database


@pytest.mark.postgres
async def test_projection_waits_for_outer_commit_and_rollback_discards_callbacks(postgres_database, monkeypatch):
    from app.services import traffic_accounting_service as service

    seen = []

    async def deliver(subscription_id):
        seen.append(subscription_id)

    monkeypatch.setattr(service, '_deliver_projection_by_id', deliver)
    async with database(monkeypatch, postgres_database) as db:
        async with db.begin_nested():
            service.schedule_projection_after_commit(db, 1)
        await asyncio.sleep(0)
        assert seen == []
        await db.rollback()
        await db.commit()
        await asyncio.sleep(0)
        assert seen == []
        async with db.begin_nested():
            service.schedule_projection_after_commit(db, 2)
        await db.commit()
        await asyncio.sleep(0)
        assert seen == [2]


@pytest.mark.postgres
async def test_stop_drains_inflight_projection_tasks(postgres_database, monkeypatch):
    from app.services import traffic_accounting_service as service

    stopped = []

    async def deliver(subscription_id):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.append(subscription_id)

    monkeypatch.setattr(service, '_deliver_projection_by_id', deliver)
    async with database(monkeypatch, postgres_database) as db:
        service.schedule_projection_after_commit(db, 1)
        await db.commit()
        await asyncio.sleep(0)
        await service.stop_projection_tasks(timeout=0.01)
        assert stopped == [1]
        assert not service._projection_tasks
