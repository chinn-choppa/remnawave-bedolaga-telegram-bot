from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficGraceIntent, Subscription
from app.services.bs_traffic.grace_intent import claim_restore, stage_restore
from app.services.bs_traffic.panel_binding import bind_reader
from tests.database.test_bs_traffic_grace_intent_postgres import SCOPE, TABLES, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [*TABLES, BsTrafficCollectionBudget.__table__]


async def prepared(db):
    args = await setup(db)
    operation = await stage_restore(db, **args)
    keys = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
    receipt = await claim_restore(db, **keys, operation_id=operation)
    await db.commit()
    factory = async_sessionmaker(db.bind, expire_on_commit=False)
    api = SimpleNamespace(
        base_url='https://panel.example', get_split_grace_snapshot=AsyncMock(return_value=receipt.target)
    )
    reader = await bind_reader(factory, api, panel_scope=SCOPE)
    return keys | {'operation_id': operation}, reader, api, factory


async def test_recovery_confirms_full_target_without_closing_grace(postgres_database):
    from app.services.bs_traffic.grace_recovery import recover_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        assert await recover_restore(reader, **keys) == 'verified'
        row = await db.get(BsTrafficGraceIntent, keys['operation_id'])
        assert row.state == 'verified' and row.verified_at is not None
        assert await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == 1))
        assert await recover_restore(reader, **keys) == 'verified'
        api.get_split_grace_snapshot.assert_awaited_once()


@pytest.mark.parametrize('field,value', [('hwid_device_limit', 99), ('status', 'DISABLED'), ('used_traffic_bytes', 0)])
async def test_mismatch_stays_attempted_without_resending(postgres_database, field, value):
    from app.services.bs_traffic.grace_recovery import recover_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        api.get_split_grace_snapshot.return_value = replace(api.get_split_grace_snapshot.return_value, **{field: value})
        assert await recover_restore(reader, **keys) == 'unconfirmed'
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_purchase_or_admin_change_during_read_cannot_verify_old_goal(postgres_database):
    from app.services.bs_traffic.grace_recovery import recover_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, factory = await prepared(db)
        result = api.get_split_grace_snapshot.return_value

        async def read(_):
            # NOWAIT proves HTTP is outside business row locks.
            async with factory() as worker, worker.begin():
                await worker.execute(select(Subscription).where(Subscription.id == 1).with_for_update(nowait=True))
                await worker.execute(update(Subscription).where(Subscription.id == 1).values(device_limit=99))
            return result

        api.get_split_grace_snapshot.side_effect = read
        assert await recover_restore(reader, **keys) == 'obsolete'
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_cancellation_preserves_unknown_attempt(postgres_database):
    import asyncio

    from app.services.bs_traffic.grace_recovery import recover_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        api.get_split_grace_snapshot.side_effect = asyncio.CancelledError
        with pytest.raises(asyncio.CancelledError):
            await recover_restore(reader, **keys)
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_late_readback_can_confirm_after_send_deadline(postgres_database):
    from datetime import UTC, datetime, timedelta

    from app.services.bs_traffic.grace_recovery import recover_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, _, _ = await prepared(db)
        await db.execute(update(BsTrafficGraceIntent).values(send_deadline=datetime.now(UTC) - timedelta(seconds=30)))
        await db.commit()
        assert await recover_restore(reader, **keys) == 'verified'


async def test_provider_error_persists_shared_pause_and_keeps_attempt(postgres_database):
    from app.external.remnawave_api import RemnaWaveAPIError
    from app.services.bs_traffic.grace_recovery import recover_restore
    from app.services.bs_traffic.panel_admission import PanelBusy
    from app.services.bs_traffic.panel_pause import remaining_pause

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        api.get_split_grace_snapshot.side_effect = RemnaWaveAPIError('Synthetic rate limit', 429)
        assert await recover_restore(reader, **keys) == 'unconfirmed'
        assert await remaining_pause(db, panel_scope=SCOPE) > 0
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'
        with pytest.raises(PanelBusy):
            await recover_restore(reader, **keys)
        api.get_split_grace_snapshot.assert_awaited_once()


async def test_retirement_during_read_never_verifies(postgres_database):
    from app.services.bs_traffic.grace_recovery import recover_restore
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, factory = await prepared(db)
        result = api.get_split_grace_snapshot.return_value

        async def read(_):
            async with factory() as worker, worker.begin():
                await retire_binding(worker, panel_scope=SCOPE)
            return result

        api.get_split_grace_snapshot.side_effect = read
        with pytest.raises(LedgerConflict):
            await recover_restore(reader, **keys)
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_foreign_owner_is_rejected_before_http(postgres_database):
    from app.services.bs_traffic.grace_recovery import recover_restore
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        with pytest.raises(LedgerConflict):
            await recover_restore(reader, **(keys | {'user_id': 2}))
        api.get_split_grace_snapshot.assert_not_awaited()
