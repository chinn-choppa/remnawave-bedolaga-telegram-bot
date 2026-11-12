from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficDelivery, User
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.delivery_store import claim
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.recovery_preflight import recover_from_panel
from tests.database.test_bs_traffic_policy_postgres import ORDINARY
from tests.database.test_bs_traffic_prepare_postgres import client, prepare, setup
from tests.database.test_bs_traffic_sender_postgres import panel
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def attempt(db, api, *, old=True):
    target = await setup(db, api)
    prepared = await prepare(db, api, target)
    await claim(db, **target, operation_id=prepared.operation_id, current_guard_hash=prepared.guard_hash)
    if old:
        await db.execute(
            update(BsTrafficDelivery)
            .where(BsTrafficDelivery.id == prepared.operation_id)
            .values(attempted_at=datetime.now(UTC) - timedelta(seconds=16))
        )
    await db.commit()
    api.get_system_configuration.reset_mock()
    api.get_internal_squad_topology.reset_mock()
    api.get_split_access_snapshot.reset_mock()
    api.get_split_access_snapshot.return_value = panel(active_internal_squads=[{'uuid': ORDINARY}])
    api.update_internal_squads_once = AsyncMock()
    return target, prepared.operation_id


async def recover(db, api, target, operation):
    return await recover_from_panel(async_sessionmaker(db.bind), api, **target, operation_id=operation)


async def test_recovery_builds_own_evidence_and_never_patches(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        assert await recover(db, api, target, operation) == 'verified'
        assert api.get_system_configuration.await_count == 1
        assert api.get_internal_squad_topology.await_count == 2
        api.get_split_access_snapshot.assert_awaited_once_with(12)
        api.update_internal_squads_once.assert_not_awaited()
        assert await recover(db, api, target, operation) == 'already_verified'
        assert api.get_system_configuration.await_count == 1


async def test_live_sender_not_probed_or_interrupted(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api, old=False)
        assert await recover(db, api, target, operation) == 'in_progress'
        api.get_system_configuration.assert_not_awaited()
        api.get_split_access_snapshot.assert_not_awaited()


async def test_wrong_owner_rejected_before_network(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        with pytest.raises(LedgerConflict):
            await recover(db, api, dict(target, user_id=2), operation)
        api.get_system_configuration.assert_not_awaited()


async def test_changed_topology_cannot_verify_old_attempt(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        original = api.get_internal_squad_topology.side_effect

        async def changed(squad):
            value = await original(squad)
            value['accessibleNodes'][0]['activeInbounds'] = ['different-inbound']
            return value

        api.get_internal_squad_topology.side_effect = changed
        assert await recover(db, api, target, operation) == 'attention'
        assert (await db.get(BsTrafficDelivery, operation)).state == 'attention'
        api.update_internal_squads_once.assert_not_awaited()


@pytest.mark.parametrize('failure', ['outage', 'configuration'])
async def test_unavailable_evidence_keeps_original_attempt(postgres_database, failure):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        if failure == 'outage':
            api.get_system_configuration.side_effect = RemnaWaveAPIError('Unavailable', status_code=503)
        else:
            api.get_system_configuration.return_value['service']['disableUserUsageRecords'] = True
        assert await recover(db, api, target, operation) == 'unconfirmed'
        assert (await db.get(BsTrafficDelivery, operation)).state == 'attempted'
        api.get_split_access_snapshot.assert_not_awaited()


async def test_fresh_recovery_does_not_extend_original_delivery_deadline(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        deadline = datetime.now(UTC) - timedelta(seconds=1)
        await db.execute(
            update(BsTrafficDelivery).where(BsTrafficDelivery.id == operation).values(valid_until=deadline)
        )
        await db.commit()
        assert await recover(db, api, target, operation) == 'verified'
        row = await db.get(BsTrafficDelivery, operation)
        assert row.valid_until == deadline
        assert row.state == 'verified'


@pytest.mark.parametrize('change', ['owner', 'generation', 'purchase', 'disabled'])
async def test_local_changes_during_topology_fetch_cannot_verify_old_access(postgres_database, change):
    from app.database.models import BsTrafficLedger, Subscription
    from tests.database.test_bs_traffic_ledger_postgres import issue

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        configuration = api.get_system_configuration.return_value

        async def changed():
            async with async_sessionmaker(db.bind)() as other, other.begin():
                if change == 'owner':
                    await other.execute(update(Subscription).where(Subscription.id == 1).values(user_id=2))
                elif change == 'generation':
                    await other.execute(
                        update(BsTrafficLedger)
                        .where(BsTrafficLedger.subscription_id == 1)
                        .values(account_generation='f' * 64)
                    )
                elif change == 'purchase':
                    await issue(other, account_generation=target['account_generation'])
                else:
                    await other.execute(update(User).where(User.id == 1).values(status='blocked'))
            return configuration

        api.get_system_configuration.side_effect = changed
        assert await recover(db, api, target, operation) == ('attention' if change == 'disabled' else 'obsolete')
        row = await db.get(BsTrafficDelivery, operation)
        assert row.state in ('attempted', 'attention') and row.token is not None
        api.update_internal_squads_once.assert_not_awaited()


async def test_recovery_deadline_preserves_attempt(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic import recovery_preflight

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        entered, cancelled = asyncio.Event(), asyncio.Event()

        async def blocked():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        api.get_system_configuration.side_effect = blocked
        monkeypatch.setattr(recovery_preflight, 'RECOVERY_TIMEOUT', 0.2)
        assert await recover(db, api, target, operation) == 'unconfirmed'
        assert entered.is_set() and cancelled.is_set()
        assert (await db.get(BsTrafficDelivery, operation)).state == 'attempted'
        api.get_split_access_snapshot.assert_not_awaited()


@pytest.mark.parametrize('state', ['pending', 'cancelled', 'not_sent'])
async def test_nonrecoverable_states_do_not_contact_panel(postgres_database, state):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        changes = dict(state=state)
        if state == 'not_sent':
            changes['not_sent_reason'] = 'stale_evidence'
        else:
            changes.update(token=None, attempted_at=None)
        await db.execute(update(BsTrafficDelivery).where(BsTrafficDelivery.id == operation).values(**changes))
        await db.commit()
        expected = 'not_sent' if state == 'not_sent' else 'not_attempted'
        assert await recover(db, api, target, operation) == expected
        api.get_system_configuration.assert_not_awaited()
        api.get_split_access_snapshot.assert_not_awaited()


async def test_recovery_sql_deadline_leaves_attempt_untouched(postgres_database, monkeypatch):
    from sqlalchemy import select

    from app.services.bs_traffic import recovery_preflight

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        monkeypatch.setattr(recovery_preflight, 'RECOVERY_TIMEOUT', 0.2)
        async with async_sessionmaker(db.bind)() as blocker, blocker.begin():
            await blocker.execute(select(User.id).where(User.id == 1).with_for_update())
            assert await recover(db, api, target, operation) == 'unconfirmed'
        assert (await db.get(BsTrafficDelivery, operation)).state == 'attempted'
        api.get_system_configuration.assert_not_awaited()


async def test_external_cancellation_is_not_reported_as_success(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        entered, cancelled = asyncio.Event(), asyncio.Event()

        async def blocked():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        api.get_system_configuration.side_effect = blocked
        task = asyncio.create_task(recover(db, api, target, operation))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
        assert (await db.get(BsTrafficDelivery, operation)).state == 'attempted'
        api.get_split_access_snapshot.assert_not_awaited()


async def test_fresh_readback_can_confirm_after_original_evidence_expired(postgres_database):
    """A crash must not strand an applied purchase merely because its proof aged."""
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target, operation = await attempt(db, api)
        row = await db.get(BsTrafficDelivery, operation)
        original_token = row.token
        expired = datetime.now(UTC) - timedelta(hours=1)
        row.valid_until = expired
        await db.commit()
        assert await recover(db, api, target, operation) == 'verified'
        await db.refresh(row)
        assert row.token == original_token and row.valid_until == expired
        api.update_internal_squads_once.assert_not_awaited()
