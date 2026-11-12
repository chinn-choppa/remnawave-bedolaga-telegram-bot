"""Opt-in disconnect follows verified exhaustion, never money or access retries."""

from unittest.mock import AsyncMock

import pytest

from app.config import settings
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_verified_exhaustion_drops_only_bs_once(postgres_database, monkeypatch):
    from app.services.bs_traffic.connection_drop import drop_after_delivery
    from app.services.bs_traffic.preparation import prepare_delivery
    from app.services.bs_traffic.sender import send_delivery
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, keys, account = await prepared_purchase(db, monkeypatch)
        account.active_internal_squads = [{'uuid': ORDINARY}, {'uuid': BS}]
        monkeypatch.setattr(settings, 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED', True)
        reader._api.request_user_connection_drop = AsyncMock()
        prepared = await prepare_delivery(
            reader._factory, reader, **keys, operation_key='quota-exhaustion', settle_usage=False
        )
        assert prepared.status == 'queued'
        assert await drop_after_delivery(reader, **keys, operation_id=prepared.operation_id) == 'waiting'
        assert (
            await send_delivery(
                reader._factory,
                reader._api,
                **keys,
                operation_id=prepared.operation_id,
                evidence=prepared.evidence,
                current_guard_hash=prepared.guard_hash,
                bound_reader=reader,
            )
            == 'verified'
        )
        assert await drop_after_delivery(reader, **keys, operation_id=prepared.operation_id) == 'accepted'
        assert await drop_after_delivery(reader, **keys, operation_id=prepared.operation_id) == 'accepted'
        reader._api.request_user_connection_drop.assert_awaited_once()
        nodes = reader._api.request_user_connection_drop.call_args.args[1]
        assert nodes == list(prepared.evidence.topology.bs_nodes)
        assert {item['uuid'] for item in account.active_internal_squads} == {ORDINARY}


@pytest.mark.parametrize('change', ['opt_out', 'grace', 'renewed_revision', 'changed_generation', 'unknown'])
async def test_drop_revalidates_and_never_retries_unknown(postgres_database, monkeypatch, change):
    from app.database.models import BsTrafficDelivery, BsTrafficLedger, Subscription
    from app.services.bs_traffic.connection_drop import drop_after_delivery
    from app.services.bs_traffic.preparation import prepare_delivery
    from app.services.bs_traffic.sender import send_delivery
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, keys, account = await prepared_purchase(db, monkeypatch)
        account.active_internal_squads = [{'uuid': ORDINARY}, {'uuid': BS}]
        monkeypatch.setattr(settings, 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED', True)
        reader._api.request_user_connection_drop = AsyncMock()
        prepared = await prepare_delivery(
            reader._factory, reader, **keys, operation_key='quota-exhaustion', settle_usage=False
        )
        assert (
            await send_delivery(
                reader._factory,
                reader._api,
                **keys,
                operation_id=prepared.operation_id,
                evidence=prepared.evidence,
                current_guard_hash=prepared.guard_hash,
                bound_reader=reader,
            )
            == 'verified'
        )
        if change == 'opt_out':
            monkeypatch.setattr(settings, 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED', False)
        elif change == 'grace':
            (await db.get(Subscription, keys['subscription_id'])).grace_session_open = True
        elif change == 'changed_generation':
            (await db.get(BsTrafficDelivery, prepared.operation_id)).guard_hash = '0' * 64
        elif change == 'renewed_revision':
            (await db.get(BsTrafficLedger, keys['subscription_id'])).revision += 1
        else:
            reader._api.request_user_connection_drop.side_effect = TimeoutError
        await db.commit()
        result = await drop_after_delivery(reader, **keys, operation_id=prepared.operation_id)
        assert result == ('unknown' if change == 'unknown' else 'cancelled')
        await drop_after_delivery(reader, **keys, operation_id=prepared.operation_id)
        assert reader._api.request_user_connection_drop.await_count == (1 if change == 'unknown' else 0)


async def test_access_write_holds_binding_until_request_completes(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.panel_binding import retire_binding
    from app.services.bs_traffic.preparation import prepare_delivery
    from app.services.bs_traffic.sender import send_delivery
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, keys, account = await prepared_purchase(db, monkeypatch)
        account.active_internal_squads = [{'uuid': ORDINARY}, {'uuid': BS}]
        prepared = await prepare_delivery(
            reader._factory, reader, **keys, operation_key='restriction', settle_usage=False
        )
        retired_before_write = []
        retiring = None

        async def retire():
            async with reader._factory() as other, other.begin():
                await retire_binding(other, panel_scope=reader.panel_scope)

        async def write(user_id, squads):
            nonlocal retiring
            retiring = asyncio.create_task(retire())
            await asyncio.sleep(0.1)
            retired_before_write.append(retiring.done())
            account.active_internal_squads = [{'uuid': item} for item in squads]
            return account

        reader._api.update_internal_squads_once.side_effect = write
        await send_delivery(
            reader._factory,
            reader._api,
            **keys,
            operation_id=prepared.operation_id,
            evidence=prepared.evidence,
            current_guard_hash=prepared.guard_hash,
            bound_reader=reader,
        )
        if retiring:
            await retiring
        assert retired_before_write == [False]


async def test_drop_migration_retains_attempt_history(postgres_database, monkeypatch):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.database.models import BsTrafficDelivery
    from app.services.bs_traffic.preparation import prepare_delivery
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0163_bs_connection_drop.py')
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, keys, account = await prepared_purchase(db, monkeypatch)

        def reinstall(connection):
            with Operations.context(MigrationContext.configure(connection)):
                migration.downgrade()
                migration.upgrade()
                migration.upgrade()
            indexes = {item['name']: item for item in sa.inspect(connection).get_indexes('bs_traffic_deliveries')}
            assert indexes['ix_bs_delivery_drop_pending']['column_names'] == ['id']

        await (await db.connection()).run_sync(reinstall)
        await db.commit()
        account.active_internal_squads = [{'uuid': ORDINARY}, {'uuid': BS}]
        monkeypatch.setattr(settings, 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED', True)
        prepared = await prepare_delivery(
            reader._factory, reader, **keys, operation_key='quota-drop', settle_usage=False
        )
        assert (await db.get(BsTrafficDelivery, prepared.operation_id)).connection_drop['state'] == 'pending'

        def refuse(connection):
            with (
                Operations.context(MigrationContext.configure(connection)),
                pytest.raises(RuntimeError, match='history'),
            ):
                migration.downgrade()

        await (await db.connection()).run_sync(refuse)


@pytest.mark.parametrize('override', [{'user_id': 2}, {'account_generation': '0' * 64}])
async def test_wrong_owner_cannot_request_disconnect(postgres_database, monkeypatch, override):
    from app.services.bs_traffic.connection_drop import drop_after_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, keys, _ = await prepared_purchase(db, monkeypatch)
        reader._api.request_user_connection_drop = AsyncMock()
        assert await drop_after_delivery(reader, **(keys | override), operation_id=1) == 'unknown'
        reader._api.request_user_connection_drop.assert_not_awaited()
