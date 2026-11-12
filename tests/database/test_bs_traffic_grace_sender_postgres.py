from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficGraceIntent, Subscription, Tariff, User
from app.services.bs_traffic.grace_intent import stage_restore
from app.services.bs_traffic.panel_binding import bind_reader
from tests.database.test_bs_traffic_grace_intent_postgres import SCOPE, TABLES, setup
from tests.database.test_bs_traffic_policy_postgres import evidence
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [*TABLES, BsTrafficCollectionBudget.__table__]


async def prepared(db, *, ordinary=False, external=None):
    args = await setup(db)
    sub = await db.get(Subscription, 1)
    owner = await db.get(User, 1)
    owner.remnawave_id = 12
    await db.flush()
    args['target'] = replace(args['target'], expire_at=sub.end_date, hwid_device_limit=sub.device_limit)
    if ordinary:
        from datetime import UTC, datetime, timedelta

        from app.database.models import BsTrafficLedger
        from app.services.bs_traffic.ledger import grant
        from tests.database.test_bs_traffic_policy_postgres import ORDINARY

        await grant(
            db,
            user_id=1,
            subscription_id=1,
            account_generation=args['account_generation'],
            group='ordinary',
            operation_key='new-paid',
            amount_bytes=1000,
            starts_at=datetime.now(UTC) - timedelta(seconds=1),
            expires_at=sub.end_date,
        )
        args['revision'] = (await db.get(BsTrafficLedger, 1)).revision
        args['target'] = replace(args['target'], active_internal_squads=[{'uuid': ORDINARY}])
    if external is not None:
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.external_squad_uuid = external
        args['target'] = replace(args['target'], external_squad_uuid=external)
        await db.flush()
    operation = await stage_restore(db, **args)
    await db.commit()
    remote = {'state': args['expected_source']}

    async def read(_):
        return remote['state']

    async def write(target):
        remote['state'] = target

    api = SimpleNamespace(
        base_url='https://panel.example',
        split_external_squad_exists=AsyncMock(return_value=True),
        get_split_grace_snapshot=AsyncMock(side_effect=read),
        restore_split_user_once=AsyncMock(side_effect=write),
    )
    factory = async_sessionmaker(db.bind, expire_on_commit=False)
    reader = await bind_reader(factory, api, panel_scope=SCOPE)
    keys = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
    return keys | {'operation_id': operation}, reader, api, remote


async def test_sender_applies_once_and_confirms_with_separate_get(postgres_database):
    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        api.restore_split_user_once.assert_awaited_once()
        assert api.get_split_grace_snapshot.await_count == 2
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        api.restore_split_user_once.assert_awaited_once()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'verified'


@pytest.mark.parametrize('changed', ['devices', 'strategy', 'squads'])
async def test_full_source_change_blocks_patch(postgres_database, changed):
    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, remote = await prepared(db)
        changes = {
            'devices': {'hwid_device_limit': 99},
            'strategy': {'traffic_limit_strategy': 'MONTH'},
            'squads': {'active_internal_squads': []},
        }
        remote['state'] = replace(remote['state'], **changes[changed])
        assert await send_restore(reader, **keys, evidence=evidence()) == 'source_changed'
        api.restore_split_user_once.assert_not_awaited()


async def test_timeout_after_applied_write_is_recovered_without_replay(postgres_database):
    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, remote = await prepared(db)

        async def write(target):
            remote['state'] = target
            raise TimeoutError

        api.restore_split_user_once.side_effect = write
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        api.restore_split_user_once.assert_awaited_once()


async def test_new_paid_group_is_restored_without_reviving_empty_bs(postgres_database):
    from app.services.bs_traffic.grace_sender import send_restore
    from tests.database.test_bs_traffic_policy_postgres import ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, remote = await prepared(db, ordinary=True)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        assert remote['state'].active_internal_squads == [{'uuid': ORDINARY}]
        api.restore_split_user_once.assert_awaited_once()


async def test_concurrent_senders_cannot_repeat_patch(postgres_database):
    import asyncio

    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        results = await asyncio.gather(*(send_restore(reader, **keys, evidence=evidence()) for _ in range(2)))
        assert results == ['verified', 'verified']
        api.restore_split_user_once.assert_awaited_once()


async def test_unapplied_timeout_stays_unconfirmed_without_replay(postgres_database):
    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        api.restore_split_user_once.side_effect = TimeoutError
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        api.restore_split_user_once.assert_awaited_once()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_expired_topology_cannot_authorize_patch(postgres_database):
    from datetime import UTC, datetime, timedelta

    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        stale = replace(evidence(), valid_until=datetime.now(UTC) - timedelta(seconds=1))
        assert await send_restore(reader, **keys, evidence=stale) == 'obsolete'
        api.restore_split_user_once.assert_not_awaited()


async def test_cancellation_during_patch_keeps_durable_attempt(postgres_database):
    import asyncio

    from app.services.bs_traffic.grace_sender import send_restore

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db)
        api.restore_split_user_once.side_effect = asyncio.CancelledError
        with pytest.raises(asyncio.CancelledError):
            await send_restore(reader, **keys, evidence=evidence())
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'
        await db.rollback()
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        api.restore_split_user_once.assert_awaited_once()
