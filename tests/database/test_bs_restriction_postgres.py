"""Expiry/admin revocation does not depend on traffic telemetry or topology HTTP."""

from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import Subscription, User
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepare(db, monkeypatch, *, blocked=False):
    _, reader, keys, _ = await prepared_purchase(db, monkeypatch)
    sub = await db.get(Subscription, keys['subscription_id'])
    if blocked:
        (await db.get(User, sub.user_id)).status = 'blocked'
    else:
        sub.end_date = await db.scalar(select(func.clock_timestamp())) - timedelta(seconds=1)
    await db.commit()
    remote = {'state': reader._api.get_split_grace_snapshot.return_value}
    reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

    async def write(target):
        remote['state'] = target

    reader._api.restrict_split_user_once = AsyncMock(side_effect=write)
    return reader, keys, remote


@pytest.mark.parametrize('blocked', [False, True])
async def test_revoke_without_usage_or_topology_read(postgres_database, monkeypatch, blocked):
    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote = await prepare(db, monkeypatch, blocked=blocked)
        before = remote['state']
        assert await restrict_account(reader, **keys) == 'verified'
        assert remote['state'].status == 'DISABLED'
        assert remote['state'].active_internal_squads == []
        assert remote['state'].used_traffic_bytes == before.used_traffic_bytes
        assert await restrict_account(reader, **keys) == 'verified'
        assert reader._api.restrict_split_user_once.await_count == 1


@pytest.mark.parametrize('applied', [True, False])
async def test_unknown_restriction_is_never_resent(postgres_database, monkeypatch, applied):
    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote = await prepare(db, monkeypatch)

        async def write(target):
            if applied:
                remote['state'] = replace(target, used_traffic_bytes=target.used_traffic_bytes + 5)
            raise TimeoutError

        reader._api.restrict_split_user_once.side_effect = write
        await restrict_account(reader, **keys)
        assert await restrict_account(reader, **keys) == ('verified' if applied else 'unconfirmed')
        assert reader._api.restrict_split_user_once.await_count == 1


async def test_new_paid_term_or_open_grace_prevents_restriction(postgres_database, monkeypatch):
    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote = await prepare(db, monkeypatch)
        sub = await db.get(Subscription, keys['subscription_id'])
        sub.grace_session_open = True
        await db.commit()
        assert await restrict_account(reader, **keys) == 'not_required'
        sub.grace_session_open = False
        sub.end_date += timedelta(days=30)
        await db.commit()
        assert await restrict_account(reader, **keys) == 'not_required'
        reader._api.restrict_split_user_once.assert_not_awaited()


async def test_fleet_revokes_expired_account_when_telemetry_is_unavailable(postgres_database, monkeypatch):
    from types import SimpleNamespace

    from app.database.models import BsTrafficFleetWork
    from app.services.bs_traffic.fleet_settlement import _settle

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote = await prepare(db, monkeypatch)
        db.add(
            BsTrafficFleetWork(
                subscription_id=keys['subscription_id'],
                panel_scope=reader.panel_scope,
                account_generation=keys['account_generation'],
                target_round='offline-round',
                claim_token='offline-lease',
                due_at=await db.scalar(select(func.clock_timestamp())),
                access_dirty=True,
            )
        )
        await db.commit()
        cache = SimpleNamespace(get=AsyncMock(side_effect=TimeoutError))
        assert await _settle(reader._factory, reader, cache, keys['subscription_id'], 'offline-lease')
        cache.get.assert_not_awaited()
        assert remote['state'].status == 'DISABLED'
        await db.refresh(await db.get(BsTrafficFleetWork, keys['subscription_id']))
        work = await db.get(BsTrafficFleetWork, keys['subscription_id'])
        assert work.applied_squads == [] and not work.access_dirty


async def test_unknown_restriction_fences_new_paid_entitlements(postgres_database, monkeypatch):
    from app.database.models import Transaction
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _ = await prepare(db, monkeypatch)
        reader._api.restrict_split_user_once.side_effect = TimeoutError
        assert await restrict_account(reader, **keys) == 'unconfirmed'
        user = await db.get(User, keys['user_id'])
        balance = user.balance_kopeks
        before = await db.scalar(select(func.count()).select_from(Transaction))
        with pytest.raises(LedgerConflict):
            await renew_from_balance(
                db, **keys, period_days=30, expected_price_kopeks=2000, operation_key='must-wait-for-restriction'
            )
        await db.commit()
        assert user.balance_kopeks == balance
        assert await db.scalar(select(func.count()).select_from(Transaction)) == before


async def test_concurrent_restriction_sends_only_one_request(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _ = await prepare(db, monkeypatch)
        results = await asyncio.gather(restrict_account(reader, **keys), restrict_account(reader, **keys))
        assert 'verified' in results
        assert reader._api.restrict_split_user_once.await_count == 1


async def test_paid_extension_during_observation_cancels_unsent_restriction(postgres_database, monkeypatch):
    from app.services.bs_traffic import restriction

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _ = await prepare(db, monkeypatch)
        original = restriction._read

        async def read_then_renew(*args):
            result = await original(*args)
            async with reader._factory() as other, other.begin():
                sub = await other.get(Subscription, keys['subscription_id'])
                sub.end_date += timedelta(days=30)
            return result

        monkeypatch.setattr(restriction, '_read', read_then_renew)
        assert await restriction.restrict_account(reader, **keys) == 'not_required'
        reader._api.restrict_split_user_once.assert_not_awaited()


async def test_restriction_migration_preserves_unknown_attempt(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.services.bs_traffic.restriction import restrict_account
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0161_bs_account_restriction.py')
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _ = await prepare(db, monkeypatch)

        def install(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()
                migration.upgrade()

        await (await db.connection()).run_sync(install)
        await db.commit()
        reader._api.restrict_split_user_once.side_effect = TimeoutError
        assert await restrict_account(reader, **keys) == 'unconfirmed'

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)), pytest.raises(RuntimeError, match='history'):
                migration.downgrade()

        await (await db.connection()).run_sync(refuse)


async def test_pending_restriction_keeps_paid_balances_readable(postgres_database, monkeypatch):
    from app.services.bs_traffic.customer_summary import customer_summary
    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _ = await prepare(db, monkeypatch)
        reader._api.restrict_split_user_once.side_effect = TimeoutError
        await restrict_account(reader, **keys)
        result = await customer_summary(db, user_id=keys['user_id'], subscription_id=keys['subscription_id'])
        assert set(result['groups']) == {'ordinary', 'bs'}


async def test_retirement_cannot_overtake_final_restriction_write(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.panel_binding import retire_binding
    from app.services.bs_traffic.restriction import restrict_account

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote = await prepare(db, monkeypatch)
        retiring = None
        retired_before_write = []

        async def retire():
            async with reader._factory() as other, other.begin():
                await retire_binding(other, panel_scope=reader.panel_scope)

        async def write(target):
            nonlocal retiring
            retiring = asyncio.create_task(retire())
            await asyncio.sleep(0.1)
            retired_before_write.append(retiring.done())
            remote['state'] = target

        reader._api.restrict_split_user_once.side_effect = write
        await restrict_account(reader, **keys)
        if retiring is not None:
            await retiring
        assert retired_before_write == [False]
