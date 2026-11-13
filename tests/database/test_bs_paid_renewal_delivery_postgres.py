"""Renewal delivery keeps paid receipts durable and never replays unknown PATCH."""

from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from app.database.models import BsTrafficRenewalIntent, Subscription, Transaction
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepared(db, monkeypatch):
    _, reader, keys, _ = await prepared_purchase(db, monkeypatch)
    sub = await db.get(Subscription, keys['subscription_id'])
    sub.end_date += timedelta(days=30)
    tx = Transaction(
        user_id=1,
        type='subscription_payment',
        amount_kopeks=-2500,
        payment_method='balance',
        external_id='split-renewal:synthetic',
        is_completed=True,
    )
    db.add(tx)
    await db.flush()
    intent = BsTrafficRenewalIntent(
        subscription_id=1,
        operation_key=tx.external_id,
        transaction_id=tx.id,
        charged_kopeks=2500,
        parameters={},
        panel_scope=reader.panel_scope,
        account_generation=keys['account_generation'],
        intended_expires_at=sub.end_date,
    )
    db.add(intent)
    await db.commit()
    remote = {'state': reader._api.get_split_grace_snapshot.return_value}
    reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

    async def write(target):
        remote['state'] = target

    reader._api.restore_split_user_once = AsyncMock(side_effect=write)
    return reader, keys | {'operation_id': intent.id}, remote, sub.end_date


async def test_expiry_and_full_paid_state_delivered_once(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, end = await prepared(db, monkeypatch)
        old_counter = remote['state'].used_traffic_bytes
        assert await advance_renewal_delivery(reader, **keys) == 'verified'
        assert remote['state'].expire_at == end
        assert remote['state'].status == 'ACTIVE'
        assert remote['state'].traffic_limit_bytes == 0
        assert remote['state'].traffic_limit_strategy == 'NO_RESET'
        assert remote['state'].used_traffic_bytes == old_counter
        assert await advance_renewal_delivery(reader, **keys) == 'already_verified'
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('applied', [False, True])
async def test_unknown_attempt_only_reads_back(postgres_database, monkeypatch, applied):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, _ = await prepared(db, monkeypatch)

        async def uncertain(target):
            if applied:
                remote['state'] = replace(target, used_traffic_bytes=target.used_traffic_bytes + 3)
            raise TimeoutError

        reader._api.restore_split_user_once.side_effect = uncertain
        await advance_renewal_delivery(reader, **keys)
        result = await advance_renewal_delivery(reader, **keys)
        assert result == ('verified' if applied else 'unconfirmed')
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_grace_overlay_remains_pending(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, _ = await prepared(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        sub.grace_session_open = True
        await db.commit()
        before = remote['state']
        assert await advance_renewal_delivery(reader, **keys) == 'blocked_grace'
        assert remote['state'] == before
        reader._api.restore_split_user_once.assert_not_awaited()
        row = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
        assert row.state == 'pending' and row.attempt_token is None
        await db.rollback()


@pytest.mark.parametrize('change', ['owner', 'amount', 'expiry', 'status', 'generation', 'missing_receipt'])
async def test_stale_or_unpaid_intent_never_writes(postgres_database, monkeypatch, change):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await prepared(db, monkeypatch)
        row = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
        tx = await db.get(Transaction, row.transaction_id)
        sub = await db.get(Subscription, 1)
        if change == 'owner':
            keys['user_id'] = 2
        elif change == 'amount':
            tx.amount_kopeks = -1
        elif change == 'expiry':
            sub.end_date += timedelta(days=1)
        elif change == 'status':
            sub.status = 'disabled'
        elif change == 'generation':
            keys['account_generation'] = 'f' * 64
        else:
            row.transaction_id = None
        await db.commit()
        assert await advance_renewal_delivery(reader, **keys) in ('obsolete', 'pending_receipt')
        reader._api.restore_split_user_once.assert_not_awaited()


async def test_concurrent_delivery_has_one_durable_claim(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, end = await prepared(db, monkeypatch)
        results = await asyncio.gather(*(advance_renewal_delivery(reader, **keys) for _ in range(2)))
        assert 'verified' in results
        assert remote['state'].expire_at == end
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_pending_grace_resumes_only_after_overlay_is_closed(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await prepared(db, monkeypatch)
        sub = await db.get(Subscription, 1)
        sub.grace_session_open = True
        await db.commit()
        assert await advance_renewal_delivery(reader, **keys) == 'blocked_grace'
        sub.grace_session_open = False
        await db.commit()
        assert await advance_renewal_delivery(reader, **keys) == 'verified'


async def test_provider_receipt_requires_exact_persisted_metadata(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await prepared(db, monkeypatch)
        row = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
        tx = await db.get(Transaction, row.transaction_id)
        tx.payment_method, tx.external_id = 'synthetic-provider', 'synthetic-invoice'
        row.parameters = {
            'payment_receipt': {
                'method': tx.payment_method,
                'external_id': tx.external_id,
                'amount_kopeks': tx.amount_kopeks,
            }
        }
        await db.commit()
        assert await advance_renewal_delivery(reader, **keys) == 'verified'


@pytest.mark.parametrize(
    'ordinary,bs', [('finite', 'finite'), ('finite', 'unlimited'), ('unlimited', 'finite'), ('unlimited', 'unlimited')]
)
async def test_entitled_groups_are_selected_without_counter_reset(postgres_database, monkeypatch, ordinary, bs):
    from datetime import UTC, datetime

    from app.database.models import BsTrafficLedger, Tariff
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from app.services.bs_traffic.ledger import grant
    from app.services.bs_traffic.policy import configure_groups
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, end = await prepared(db, monkeypatch)
        owner = {key: value for key, value in keys.items() if key != 'operation_id'}
        for group in ('ordinary', 'bs'):
            await grant(
                db,
                **owner,
                group=group,
                operation_key=f'synthetic-renewed-{group}',
                amount_bytes=500,
                starts_at=datetime.now(UTC) - timedelta(seconds=1),
                expires_at=end,
            )
        state = await db.get(BsTrafficLedger, 1)
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.traffic_limit_gb = 0 if ordinary == 'unlimited' else 10
        tariff.bs_traffic_settings = {
            **tariff.bs_traffic_settings,
            'limit_gb': 0 if bs == 'unlimited' else 5,
            'topup_enabled': bs != 'unlimited',
        }
        await db.flush()
        evidence = await collect_topology_evidence(
            reader, panel_identity=reader.panel_scope, ordinary_squads=(ORDINARY,), bs_squads=(BS,)
        )
        await configure_groups(
            db, **owner, ordinary_mode=ordinary, bs_mode=bs, evidence=evidence, expected_revision=state.revision
        )
        await db.commit()
        counter = remote['state'].used_traffic_bytes
        assert await advance_renewal_delivery(reader, **keys) == 'verified'
        assert {item['uuid'] for item in remote['state'].active_internal_squads} == {ORDINARY, BS}
        assert remote['state'].used_traffic_bytes == counter


async def test_changed_topology_never_activates_paid_account(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await prepared(db, monkeypatch)
        original = reader._api.get_internal_squad_topology.side_effect

        async def changed(squad):
            value = await original(squad)
            value['accessibleNodes'][0]['activeInbounds'] = ['changed']
            return value

        reader._api.get_internal_squad_topology.side_effect = changed
        assert await advance_renewal_delivery(reader, **keys) == 'obsolete'
        reader._api.restore_split_user_once.assert_not_awaited()


async def test_renewal_migration_preserves_receipts(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import inspect

    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0155_bs_paid_renewal.py')
    async with postgres_session(postgres_database, TABLES) as db:
        connection = await db.connection()

        def cycle(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()
                columns = {column['name']: column for column in inspect(sync).get_columns('bs_traffic_renewal_intents')}
                assert columns['transaction_id']['nullable']
                assert not columns['parameters']['nullable']
                _load('0160_bs_renewal_superseded.py').upgrade()

        await connection.run_sync(cycle)
        await prepared(db, monkeypatch)
        connection = await db.connection()

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)):
                with pytest.raises(RuntimeError, match='receipts'):
                    migration.downgrade()

        await connection.run_sync(refuse)


async def test_applied_unknown_recovers_after_usage_revision(postgres_database, monkeypatch):
    from app.database.models import BsTrafficLedger
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, _ = await prepared(db, monkeypatch)

        async def uncertain(target):
            remote['state'] = target
            raise TimeoutError

        reader._api.restore_split_user_once.side_effect = uncertain
        assert await advance_renewal_delivery(reader, **keys) == 'unconfirmed'
        state = await db.get(BsTrafficLedger, 1)
        state.revision += 1
        await db.commit()
        assert await advance_renewal_delivery(reader, **keys) == 'verified'
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_known_unsent_source_change_preserves_attempt_and_reprepares(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, _ = await prepared(db, monkeypatch)
        calls = 0

        async def read(_):
            nonlocal calls
            calls += 1
            if calls == 2:
                remote['state'] = replace(remote['state'], hwid_device_limit=99)
            return remote['state']

        reader._api.get_split_grace_snapshot.side_effect = read
        assert await advance_renewal_delivery(reader, **keys) == 'source_changed'
        reader._api.restore_split_user_once.assert_not_awaited()
        row = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
        assert row.state == 'pending'
        assert len(row.attempt_history) == 1
        assert row.attempt_history[0]['reason'] == 'source_changed'
        token = row.attempt_history[0]['attempt_token']
        await db.rollback()
        assert await advance_renewal_delivery(reader, **keys) == 'verified'
        await db.refresh(row)
        assert row.attempt_token != token
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_paid_checkout_restart_delivers_once_without_second_charge(postgres_database, monkeypatch):
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import BsTrafficAllowance, User
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
    from tests.database.test_bs_paid_renewal_postgres import prepared as prepare_checkout
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub = await prepare_checkout(db, monkeypatch)
        old_end = sub.end_date
        remote = {'state': reader._api.get_split_grace_snapshot.return_value}
        initial_counter = remote['state'].used_traffic_bytes
        reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

        async def write(target):
            remote['state'] = target

        reader._api.restore_split_user_once = AsyncMock(side_effect=write)
        purchase = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='renew:restart-end-to-end'
        )
        await db.commit()
        assert (await db.get(User, 1)).balance_kopeks == 8000
        assert sub.end_date == old_end + timedelta(days=30)
        grant_ids = set(purchase.grants.values())
        assert len(grant_ids) == 2
        await db.rollback()
        # Delivery and replay use independent sessions, as after a process restart.
        assert await advance_renewal_delivery(reader, **owner, operation_id=purchase.intent_id) == 'verified'
        assert remote['state'].expire_at == old_end + timedelta(days=30)
        assert {item['uuid'] for item in remote['state'].active_internal_squads} == {ORDINARY, BS}
        assert remote['state'].used_traffic_bytes == initial_counter
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        async with factory() as restarted, restarted.begin():
            replay = await renew_from_balance(
                restarted, **owner, period_days=30, expected_price_kopeks=2000, operation_key='renew:restart-end-to-end'
            )
            assert replay.replayed and replay.intent_id == purchase.intent_id
            assert set(replay.grants.values()) == grant_ids
            assert (await restarted.get(User, 1)).balance_kopeks == 8000
            assert await restarted.scalar(select(func.count()).select_from(Transaction)) == 1
            grants = (
                await restarted.scalars(select(BsTrafficAllowance).where(BsTrafficAllowance.id.in_(grant_ids)))
            ).all()
            assert {grant.traffic_group for grant in grants} == {'ordinary', 'bs'}
        assert await advance_renewal_delivery(reader, **owner, operation_id=purchase.intent_id) == 'already_verified'
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_claim_stops_at_next_base_extension_boundary(postgres_database, monkeypatch):
    from datetime import UTC, datetime

    from sqlalchemy import select

    from app.database.models import BsTrafficAllowance, BsTrafficBaseExtension
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, end = await prepared(db, monkeypatch)
        base = await db.scalar(select(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'ordinary'))
        boundary = datetime.now(UTC) + timedelta(seconds=20)
        db.add(
            BsTrafficBaseExtension(
                subscription_id=1,
                allowance_id=base.id,
                traffic_group='ordinary',
                operation_key='synthetic-future-extension',
                request_hash='a' * 64,
                requested_at=datetime.now(UTC),
                starts_at=boundary,
                expires_at=end,
            )
        )
        await db.commit()
        assert await advance_renewal_delivery(reader, **keys) == 'verified'
        row = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
        assert row.valid_until == boundary
        assert row.send_deadline <= boundary


@pytest.mark.parametrize('lost_reply', [False, True])
@pytest.mark.parametrize('drift_ms', [0, -1, 1])
async def test_panel_expiry_precision_recovers_without_replaying_write(
    postgres_database, monkeypatch, lost_reply, drift_ms
):
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, end = await prepared(db, monkeypatch)
        exact_end = end.replace(microsecond=330834)
        sub = await db.get(Subscription, keys['subscription_id'])
        intent = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
        sub.end_date = intent.intended_expires_at = exact_end
        await db.commit()

        async def panel_write(target):
            panel_end = target.expire_at.replace(microsecond=330000) + timedelta(milliseconds=drift_ms)
            remote['state'] = replace(target, expire_at=panel_end)
            if lost_reply:
                raise TimeoutError('Synthetic lost response after panel accepted the write')

        reader._api.restore_split_user_once = AsyncMock(side_effect=panel_write)
        await advance_renewal_delivery(reader, **keys)
        outcome = await advance_renewal_delivery(reader, **keys)
        assert outcome in ('verified', 'already_verified') if drift_ms == 0 else outcome == 'unconfirmed'
        assert reader._api.restore_split_user_once.await_count == 1
        await db.refresh(intent)
        assert intent.state == ('verified' if drift_ms == 0 else 'attempted')
