"""Superseded means covered by a later verified paid term, never a fake PATCH receipt."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficRenewalIntent, Transaction, User
from app.services.bs_traffic.renewal import renew_from_balance
from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def two_payments(db, monkeypatch):
    reader, owner, sub = await prepared(db, monkeypatch)
    first = await renew_from_balance(db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='first')
    await db.commit()
    second = await renew_from_balance(db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='second')
    await db.commit()
    remote = {'state': reader._api.get_split_grace_snapshot.return_value}
    reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

    async def write(target):
        remote['state'] = target

    reader._api.restore_split_user_once = AsyncMock(side_effect=write)
    return reader, owner, sub, first, second


async def test_verified_newer_paid_term_supersedes_only_unsent_receipt(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, first, second = await two_payments(db, monkeypatch)
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'verified'
        old = await db.get(BsTrafficRenewalIntent, first.intent_id, populate_existing=True)
        assert old.state == 'superseded' and old.verified_at is None and old.attempt_token is None
        assert old.parameters['superseded_by_intent_id'] == second.intent_id
        assert await advance_renewal_delivery(reader, **owner, operation_id=first.intent_id) == 'superseded'
        again = await renew_from_balance(db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='first')
        assert again.replayed and not again.delivery_pending
        assert (await db.get(User, sub.user_id)).balance_kopeks == 6000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_unconfirmed_successor_does_not_supersede_pending(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, first, second = await two_payments(db, monkeypatch)
        reader._api.restore_split_user_once.side_effect = TimeoutError
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'unconfirmed'
        old = await db.get(BsTrafficRenewalIntent, first.intent_id, populate_existing=True)
        assert old.state == 'pending' and 'superseded_by_intent_id' not in old.parameters


async def test_invalid_old_financial_receipt_is_not_superseded(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, first, second = await two_payments(db, monkeypatch)
        (await db.get(Transaction, first.transaction_id)).amount_kopeks = -1
        await db.commit()
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'verified'
        assert (await db.get(BsTrafficRenewalIntent, first.intent_id, populate_existing=True)).state == 'pending'


async def test_supersede_never_changes_unknown_claim(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_delivery import _supersede_pending

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, first, second = await two_payments(db, monkeypatch)
        reader._api.restore_split_user_once.side_effect = TimeoutError
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'unconfirmed'
        unknown = await db.get(BsTrafficRenewalIntent, second.intent_id, populate_existing=True)
        immutable = (unknown.attempt_token, unknown.target_hash, unknown.guard_hash, unknown.parameters)
        # Give the first receipt a later synthetic ID/expiry only to challenge the
        # cleanup predicate against an actual durable unknown attempt.
        newer = BsTrafficRenewalIntent(
            id=unknown.id + 1,
            subscription_id=unknown.subscription_id,
            panel_scope=unknown.panel_scope,
            account_generation=unknown.account_generation,
            state='verified',
            verified_at=unknown.attempted_at,
            created_at=unknown.created_at,
            intended_expires_at=unknown.intended_expires_at.replace(year=unknown.intended_expires_at.year + 1),
        )
        await _supersede_pending(db, newer, owner['user_id'])
        await db.refresh(unknown)
        assert unknown.state == 'attempted'
        assert (unknown.attempt_token, unknown.target_hash, unknown.guard_hash, unknown.parameters) == immutable


async def test_superseded_migration_preserves_money_and_unknown_claims(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.database.test_retired_gateway_migration_postgres import _load

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, first, second = await two_payments(db, monkeypatch)
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'verified'
        connection = await db.connection()

        def cycle(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration = _load('0159_bs_renewal_superseded.py')
                migration.downgrade()
                migration.upgrade()

        await connection.run_sync(cycle)
        await db.commit()
        assert (await db.get(BsTrafficRenewalIntent, first.intent_id, populate_existing=True)).state == 'pending'
        assert (await db.get(BsTrafficRenewalIntent, second.intent_id, populate_existing=True)).state == 'verified'
        await db.rollback()
        assert await advance_renewal_delivery(reader, **owner, operation_id=second.intent_id) == 'already_verified'
        assert (await db.get(BsTrafficRenewalIntent, first.intent_id, populate_existing=True)).state == 'superseded'
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
