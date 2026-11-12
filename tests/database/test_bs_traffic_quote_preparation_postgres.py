from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update

from app.database.models import BsTrafficPanelBinding, Subscription, Tariff
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.quote_preparation import prepare_package_quote
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_trusted_reader_quotes_without_purchase_or_panel_write(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, _, _ = await purchased(db, monkeypatch)
        before = await totals(db)
        quote = await prepare_package_quote(
            reader, user_id=target['user_id'], subscription_id=target['subscription_id'], group='bs', gb=10
        )
        assert quote.price_kopeks == 2500
        assert await totals(db) == before
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_wrong_owner_is_rejected_before_network(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, _, _ = await purchased(db, monkeypatch)
        reader._api.get_split_access_snapshot.reset_mock()
        with pytest.raises(LedgerConflict):
            await prepare_package_quote(reader, user_id=2, subscription_id=target['subscription_id'], group='bs', gb=10)
        reader._api.get_split_access_snapshot.assert_not_awaited()


async def test_tariff_edit_during_read_rejects_old_snapshot(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, _, account = await purchased(db, monkeypatch)

        async def read(_):
            async with reader._factory() as other, other.begin():
                tariff_id = await other.scalar(
                    select(Subscription.tariff_id).where(Subscription.id == target['subscription_id'])
                )
                await other.execute(update(Tariff).where(Tariff.id == tariff_id).values(traffic_limit_gb=0))
            return account

        reader._api.get_split_access_snapshot = AsyncMock(side_effect=read)
        with pytest.raises(LedgerConflict):
            await prepare_package_quote(reader, user_id=1, subscription_id=target['subscription_id'], group='bs', gb=10)
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_retired_binding_does_not_return_a_quote(postgres_database, monkeypatch):
    from datetime import UTC, datetime

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, _, _ = await purchased(db, monkeypatch)
        await db.execute(update(BsTrafficPanelBinding).values(retired_at=datetime.now(UTC)))
        await db.commit()
        with pytest.raises(LedgerConflict):
            await prepare_package_quote(reader, user_id=1, subscription_id=target['subscription_id'], group='bs', gb=10)


async def test_replaced_panel_account_cannot_supply_a_quote(postgres_database, monkeypatch):
    from datetime import timedelta

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, _, account = await purchased(db, monkeypatch)
        before = await totals(db)
        account.created_at += timedelta(days=1)
        with pytest.raises(LedgerConflict, match='Panel account changed'):
            await prepare_package_quote(reader, user_id=1, subscription_id=target['subscription_id'], group='bs', gb=10)
        assert await totals(db) == before
        reader._api.update_internal_squads_once.assert_not_awaited()
