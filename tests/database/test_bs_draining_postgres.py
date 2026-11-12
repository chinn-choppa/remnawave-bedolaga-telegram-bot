"""Stopping new BS sales preserves already paid grants and their receipts."""

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficAllowance, BsTrafficLedger
from app.services.bs_traffic import policy
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.purchase import purchase_package
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_draining_rejects_new_purchase_without_charging(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, _ = await prepared_purchase(db, monkeypatch)
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        state.access_policy = dict(state.access_policy, bs_purchase_enabled=False)
        await db.commit()
        before = await totals(db)
        with pytest.raises(LedgerConflict, match='BS purchases'):
            await purchase_package(db, **args)
        assert await totals(db) == before


async def test_stop_and_resume_preserve_paid_receipt(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, _ = await prepared_purchase(db, monkeypatch)
        paid = await purchase_package(db, **args)
        await db.commit()
        before = await totals(db)
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        grants = select(
            BsTrafficAllowance.id,
            BsTrafficAllowance.remaining_bytes,
            BsTrafficAllowance.starts_at,
            BsTrafficAllowance.expires_at,
        ).order_by(BsTrafficAllowance.id)
        original_grants = (await db.execute(grants)).all()
        for enabled in (False, True, False):
            revision = await policy.set_bs_purchase_enabled(
                db, **target, enabled=enabled, expected_revision=state.revision
            )
            await db.commit()
            assert state.revision == revision
            replay = await purchase_package(db, **args)
            assert replay.replayed and replay.grant_id == paid.grant_id
            assert await totals(db) == before
            assert (await db.execute(grants)).all() == original_grants


async def test_paid_delivery_completes_after_sales_stop(postgres_database, monkeypatch):
    from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        args, reader, target, _ = await prepared_purchase(db, monkeypatch)
        paid = await purchase_package(db, **args)
        await db.commit()
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        await policy.set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=state.revision)
        await db.commit()
        before = (await totals(db))[:3]
        await db.rollback()
        assert await advance_purchase_delivery(reader, **target, operation_id=paid.delivery_id) == 'verified'
        assert (await totals(db))[:3] == before
        reader._api.update_internal_squads_once.assert_awaited_once()


@pytest.mark.parametrize('case', ['foreign_owner', 'stale_revision', 'rollback'])
async def test_state_change_requires_owner_revision_and_commit(postgres_database, monkeypatch, case):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        revision = state.revision
        if case == 'rollback':
            await policy.set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=revision)
            await db.rollback()
        else:
            with pytest.raises(LedgerConflict):
                await policy.set_bs_purchase_enabled(
                    db,
                    **(target | {'user_id': 2} if case == 'foreign_owner' else target),
                    enabled=False,
                    expected_revision=revision - 1 if case == 'stale_revision' else revision,
                )
            await db.rollback()
        await db.refresh(state)
        assert state.revision == revision
        assert state.access_policy.get('bs_purchase_enabled', True) is True


async def test_draining_allows_ordinary_topup_and_survives_policy_refresh(postgres_database, monkeypatch):
    from app.database.models import Subscription, Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, _ = await prepared_purchase(db, monkeypatch)
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        await policy.set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=state.revision)
        await policy.configure_groups(
            db,
            **target,
            ordinary_mode='finite',
            bs_mode='finite',
            evidence=args['evidence'],
            expected_revision=state.revision,
        )
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.traffic_topup_enabled = True
        tariff.traffic_topup_packages = {'10': 2500}
        await db.commit()
        result = await purchase_package(db, **(args | {'group': 'ordinary'}))
        assert result.price_kopeks == 2500
        assert state.access_policy['bs_purchase_enabled'] is False


@pytest.mark.parametrize('explicit', [False, True])
async def test_tariff_disable_preserves_paid_delivery_and_blocks_new_sales(postgres_database, monkeypatch, explicit):
    from app.database.crud.tariff import update_tariff
    from app.database.models import Subscription, Tariff
    from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        args, reader, target, _ = await prepared_purchase(db, monkeypatch)
        paid = await purchase_package(db, **args)
        await db.commit()
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        original = dict(tariff.bs_traffic_settings)
        await update_tariff(
            db, tariff, bs_traffic_settings=(original | {'enabled': False, 'limit_gb': 20}) if explicit else None
        )
        assert {key: value for key, value in tariff.bs_traffic_settings.items() if key != 'disabled_at'} == original | {
            'enabled': False
        }
        before = (await totals(db))[:3]
        with pytest.raises(LedgerConflict):
            await purchase_package(db, **(args | {'operation_key': 'after-disable'}))
        assert (await totals(db))[:3] == before
        await db.rollback()
        assert await advance_purchase_delivery(reader, **target, operation_id=paid.delivery_id) == 'verified'
        assert (await totals(db))[:3] == before


async def test_reenabled_tariff_reopens_sales_without_reissuing_paid_grants(postgres_database, monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.crud.tariff import update_tariff
    from app.database.models import Subscription, Tariff
    from app.services.bs_traffic.drain_worker import process_drain_page

    async with postgres_session(postgres_database, TABLES) as db:
        args, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        original = dict(tariff.bs_traffic_settings)
        before = await totals(db)
        await update_tariff(db, tariff, bs_traffic_settings=None)
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        await process_drain_page(factory, panel_scope=reader.panel_scope)
        await db.refresh(await db.get(BsTrafficLedger, sub.id))
        assert (await db.get(BsTrafficLedger, sub.id)).access_policy['bs_purchase_enabled'] is False
        await db.commit()
        await update_tariff(db, tariff, bs_traffic_settings=dict(original, enabled=True))
        result = await process_drain_page(factory, panel_scope=reader.panel_scope)
        assert result.changed == 1
        await db.refresh(await db.get(BsTrafficLedger, sub.id))
        assert (await db.get(BsTrafficLedger, sub.id)).access_policy['bs_purchase_enabled'] is True
        assert await totals(db) == before
