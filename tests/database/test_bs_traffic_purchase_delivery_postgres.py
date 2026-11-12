"""Paid delivery resumes from persisted receipts without money or grant replay."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.database.models import BsTrafficDelivery, Transaction
from app.services.bs_traffic.purchase import purchase_package
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from tests.database.test_bs_traffic_prepare_postgres import client
from tests.database.test_bs_traffic_purchase_postgres import TABLES, setup, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepared_purchase(db, monkeypatch):
    args, reader = await setup(db, monkeypatch, with_reader=True)
    # Use real topology parser with the existing synthetic panel contract.
    catalog = client()
    reader._api.get_system_configuration = catalog.get_system_configuration
    reader._api.get_internal_squad_topology = catalog.get_internal_squad_topology
    from app.database.models import BsTrafficLedger
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from app.services.bs_traffic.policy import configure_groups
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    target = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
    evidence = await collect_topology_evidence(
        reader, panel_identity=reader.panel_scope, ordinary_squads=(ORDINARY,), bs_squads=(BS,)
    )
    state = await db.get(BsTrafficLedger, 1)
    await configure_groups(
        db, **target, ordinary_mode='finite', bs_mode='finite', evidence=evidence, expected_revision=state.revision
    )
    args['evidence'] = evidence
    await db.commit()
    original = reader._api.get_split_grace_snapshot.return_value
    account = SimpleNamespace(
        id=original.id, created_at=original.created_at, active_internal_squads=[{'uuid': ORDINARY}]
    )
    reader._api.get_split_access_snapshot = AsyncMock(side_effect=lambda _: account)

    async def write(user_id, squads):
        account.active_internal_squads = [{'uuid': item} for item in squads]
        return account

    reader._api.update_internal_squads_once = AsyncMock(side_effect=write)
    return args, reader, target, account


async def purchased(db, monkeypatch):
    args, reader, target, account = await prepared_purchase(db, monkeypatch)
    result = await purchase_package(db, **args)
    await db.commit()
    return reader, target, result, account


async def test_restart_replans_expired_unsent_receipt_without_recharging(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)
        root = await db.get(BsTrafficDelivery, purchase.delivery_id)
        root.valid_until = datetime.now(UTC) - timedelta(hours=1)
        await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=root.id) == 'verified'
        assert await advance_purchase_delivery(reader, **target, operation_id=root.id) == 'already_verified'
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert (await totals(db))[:3] == (7500, 1, 1)
        await db.refresh(root)
        assert root.state == 'cancelled' and root.token is None
        assert len(account.active_internal_squads) == 2


async def test_missing_payment_receipt_never_touches_panel(postgres_database, monkeypatch):
    from sqlalchemy import delete

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        await db.execute(delete(Transaction))
        await db.commit()
        reader._api.get_split_access_snapshot.reset_mock()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'obsolete'
        reader._api.get_split_access_snapshot.assert_not_awaited()
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_unknown_write_outcome_never_gets_a_second_patch(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        reader._api.update_internal_squads_once.side_effect = TimeoutError('unknown outcome')
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) in (
            'attention',
            'unconfirmed',
        )
        for _ in range(2):
            await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id)
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert (await totals(db))[:3] == (7500, 1, 1)


async def test_bounded_scan_skips_ancestors_and_verified_purchase(postgres_database, monkeypatch):
    from app.services.bs_traffic.purchase_delivery_worker import purchase_delivery_page

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        page = await purchase_delivery_page(db, panel_scope=reader.panel_scope, after_id=0, limit=1)
        assert len(page.items) == 1 and page.items[0].operation_id == purchase.delivery_id
        assert page.scanned == 1
        await db.rollback()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        page = await purchase_delivery_page(db, panel_scope=reader.panel_scope, after_id=0, limit=1)
        assert not page.items and page.scanned == 1
        # The original cancelled row has a successor; neither gets resubmitted.
        page = await purchase_delivery_page(db, panel_scope=reader.panel_scope, after_id=page.next_after, limit=1)
        assert not page.items


async def test_worker_can_complete_then_stop_without_another_payment(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.purchase_delivery_worker import run_purchase_delivery_worker

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await purchased(db, monkeypatch)
        stop = asyncio.Event()
        original = reader._api.update_internal_squads_once.side_effect

        async def write(user_id, squads):
            result = await original(user_id, squads)
            stop.set()
            return result

        reader._api.update_internal_squads_once.side_effect = write
        await asyncio.wait_for(run_purchase_delivery_worker(reader, stop, page_size=1), 10)
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert (await totals(db))[:3] == (7500, 1, 1)


async def test_parallel_recovery_sends_once(postgres_database, monkeypatch):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        await asyncio.gather(
            *(advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) for _ in range(2))
        )
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) in (
            'verified',
            'already_verified',
        )
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert (await totals(db))[:3] == (7500, 1, 1)


@pytest.mark.parametrize('change', ['owner', 'binding'])
async def test_recovery_rechecks_owner_and_panel_binding(postgres_database, monkeypatch, change):
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        if change == 'owner':
            target['user_id'] = 2
        else:
            await retire_binding(db, panel_scope=reader.panel_scope)
            await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'obsolete'
        reader._api.get_split_access_snapshot.assert_not_awaited()
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_expired_paid_allowance_cannot_be_confirmed_by_matching_old_squads(postgres_database, monkeypatch):
    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.delivery_store import claim
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)
        row = await db.get(BsTrafficDelivery, purchase.delivery_id)
        await claim(db, **target, operation_id=row.id, current_guard_hash=row.guard_hash)
        now = datetime.now(UTC)
        row.attempted_at = now - timedelta(minutes=2)
        row.valid_until = now - timedelta(minutes=1)
        grant = await db.get(BsTrafficAllowance, purchase.grant_id)
        grant.starts_at, grant.expires_at = now - timedelta(days=31), now - timedelta(seconds=1)
        account.active_internal_squads = [{'uuid': ORDINARY}, {'uuid': BS}]
        await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'attention'
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_pending_other_group_does_not_strand_confirmed_purchase(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import open_usage

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)
        now = datetime.now(UTC)
        await open_usage(
            db,
            **target,
            group='ordinary',
            source_key='ordinary-delay',
            amount_bytes=10,
            interval_start=now - timedelta(seconds=1),
            interval_end=now,
            evidence_kind='raw_pending',
        )
        await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        assert (
            await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'already_verified'
        )
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert len(account.active_internal_squads) == 2


async def test_operator_price_mode_edit_preserves_already_paid_delivery(postgres_database, monkeypatch):
    from app.database.models import Subscription, Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        (await db.get(Tariff, sub.tariff_id)).traffic_limit_gb = 0
        await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert (await totals(db))[:3] == (7500, 1, 1)


async def test_tariff_change_during_panel_read_blocks_stale_delivery(postgres_database, monkeypatch):
    from sqlalchemy import select, update

    from app.database.models import Subscription, Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)

        async def read(panel_id):
            async with reader._factory() as other, other.begin():
                tariff_id = await other.scalar(
                    select(Subscription.tariff_id).where(Subscription.id == target['subscription_id'])
                )
                await other.execute(update(Tariff).where(Tariff.id == tariff_id).values(traffic_limit_gb=0))
            return account

        reader._api.get_split_access_snapshot.side_effect = read
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) in (
            'stale',
            'obsolete',
            'not_sent',
        )
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_tariff_change_between_receipt_and_preparation_is_rejected(postgres_database, monkeypatch):
    from sqlalchemy import update

    from app.database.models import Subscription
    from app.services.bs_traffic import purchase_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        prepare = purchase_delivery.prepare_delivery

        async def changed_prepare(*args, **kwargs):
            async with reader._factory() as other, other.begin():
                await other.execute(
                    update(Subscription).where(Subscription.id == target['subscription_id']).values(tariff_id=None)
                )
            return await prepare(*args, **kwargs)

        monkeypatch.setattr(purchase_delivery, 'prepare_delivery', changed_prepare)
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'obsolete'
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_later_price_change_does_not_block_paid_delivery(postgres_database, monkeypatch):
    from app.database.models import Subscription, Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.bs_traffic_settings = {**tariff.bs_traffic_settings, 'topup_packages': {'10': 9000}}
        await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert (await totals(db))[:3] == (7500, 1, 1)


async def test_operator_squad_edit_does_not_redirect_paid_package(postgres_database, monkeypatch):
    from app.database.models import Subscription, Tariff
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.bs_traffic_settings = {
            **tariff.bs_traffic_settings,
            'squad_uuids': ['00000000-0000-4000-8000-000000000099'],
        }
        await db.commit()
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        assert {item['uuid'] for item in account.active_internal_squads} == {BS, ORDINARY}
        assert (await totals(db))[:3] == (7500, 1, 1)
