from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficAllowance, BsTrafficManagedTransfer, Subscription
from app.external.remnawave_api import SplitGraceSnapshot
from app.services.bs_traffic import managed_cutover as module
from app.services.bs_traffic.ledger import LedgerConflict, open_usage
from app.services.bs_traffic.managed_transfer_capture import capture_managed_transfer
from app.services.bs_traffic.managed_transfer_commit import commit_managed_transfer
from app.services.bs_traffic.policy import configure_groups
from tests.database.test_bs_traffic_managed_capture_postgres import prepared
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY, evidence
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def setup(db, monkeypatch, *, ordinary='finite', bs='unlimited'):
    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    reader, api, keys = await prepared(db)
    topology = evidence()
    target = {k: v for k, v in keys.items() if k != 'destination_group'}
    await configure_groups(db, **target, ordinary_mode=ordinary, bs_mode=bs, evidence=topology, expected_revision=0)
    sub = await db.get(Subscription, 1)
    sub.is_trial = False
    sub.status = 'active'
    expiry = sub.end_date
    await db.commit()
    observed = await capture_managed_transfer(reader, **keys)
    await commit_managed_transfer(db, **target, operation_key='synthetic-cutover', observation=observed)
    await db.commit()
    original = api.get_split_transfer_snapshot.return_value
    api.get_split_grace_snapshot = AsyncMock(
        return_value=SplitGraceSnapshot(
            original.id,
            original.created_at,
            [{'uuid': ORDINARY}],
            'ACTIVE',
            expiry,
            1000,
            'NO_RESET',
            250,
            2,
            None,
            original.created_at,
        )
    )
    monkeypatch.setattr(module, 'collect_topology_evidence', AsyncMock(return_value=topology))
    return reader, api, target


async def test_preparation_preserves_paid_fields_and_never_activates_or_recredits(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        plan = await module.prepare_managed_cutover(reader, **keys)
        assert plan.target.traffic_limit_bytes == 0 and plan.target.status == 'ACTIVE'
        assert plan.target.active_internal_squads == [{'uuid': s} for s in sorted((ORDINARY, BS))]
        source = api.get_split_grace_snapshot.return_value
        assert (
            replace(plan.target, traffic_limit_bytes=1000, active_internal_squads=source.active_internal_squads)
            == source
        )
        again = await module.prepare_managed_cutover(reader, **keys)
        assert again.guard_hash == plan.guard_hash
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'
        assert await db.scalar(select(func.count()).select_from(BsTrafficManagedTransfer)) == 1
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800


@pytest.mark.parametrize(
    'change',
    [
        {'traffic_limit_bytes': 1001},
        {'used_traffic_bytes': 0},
        {'status': 'DISABLED'},
        {'traffic_limit_strategy': 'MONTH'},
        {'id': 99},
        {'active_internal_squads': [{'uuid': '99999999-9999-4999-8999-999999999999'}]},
    ],
)
async def test_changed_panel_source_is_not_overwritten(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        api.get_split_grace_snapshot.return_value = replace(api.get_split_grace_snapshot.return_value, **change)
        with pytest.raises(LedgerConflict):
            await module.prepare_managed_cutover(reader, **keys)


@pytest.mark.parametrize('field,value', [('is_trial', True), ('grace_session_open', True), ('status', 'disabled')])
async def test_ineligible_local_subscription_stops_before_http(postgres_database, monkeypatch, field, value):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        setattr(await db.get(Subscription, 1), field, value)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await module.prepare_managed_cutover(reader, **keys)
        api.get_split_grace_snapshot.assert_not_called()


@pytest.mark.parametrize('change', ['devices', 'tariff'])
async def test_http_holds_no_business_lock_and_concurrent_rights_change_invalidates_plan(
    postgres_database, monkeypatch, change
):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        source = api.get_split_grace_snapshot.return_value

        async def read(_):
            async with reader._factory() as other, other.begin():
                sub = await other.scalar(select(Subscription).where(Subscription.id == 1).with_for_update(nowait=True))
                if change == 'devices':
                    sub.device_limit += 1
                else:
                    from app.database.models import Tariff

                    (
                        await other.get(Tariff, sub.tariff_id)
                    ).external_squad_uuid = '77777777-7777-4777-8777-777777777777'
            return source

        api.get_split_grace_snapshot.side_effect = read
        with pytest.raises(LedgerConflict, match='Local rights changed'):
            await module.prepare_managed_cutover(reader, **keys)


async def test_unresolved_raw_usage_blocks_preparation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        await open_usage(
            db,
            **keys,
            group='ordinary',
            source_key='late-before-cutover',
            amount_bytes=10,
            interval_start=now - timedelta(seconds=10),
            interval_end=now,
            evidence_kind='raw_pending',
        )
        await db.commit()
        with pytest.raises(LedgerConflict, match='Unresolved usage'):
            await module.prepare_managed_cutover(reader, **keys)
        api.get_split_grace_snapshot.assert_not_called()


async def test_exhausted_bs_keeps_ordinary_access(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch, bs='finite')
        plan = await module.prepare_managed_cutover(reader, **keys)
        assert plan.target.active_internal_squads == [{'uuid': ORDINARY}]


async def test_external_squad_devices_and_reset_marker_are_preserved(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        source = replace(
            api.get_split_grace_snapshot.return_value,
            external_squad_uuid='77777777-7777-4777-8777-777777777777',
            hwid_device_limit=0,
        )
        api.get_split_grace_snapshot.return_value = source
        plan = await module.prepare_managed_cutover(reader, **keys)
        assert plan.target.external_squad_uuid == source.external_squad_uuid
        assert plan.target.hwid_device_limit == 0
        assert plan.target.last_traffic_reset_at == source.last_traffic_reset_at
        assert plan.target.used_traffic_bytes == source.used_traffic_bytes


@pytest.mark.parametrize('change', ['topology', 'deadline', 'binding', 'opt_out'])
async def test_obsolete_evidence_cannot_prepare_cutover(postgres_database, monkeypatch, change):
    from datetime import UTC, datetime

    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        collected = module.collect_topology_evidence.return_value
        if change == 'topology':
            module.collect_topology_evidence.return_value = replace(collected, fingerprint='b' * 64)
        elif change == 'deadline':
            module.collect_topology_evidence.return_value = replace(collected, valid_until=datetime.now(UTC))
        elif change == 'binding':
            await retire_binding(db, panel_scope=reader.panel_scope)
            await db.commit()
        else:
            monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
        with pytest.raises(LedgerConflict):
            await module.prepare_managed_cutover(reader, **keys)
        if change in ('binding', 'opt_out'):
            api.get_split_grace_snapshot.assert_not_called()


async def test_raw_usage_arriving_during_panel_read_invalidates_cutover(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await setup(db, monkeypatch)
        source = api.get_split_grace_snapshot.return_value

        async def read(_):
            async with reader._factory() as other, other.begin():
                now = await other.scalar(select(func.clock_timestamp()))
                await open_usage(
                    other,
                    **keys,
                    group='ordinary',
                    source_key='late-during-read',
                    amount_bytes=50,
                    interval_start=now - timedelta(seconds=10),
                    interval_end=now,
                    evidence_kind='raw_pending',
                )
            return source

        api.get_split_grace_snapshot.side_effect = read
        with pytest.raises(LedgerConflict, match='Unresolved usage'):
            await module.prepare_managed_cutover(reader, **keys)
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800
