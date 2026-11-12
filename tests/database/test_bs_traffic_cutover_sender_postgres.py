import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from app.database.models import (
    BsTrafficCollectionBudget,
    BsTrafficCutoverIntent,
    BsTrafficObservation,
    BsTrafficPoll,
    Subscription,
)
from app.services.bs_traffic import cutover_sender as sender
from tests.database.test_bs_traffic_cutover_intent_postgres import staged
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES as SOURCE_TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [*SOURCE_TABLES, BsTrafficCollectionBudget.__table__, BsTrafficObservation.__table__, BsTrafficPoll.__table__]


async def enroll_baseline(db, reader, keys):
    from app.services.bs_traffic.intake import attach_cursor
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_A

    _, day = await observe(db, reader, keys)
    await attach_cursor(db, **keys, group='ordinary', node_uuid=NODE_A, day=day)
    await db.commit()


async def setup(db, monkeypatch, *, baseline=True):
    reader, keys, plan, op = await staged(db, monkeypatch)
    if baseline:
        await enroll_baseline(db, reader, keys)
    reader._api.get_split_grace_snapshot.reset_mock()
    monkeypatch.setattr(sender, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))

    async def write(target):
        # Verify committed attempt is visible from a separate DB connection.
        async with reader._factory() as check:
            assert (await check.get(BsTrafficCutoverIntent, op)).state == 'attempted'
        reader._api.get_split_grace_snapshot.return_value = target

    reader._api.restore_split_user_once = AsyncMock(side_effect=write)
    return reader, keys, plan, op


async def state(reader, op):
    async with reader._factory() as check:
        row = await check.get(BsTrafficCutoverIntent, op)
        return row.state, row.not_sent_reason


async def test_successful_write_is_committed_once_and_read_back(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch)
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'verified'
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'verified'
        reader._api.restore_split_user_once.assert_awaited_once_with(plan.target)
        assert reader._api.get_split_grace_snapshot.await_count == 2
        assert sender.collect_topology_evidence.await_count == 1
        assert await state(reader, op) == ('verified', None)
        assert (await db.get(Subscription, 1)).traffic_accounting_mode == 'split_pending'


async def test_timeout_after_panel_applied_uses_readback_not_second_patch(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch)

        async def lost_response(target):
            reader._api.get_split_grace_snapshot.return_value = target
            raise TimeoutError('synthetic lost response')

        reader._api.restore_split_user_once.side_effect = lost_response
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'verified'
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'verified'
        assert reader._api.restore_split_user_once.await_count == 1


async def test_timeout_without_readback_match_remains_attempted(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch)
        reader._api.restore_split_user_once.side_effect = TimeoutError('synthetic timeout')
        for _ in range(2):
            assert await sender.send_cutover(reader, **keys, operation_id=op) == 'unconfirmed'
        assert reader._api.restore_split_user_once.await_count == 1
        assert await state(reader, op) == ('attempted', None)


async def test_concurrent_senders_make_one_patch(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch)
        results = await asyncio.gather(*(sender.send_cutover(reader, **keys, operation_id=op) for _ in range(2)))
        assert all(result in ('verified', 'unconfirmed') for result in results)
        assert reader._api.restore_split_user_once.await_count == 1
        assert await state(reader, op) == ('verified', None)


async def test_changed_source_is_explicitly_unsent_with_attempt_history(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch)
        reader._api.get_split_grace_snapshot.return_value = replace(
            reader._api.get_split_grace_snapshot.return_value, hwid_device_limit=77
        )
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'source_changed'
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'not_sent'
        reader._api.restore_split_user_once.assert_not_called()
        assert await state(reader, op) == ('not_sent', 'source_changed')
        async with reader._factory() as check:
            row = await check.get(BsTrafficCutoverIntent, op)
            assert row.attempt_token is not None and row.attempted_at is not None


async def test_cancellation_after_claim_never_releases_attempt_for_replay(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch)
        reader._api.restore_split_user_once.side_effect = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await sender.send_cutover(reader, **keys, operation_id=op)
        assert await state(reader, op) == ('attempted', None)
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'unconfirmed'
        assert reader._api.restore_split_user_once.await_count == 1


async def test_already_matching_target_needs_no_patch(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch)
        reader._api.get_split_grace_snapshot.return_value = plan.target
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'verified'
        reader._api.restore_split_user_once.assert_not_called()


async def test_changed_topology_is_not_sent(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch)
        sender.collect_topology_evidence.return_value = replace(plan.evidence, fingerprint='b' * 64)
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'policy_changed'
        reader._api.restore_split_user_once.assert_not_called()
        assert await state(reader, op) == ('not_sent', 'policy_changed')


async def test_provider_pause_does_not_consume_attempt(postgres_database, monkeypatch):
    from app.services.bs_traffic.panel_pause import persist_panel_pause

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch)
        await persist_panel_pause(reader._factory, reader.panel_scope, 60)
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'deferred'
        assert await state(reader, op) == ('pending', None)
        reader._api.restore_split_user_once.assert_not_called()
        sender.collect_topology_evidence.assert_not_called()


@pytest.mark.parametrize('exists', [True, False])
async def test_external_squad_requires_catalog_confirmation(postgres_database, monkeypatch, exists):
    from app.services.bs_traffic.cutover_intent import stage_cutover
    from app.services.bs_traffic.managed_cutover import prepare_managed_cutover
    from tests.database.test_bs_traffic_managed_cutover_postgres import setup as prepare

    async with postgres_session(postgres_database, TABLES) as db:
        reader, api, keys = await prepare(db, monkeypatch)
        external = '77777777-7777-4777-8777-777777777777'
        api.get_split_grace_snapshot.return_value = replace(
            api.get_split_grace_snapshot.return_value, external_squad_uuid=external
        )
        plan = await prepare_managed_cutover(reader, **keys)
        op = await stage_cutover(db, reader, **keys, operation_key='external-cutover', plan=plan)
        await db.commit()
        await enroll_baseline(db, reader, keys)
        monkeypatch.setattr(sender, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
        api.split_external_squad_exists = AsyncMock(return_value=exists)

        async def write(target):
            assert target.external_squad_uuid == external
            api.get_split_grace_snapshot.return_value = target

        api.restore_split_user_once = AsyncMock(side_effect=write)
        result = await sender.send_cutover(reader, **keys, operation_id=op)
        assert result == ('verified' if exists else 'external_missing')
        assert api.restore_split_user_once.await_count == int(exists)
        api.split_external_squad_exists.assert_awaited_once_with(external)


async def test_missing_baseline_prevents_panel_write_and_claim(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await setup(db, monkeypatch, baseline=False)
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'accounting_pending'
        reader._api.restore_split_user_once.assert_not_called()
        sender.collect_topology_evidence.assert_not_called()
        assert await state(reader, op) == ('pending', None)


@pytest.mark.parametrize('moment', ['topology', 'source'])
async def test_new_raw_data_during_preflight_never_sends(postgres_database, monkeypatch, moment):
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await setup(db, monkeypatch)
        source = reader._api.get_split_grace_snapshot.return_value

        async def collect(*args, **kwargs):
            async with reader._factory() as other, other.begin():
                await observe(other, reader, keys, amount=150)
            return plan.evidence if moment == 'topology' else source

        if moment == 'topology':
            sender.collect_topology_evidence.side_effect = collect
        else:
            reader._api.get_split_grace_snapshot.side_effect = collect
        assert await sender.send_cutover(reader, **keys, operation_id=op) == 'accounting_pending'
        reader._api.restore_split_user_once.assert_not_called()
        assert await state(reader, op) == (
            ('pending', None) if moment == 'topology' else ('not_sent', 'accounting_pending')
        )
