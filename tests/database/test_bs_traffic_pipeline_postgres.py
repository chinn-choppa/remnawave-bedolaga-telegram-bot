"""Exercise paid purchase → queued collection → accounting → panel delivery."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficAllowance, BsTrafficUsage
from app.services.bs_traffic.active_collection import enqueue_active_collection, start_active_collection
from app.services.bs_traffic.intake import attach_cursor, settle_reported_observation
from app.services.bs_traffic.preparation import prepare_delivery
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from app.services.bs_traffic.queue import run_once
from app.services.bs_traffic.sender import send_delivery
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import NODE_B, ORDINARY
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('restart_after_settlement', [False, True])
@pytest.mark.parametrize('lost_patch_response', [False, True])
async def test_bs_exhaustion_keeps_ordinary_access_and_never_recharges(
    postgres_database,
    monkeypatch,
    restart_after_settlement,
    lost_patch_response,
):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, account = await purchased(db, monkeypatch)
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        # Explicit initial enrollment exists before the next collection round.
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        paid = await db.get(BsTrafficAllowance, purchase.grant_id)
        amount = paid.remaining_bytes
        before = await totals(db)
        ordinary_before = (
            await db.scalars(
                select(BsTrafficAllowance.remaining_bytes).where(
                    BsTrafficAllowance.traffic_group == 'ordinary',
                )
            )
        ).all()
        window = await start_active_collection(db, panel_scope=reader.panel_scope)
        page = await enqueue_active_collection(db, reader, window, round_key='exhaustion')
        await db.commit()

        async def fetch(nodes, requested_day, **kwargs):
            assert requested_day == day
            return {
                'nodes': [
                    {'uuid': node, 'users': [{'id': 12, 'totalBytes': 100 + amount + 50 if node == NODE_B else 100}]}
                    for node in nodes
                ]
            }

        reader._api.get_daily_nodes_usage = AsyncMock(side_effect=fetch)
        for _ in page.job_ids:
            assert await run_once(reader._factory, reader, reader.panel_scope)
        assert not await run_once(reader._factory, reader, reader.panel_scope)
        prepared = await prepare_delivery(
            reader._factory, reader, **target, operation_key='exhaustion-delivery', settle_usage=True
        )
        assert prepared.status == 'queued'
        async with reader._factory() as intake:
            usage_id = await intake.scalar(select(BsTrafficUsage.id).where(BsTrafficUsage.traffic_group == 'bs'))
        if restart_after_settlement:
            async with reader._factory() as intake, intake.begin():
                assert await settle_reported_observation(intake, **target, node_uuid=NODE_B, day=day) is None
        reader._api.update_internal_squads_once.reset_mock()

        async def write(panel_id, squads):
            account.active_internal_squads = [{'uuid': squad} for squad in squads]
            if lost_patch_response:
                raise TimeoutError('response lost after application')
            return account

        reader._api.update_internal_squads_once.side_effect = write
        delivery_args = dict(
            **target,
            operation_id=prepared.operation_id,
            evidence=prepared.evidence,
            current_guard_hash=prepared.guard_hash,
            bound_reader=reader,
        )
        await send_delivery(reader._factory, reader._api, **delivery_args)
        # Replay recovers/reads back the same durable attempt; never sends twice.
        outcome = await send_delivery(reader._factory, reader._api, **delivery_args)
        assert outcome in ('verified', 'already_verified')
        reader._api.update_internal_squads_once.assert_awaited_once()
        assert account.active_internal_squads == [{'uuid': ORDINARY}]
        db.expire_all()
        usage = await db.get(BsTrafficUsage, usage_id)
        assert (usage.charged_bytes, usage.unallocated_bytes) == (amount, 50)
        assert (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes == 0
        assert (await totals(db))[:3] == before[:3]
        assert (
            await db.scalars(
                select(BsTrafficAllowance.remaining_bytes).where(
                    BsTrafficAllowance.traffic_group == 'ordinary',
                )
            )
        ).all() == ordinary_before


async def test_delivery_queue_failure_rolls_back_accounting(postgres_database, monkeypatch):
    from app.services.bs_traffic import preparation

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        before = (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes
        await observe(db, reader, target, NODE_B, 150)
        await db.commit()
        monkeypatch.setattr(preparation, 'enqueue', AsyncMock(side_effect=RuntimeError('queue unavailable')))
        with pytest.raises(RuntimeError, match='queue unavailable'):
            await prepare_delivery(reader._factory, reader, **target, operation_key='broken-queue', settle_usage=True)
        db.expire_all()
        assert (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes == before
        assert not (await db.scalars(select(BsTrafficUsage))).all()
        # Recovery consumes the same delta once after the failed transaction.
        async with reader._factory() as intake, intake.begin():
            usage_id = await settle_reported_observation(intake, **target, node_uuid=NODE_B, day=day)
        db.expire_all()
        assert (await db.get(BsTrafficUsage, usage_id)).charged_bytes == 50


@pytest.mark.parametrize('changed', ['topology', 'scope'])
async def test_changed_evidence_cannot_commit_usage(postgres_database, monkeypatch, changed):
    from dataclasses import replace

    from app.services.bs_traffic import preparation
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.panel_binding import bind_reader, register_binding
    from tests.database.test_bs_traffic_policy_postgres import BS

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, purchase, _ = await purchased(db, monkeypatch)
        assert await advance_purchase_delivery(reader, **target, operation_id=purchase.delivery_id) == 'verified'
        _, day = await observe(db, reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        before = (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes
        await observe(db, reader, target, NODE_B, 150)
        await db.commit()
        if changed == 'topology':
            evidence = await collect_topology_evidence(
                reader, panel_identity=reader.panel_scope, ordinary_squads=(ORDINARY,), bs_squads=(BS,)
            )
            monkeypatch.setattr(
                preparation,
                'collect_topology_evidence',
                AsyncMock(return_value=replace(evidence, fingerprint='c' * 64)),
            )
            result = await prepare_delivery(
                reader._factory, reader, **target, operation_key='changed', settle_usage=True
            )
            assert result.status == 'blocked'
        else:
            from copy import copy
            from uuid import uuid4

            scope = str(uuid4())
            other_api = copy(reader._api)
            other_api.base_url = 'https://other-panel.example'
            await register_binding(db, panel_scope=scope, endpoint=other_api.base_url)
            await db.commit()
            other = await bind_reader(reader._factory, other_api, panel_scope=scope)
            with pytest.raises(LedgerConflict, match='scope'):
                await prepare_delivery(reader._factory, other, **target, operation_key='changed', settle_usage=True)
        db.expire_all()
        assert (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes == before
        assert not (await db.scalars(select(BsTrafficUsage))).all()
