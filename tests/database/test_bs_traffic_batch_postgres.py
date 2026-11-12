"""Separate owners and durable operations under shared topology preparation."""

import asyncio

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficDelivery, Subscription, User
from app.services.bs_traffic.batch import PreparationRequest, prepare_batch
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.ledger import initialize
from app.services.bs_traffic.policy import configure_groups
from tests.database.test_bs_traffic_ledger_postgres import SCOPE
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.database.test_bs_traffic_prepare_postgres import client, setup
from tests.database.test_bs_traffic_sender_postgres import panel
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def setup_two(db, api):
    first_target = await setup(db, api)
    tariff_id = await db.scalar(select(Subscription.tariff_id).where(Subscription.id == 1))
    await db.execute(update(Subscription).where(Subscription.id == 2).values(user_id=2, tariff_id=tariff_id))
    second_target = dict(
        user_id=2,
        subscription_id=2,
        account_generation=account_generation(panel_scope=SCOPE, account=panel(id=13)),
    )
    await initialize(db, **second_target, panel_scope=SCOPE, panel_user_id=13)
    evidence = await collect_topology_evidence(api, panel_identity=SCOPE, ordinary_squads=(ORDINARY,), bs_squads=(BS,))
    await configure_groups(
        db, **second_target, ordinary_mode='unlimited', bs_mode='finite', evidence=evidence, expected_revision=0
    )
    await db.commit()
    return first_target, second_target


async def test_two_owners_shared_topology_conflict_isolation_and_replay(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        first_target, second_target = await setup_two(db, api)
        api.get_system_configuration.reset_mock()
        api.get_internal_squad_topology.reset_mock()
        api.get_split_access_snapshot.side_effect = lambda panel_id: panel(id=panel_id)
        first = PreparationRequest(**first_target, operation_key='first-owner')
        second = PreparationRequest(**second_target, operation_key='second-owner')
        wrong_owner = PreparationRequest(1, 2, second_target['account_generation'], 'forbidden')
        async with prepare_batch(
            async_sessionmaker(db.bind), api, [first, second, wrong_owner, first], panel_scope=SCOPE, concurrency=3
        ) as results:
            outcomes = [item async for item in results]
        assert len(outcomes) == 4
        assert next(item for item in outcomes if item.request == wrong_owner).status == 'conflict'
        valid = [item for item in outcomes if item.status == 'prepared']
        assert len(valid) == 3 and all(item.prepared.status == 'queued' for item in valid)
        first_results = [item.prepared.operation_id for item in valid if item.request == first]
        assert len(set(first_results)) == 1
        rows = (await db.scalars(select(BsTrafficDelivery))).all()
        assert {row.subscription_id for row in rows} == {1, 2} and len(rows) == 2
        assert api.get_system_configuration.await_count == 1
        assert api.get_internal_squad_topology.await_count == 2
        assert sorted(call.args[0] for call in api.get_split_access_snapshot.await_args_list) == [12, 12, 13]


async def test_early_exit_keeps_committed_operation_and_replay_recovers_remaining(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        first_target, second_target = await setup_two(db, api)
        entered, cancelled = asyncio.Event(), asyncio.Event()

        async def snapshot(panel_id):
            if panel_id == 13:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            await entered.wait()
            return panel(id=panel_id)

        api.get_split_access_snapshot.side_effect = snapshot
        first = PreparationRequest(**first_target, operation_key='first-owner')
        second = PreparationRequest(**second_target, operation_key='second-owner')
        factory = async_sessionmaker(db.bind)
        async with prepare_batch(factory, api, [first, second], panel_scope=SCOPE, concurrency=2) as results:
            committed = await asyncio.wait_for(anext(results), timeout=10)
            assert committed.request == first and committed.prepared.status == 'queued'
        assert cancelled.is_set()
        rows = (await db.scalars(select(BsTrafficDelivery))).all()
        assert len(rows) == 1 and rows[0].id == committed.prepared.operation_id
        await db.rollback()
        api.get_split_access_snapshot.side_effect = lambda panel_id: panel(id=panel_id)
        async with prepare_batch(factory, api, [first, second], panel_scope=SCOPE) as results:
            recovered = [item async for item in results]
        assert all(item.prepared.status == 'queued' for item in recovered)
        assert (
            next(item for item in recovered if item.request == first).prepared.operation_id
            == committed.prepared.operation_id
        )
        assert len((await db.scalars(select(BsTrafficDelivery))).all()) == 2


async def test_sql_lock_wait_times_out_without_operation_and_can_retry(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        request = PreparationRequest(**target, operation_key='locked-prepare')
        factory = async_sessionmaker(db.bind)
        async with factory() as blocker, blocker.begin():
            await blocker.execute(select(User).where(User.id == 1).with_for_update())
            async with prepare_batch(factory, api, [request], panel_scope=SCOPE, timeout_seconds=0.2) as results:
                outcomes = [item async for item in results]
            assert len(outcomes) == 1 and outcomes[0].status == 'timeout'
            assert not (await db.scalars(select(BsTrafficDelivery))).all()
            api.get_split_access_snapshot.assert_not_awaited()
            await db.rollback()
        async with prepare_batch(factory, api, [request], panel_scope=SCOPE) as results:
            recovered = [item async for item in results]
        assert recovered[0].prepared.status == 'queued'
        assert len((await db.scalars(select(BsTrafficDelivery))).all()) == 1
