"""Prepare a delivery from coherent live evidence without holding SQL during GET."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficDelivery, BsTrafficLedger, Subscription, User
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.policy import configure_groups
from app.services.bs_traffic.preparation import prepare_delivery
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, TARGET, issue
from tests.database.test_bs_traffic_policy_postgres import BS, NODE_A, NODE_B, ORDINARY, seed
from tests.database.test_bs_traffic_sender_postgres import panel
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def client():
    async def topology(squad):
        return {
            'squadUuid': squad,
            'accessibleNodes': [
                {
                    'uuid': NODE_A if squad == ORDINARY else NODE_B,
                    'configProfileUuid': ORDINARY,
                    'activeInbounds': ['synthetic'],
                }
            ],
        }

    return SimpleNamespace(
        get_system_configuration=AsyncMock(
            return_value={
                'service': {'disableUserUsageRecords': False, 'cleanUsageHistory': False},
                'misc': {'userUsageIgnoreBelowBytes': 0},
            }
        ),
        get_internal_squad_topology=AsyncMock(side_effect=topology),
        get_split_access_snapshot=AsyncMock(return_value=panel()),
    )


async def setup(db, api):
    await seed(db)
    generation = account_generation(panel_scope=SCOPE, account=panel())
    await db.execute(
        update(BsTrafficLedger).where(BsTrafficLedger.subscription_id == 1).values(account_generation=generation)
    )
    target = dict(TARGET, account_generation=generation)
    evidence = await collect_topology_evidence(api, panel_identity=SCOPE, ordinary_squads=(ORDINARY,), bs_squads=(BS,))
    await configure_groups(
        db, **target, ordinary_mode='unlimited', bs_mode='finite', evidence=evidence, expected_revision=0
    )
    await db.commit()
    return target


async def prepare(db, api, target):
    return await prepare_delivery(
        async_sessionmaker(db.bind, expire_on_commit=True), api, **target, operation_key='synthetic-prepare'
    )


async def test_live_prepare_computes_guard_and_only_stages_internal_squads(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        result = await prepare(db, api, target)
        assert result.status == 'queued'
        row = await db.get(BsTrafficDelivery, result.operation_id)
        assert row.squads == [ORDINARY] and row.state == 'pending'
        assert row.guard_hash == result.guard_hash and len(result.guard_hash) == 64
        await db.rollback()
        replay = await prepare(db, api, target)
        assert replay.operation_id == result.operation_id and replay.guard_hash == result.guard_hash


async def test_topology_change_requires_reconfiguration_without_queuing(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        original = api.get_internal_squad_topology.side_effect

        async def changed(squad):
            value = await original(squad)
            value['accessibleNodes'][0]['activeInbounds'] = ['changed']
            return value

        api.get_internal_squad_topology.side_effect = changed
        assert (await prepare(db, api, target)).status == 'blocked'
        assert not (await db.scalars(select(BsTrafficDelivery))).all()


@pytest.mark.parametrize('change', ['purchase', 'disabled'])
async def test_local_change_during_http_aborts_staging_without_sql_lock(change, postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)

        async def read(panel_id):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                if change == 'purchase':
                    await issue(other, account_generation=target['account_generation'])
                else:
                    await other.execute(update(Subscription).where(Subscription.id == 1).values(status='disabled'))
            return panel()

        api.get_split_access_snapshot.side_effect = read
        assert (await prepare(db, api, target)).status == 'stale'
        assert not (await db.scalars(select(BsTrafficDelivery))).all()


@pytest.mark.parametrize('account', [None, panel(id=13)])
async def test_wrong_account_never_queues(postgres_database, account):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        api.get_split_access_snapshot.return_value = account
        assert (await prepare(db, api, target)).status == 'blocked'
        assert not (await db.scalars(select(BsTrafficDelivery))).all()


async def test_provider_failure_leaves_queue_empty(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        api.get_system_configuration.side_effect = TimeoutError
        assert (await prepare(db, api, target)).status == 'unavailable'
        assert not (await db.scalars(select(BsTrafficDelivery))).all()


async def test_prepared_operation_can_be_sent_and_verified(postgres_database):
    from app.services.bs_traffic.sender import send_delivery

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        result = await prepare(db, api, target)

        async def write(user_id, squads):
            value = panel(tuple(squads))
            api.get_split_access_snapshot.return_value = value
            return value

        api.update_internal_squads_once = AsyncMock(side_effect=write)
        outcome = await send_delivery(
            async_sessionmaker(db.bind, expire_on_commit=True),
            api,
            **target,
            operation_id=result.operation_id,
            evidence=result.evidence,
            current_guard_hash=result.guard_hash,
        )
        assert outcome == 'verified'
        api.update_internal_squads_once.assert_awaited_once_with(12, (ORDINARY,))


async def test_grace_marker_prevents_queuing_paid_access(postgres_database):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        await db.execute(update(Subscription).where(Subscription.id == 1).values(grace_session_open=True))
        await db.commit()
        assert (await prepare(db, api, target)).status == 'blocked'
        assert not (await db.scalars(select(BsTrafficDelivery))).all()


async def test_expired_trial_is_not_enrolled_by_preparation(postgres_database):
    from datetime import UTC, datetime, timedelta

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        await db.execute(
            update(Subscription).where(Subscription.id == 1).values(end_date=datetime.now(UTC) - timedelta(seconds=1))
        )
        await db.commit()
        assert (await prepare(db, api, target)).status == 'blocked'


async def test_wrong_owner_fails_before_external_reads(postgres_database):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        api.get_system_configuration.reset_mock()
        with pytest.raises(LedgerConflict):
            await prepare(db, api, dict(target, user_id=2))
        api.get_system_configuration.assert_not_awaited()


async def test_concurrent_preparation_reuses_one_operation(postgres_database):
    import asyncio

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        results = await asyncio.wait_for(asyncio.gather(prepare(db, api, target), prepare(db, api, target)), 10)
        assert results[0].operation_id == results[1].operation_id
        assert len((await db.scalars(select(BsTrafficDelivery))).all()) == 1


@pytest.mark.parametrize('change', ['owner', 'generation', 'grace'])
async def test_identity_and_grace_changes_during_http_cannot_queue(postgres_database, change):
    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)

        async def read(panel_id):
            async with async_sessionmaker(db.bind)() as other, other.begin():
                if change == 'generation':
                    await other.execute(
                        update(BsTrafficLedger)
                        .where(BsTrafficLedger.subscription_id == 1)
                        .values(account_generation='f' * 64)
                    )
                else:
                    values = {'user_id': 2} if change == 'owner' else {'grace_session_open': True}
                    await other.execute(update(Subscription).where(Subscription.id == 1).values(**values))
            return panel()

        api.get_split_access_snapshot.side_effect = read
        result = await prepare(db, api, target)
        assert result.status in ('stale', 'blocked')
        assert not (await db.scalars(select(BsTrafficDelivery))).all()


async def test_shared_topology_does_not_cache_account_or_extend_operation(postgres_database):
    from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache

    async with postgres_session(postgres_database, [BsTrafficCollectionBudget.__table__, User.__table__]) as db:
        api = client()
        target = await setup(db, api)
        api.get_system_configuration.reset_mock()
        api.get_internal_squad_topology.reset_mock()
        cache = TopologyEvidenceCache(api, panel_identity=SCOPE)
        factory = async_sessionmaker(db.bind, expire_on_commit=True)
        first = await prepare_delivery(factory, api, **target, operation_key='cached-prepare', topology_cache=cache)
        second = await prepare_delivery(factory, api, **target, operation_key='cached-prepare', topology_cache=cache)
        assert first.status == second.status == 'queued'
        assert first.operation_id == second.operation_id
        assert first.evidence is second.evidence
        row = await db.get(BsTrafficDelivery, first.operation_id)
        assert row.valid_until == first.evidence.valid_until
        await db.rollback()
        assert api.get_system_configuration.await_count == 1
        assert api.get_internal_squad_topology.await_count == 2
        assert api.get_split_access_snapshot.await_count == 2
        api.get_split_access_snapshot.return_value = panel(id=13)
        rejected = await prepare_delivery(factory, api, **target, operation_key='cached-prepare', topology_cache=cache)
        assert rejected.status == 'blocked'
        assert api.get_split_access_snapshot.await_count == 3
