from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficObservation, BsTrafficPoll, BsTrafficUsageCursor
from app.services.bs_traffic.cutover_baseline import inspect_cutover_baseline
from app.services.bs_traffic.intake import attach_cursor, stage_observation
from app.services.bs_traffic.ledger import LedgerConflict, resolve_usage
from app.services.bs_traffic.observations import begin_poll, record_poll
from tests.database.test_bs_traffic_managed_cutover_postgres import setup
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES as SOURCE_TABLES
from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [*SOURCE_TABLES, BsTrafficObservation.__table__, BsTrafficPoll.__table__]


async def observe(db, reader, keys, node=NODE_A, amount=100, day=None):
    if day is None:
        day = (await db.scalar(select(func.clock_timestamp()))).date()
    poll = await begin_poll(
        db, panel_scope=reader.panel_scope, day=day, node_uuids=(node,), identities={12: keys['account_generation']}
    )
    await record_poll(db, poll.id, {'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': amount}]}]})
    return poll, day


async def seeded(db, monkeypatch, **modes):
    reader, _, keys = await setup(db, monkeypatch, **modes)
    poll, day = await observe(db, reader, keys)
    await attach_cursor(db, **keys, group='ordinary', node_uuid=NODE_A, day=day)
    await db.commit()
    return reader, keys, poll.id, day


async def test_explicit_baseline_allows_inspection_without_changing_balance(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _ = await seeded(db, monkeypatch)
        result = await inspect_cutover_baseline(db, reader, **keys)
        assert result.node_count == 1 and len(result.fingerprint) == 64
        assert result.fingerprint == (await inspect_cutover_baseline(db, reader, **keys)).fingerprint
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800


async def test_missing_finite_group_baseline_is_not_assumed_zero(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, day = await seeded(db, monkeypatch, bs='finite')
        with pytest.raises(LedgerConflict, match='explicit current-day'):
            await inspect_cutover_baseline(db, reader, **keys)
        await observe(db, reader, keys, node=NODE_B, amount=0)
        await attach_cursor(db, **keys, group='bs', node_uuid=NODE_B, day=day)
        assert (await inspect_cutover_baseline(db, reader, **keys)).node_count == 2


@pytest.mark.parametrize('change', ['stale', 'scope', 'incomplete', 'wrong_group', 'review', 'unprocessed'])
async def test_untrusted_or_unprocessed_evidence_blocks_activation_readiness(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, poll_id, day = await seeded(db, monkeypatch)
        poll = await db.get(BsTrafficPoll, poll_id)
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE_A, day))
        if change == 'stale':
            poll.created_at -= timedelta(seconds=60)
        elif change == 'scope':
            poll.panel_scope = '88888888-8888-4888-8888-888888888888'
        elif change == 'incomplete':
            poll.response_hash = None
        elif change == 'wrong_group':
            cursor.traffic_group = 'bs'
        elif change == 'review':
            cursor.needs_review = True
        else:
            await observe(db, reader, keys, amount=150)
        await db.commit()
        with pytest.raises(
            LedgerConflict, match='Unresolved usage' if change == 'review' else 'stale, unprocessed or inconsistent'
        ):
            await inspect_cutover_baseline(db, reader, **keys)


async def test_new_raw_delta_must_be_resolved_without_charging_imported_grants(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, day = await seeded(db, monkeypatch)
        await observe(db, reader, keys, amount=150)
        event = await stage_observation(db, **keys, node_uuid=NODE_A, day=day)
        with pytest.raises(LedgerConflict, match='Unresolved usage'):
            await inspect_cutover_baseline(db, reader, **keys)
        await resolve_usage(db, **keys, usage_id=event, decision='write_off')
        assert (await inspect_cutover_baseline(db, reader, **keys)).node_count == 1
        assert await db.scalar(select(func.sum(BsTrafficAllowance.remaining_bytes))) == 800


async def test_yesterday_cursor_does_not_enroll_today(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch)
        yesterday = (await db.scalar(select(func.clock_timestamp()))).date() - timedelta(days=1)
        await observe(db, reader, keys, day=yesterday)
        await attach_cursor(db, **keys, group='ordinary', node_uuid=NODE_A, day=yesterday)
        with pytest.raises(LedgerConflict, match='explicit current-day'):
            await inspect_cutover_baseline(db, reader, **keys)


async def test_unlimited_groups_do_not_require_finite_traffic_baselines(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch, ordinary='unlimited', bs='unlimited')
        assert (await inspect_cutover_baseline(db, reader, **keys)).node_count == 0


async def test_maximum_topology_uses_one_coverage_query(postgres_database, monkeypatch):
    from sqlalchemy import event

    from app.database.models import BsTrafficLedger

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, keys = await setup(db, monkeypatch, bs='finite')
        day = (await db.scalar(select(func.clock_timestamp()))).date()
        nodes = [f'{i:08x}-0000-4000-8000-000000000000' for i in range(1, 201)]
        ledger = await db.get(BsTrafficLedger, 1)
        ledger.access_policy = {
            **ledger.access_policy,
            'topology': {
                **ledger.access_policy['topology'],
                'ordinary_nodes': nodes[:100],
                'bs_nodes': nodes[100:],
            },
        }
        for group, selected in [('ordinary', nodes[:100]), ('bs', nodes[100:])]:
            poll = await begin_poll(
                db,
                panel_scope=reader.panel_scope,
                day=day,
                node_uuids=tuple(selected),
                identities={12: keys['account_generation']},
            )
            await record_poll(
                db, poll.id, {'nodes': [{'uuid': node, 'users': [{'id': 12, 'totalBytes': 0}]} for node in selected]}
            )
            db.add_all(
                [
                    BsTrafficUsageCursor(
                        subscription_id=1,
                        node_uuid=node,
                        day=day,
                        account_generation=keys['account_generation'],
                        traffic_group=group,
                        baseline_bytes=0,
                        processed_bytes=0,
                        needs_review=False,
                    )
                    for node in selected
                ]
            )
        await db.commit()
        queries = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            if 'JOIN bs_traffic_observations' in statement:
                queries.append(statement)

        event.listen(db.bind.sync_engine, 'before_cursor_execute', capture)
        try:
            assert (await inspect_cutover_baseline(db, reader, **keys)).node_count == 200
        finally:
            event.remove(db.bind.sync_engine, 'before_cursor_execute', capture)
        assert len(queries) == 1
