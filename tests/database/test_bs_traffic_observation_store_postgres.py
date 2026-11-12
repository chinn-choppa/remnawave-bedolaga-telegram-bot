"""Raw panel evidence is never a charge or a refund."""

from datetime import date

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficObservation, BsTrafficPoll
from app.services.bs_traffic.observations import begin_poll, record_poll
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


SCOPE = '11111111-1111-4111-8111-111111111111'
NODE = '22222222-2222-4222-8222-222222222222'
DAY = date(2026, 11, 4)
TABLES = [BsTrafficPoll.__table__, BsTrafficObservation.__table__]


def response(value):
    return {'nodes': [{'uuid': NODE, 'users': [{'id': 7, 'totalBytes': value}]}]}


async def poll(db):
    return await begin_poll(db, panel_scope=SCOPE, day=DAY, node_uuids=(NODE,), identities={7: 'generation-a'})


async def state(db):
    return (await db.scalars(select(BsTrafficObservation).execution_options(populate_existing=True))).one()


async def test_duplicate_poll_is_idempotent_and_does_not_commit(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        pending = await poll(db)
        await db.commit()
        await record_poll(db, pending.id, response(100))
        await record_poll(db, pending.id, response(100))
        assert (await state(db)).high_water_bytes == 100
        await db.rollback()
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_delayed_older_response_cannot_lower_latest_counter(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        old, new = await poll(db), await poll(db)
        await record_poll(db, new.id, response(200))
        await record_poll(db, old.id, response(100))
        saved = await state(db)
        assert (saved.high_water_bytes, saved.latest_bytes, saved.latest_poll_id) == (200, 200, new.id)
        assert not saved.needs_review


async def test_newer_decrease_preserves_high_water_and_requires_review(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        for value in (200, 100, 300):
            pending = await poll(db)
            await record_poll(db, pending.id, response(value))
        saved = await state(db)
        assert (saved.high_water_bytes, saved.latest_bytes) == (300, 300)
        assert saved.needs_review


async def test_late_old_high_counter_also_reveals_regression(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        old, new = await poll(db), await poll(db)
        await record_poll(db, new.id, response(100))
        await record_poll(db, old.id, response(200))
        saved = await state(db)
        assert (saved.high_water_bytes, saved.latest_bytes) == (200, 100)
        assert saved.needs_review


async def test_absent_user_is_not_zero(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        first = await poll(db)
        await record_poll(db, first.id, response(100))
        second = await poll(db)
        await record_poll(db, second.id, {'nodes': [{'uuid': NODE, 'users': []}]})
        saved = await state(db)
        assert saved.latest_poll_id == first.id
        assert saved.high_water_bytes == 100


async def test_reused_poll_with_different_response_is_rejected(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        pending = await poll(db)
        await record_poll(db, pending.id, response(100))
        with pytest.raises(ValueError, match='already recorded'):
            await record_poll(db, pending.id, response(200))


@pytest.mark.parametrize('value', [True, -1, 1.5, '100', 2**53, None])
async def test_inexact_or_invalid_byte_values_never_enter_store(postgres_database, value):
    async with postgres_session(postgres_database, TABLES) as db:
        pending = await poll(db)
        with pytest.raises(ValueError):
            await record_poll(db, pending.id, response(value))
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_batch_persistence_does_not_issue_one_query_per_user(postgres_database):
    from sqlalchemy import event

    async with postgres_session(postgres_database, TABLES) as db:
        pending = await begin_poll(
            db,
            panel_scope=SCOPE,
            day=DAY,
            node_uuids=(NODE,),
            identities={value: f'generation-{value}' for value in range(1, 502)},
        )
        writes = []

        def count(connection, cursor, statement, parameters, context, executemany):
            if statement.startswith('INSERT INTO bs_traffic_observations'):
                writes.append(statement)

        event.listen(db.bind.sync_engine, 'before_cursor_execute', count)
        try:
            await record_poll(
                db,
                pending.id,
                {'nodes': [{'uuid': NODE, 'users': [{'id': value, 'totalBytes': value} for value in range(1, 502)]}]},
            )
        finally:
            event.remove(db.bind.sync_engine, 'before_cursor_execute', count)
        assert len(writes) <= 3
        assert len((await db.scalars(select(BsTrafficObservation))).all()) == 501


async def test_generation_scope_and_day_are_independent(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        for scope, day, generation in (
            (SCOPE, DAY, 'a'),
            (SCOPE, DAY, 'b'),
            (SCOPE, date(2026, 11, 5), 'a'),
            (NODE, DAY, 'a'),
        ):
            pending = await begin_poll(db, panel_scope=scope, day=day, node_uuids=(NODE,), identities={7: generation})
            await record_poll(db, pending.id, response(100))
        assert len((await db.scalars(select(BsTrafficObservation))).all()) == 4


@pytest.mark.parametrize(
    'payload',
    [
        {},
        {'nodes': None},
        {
            'nodes': [
                {
                    'uuid': NODE,
                    'users': [
                        {'id': 7, 'totalBytes': 100},
                        {'id': 7, 'totalBytes': 200},
                    ],
                }
            ]
        },
        {'nodes': [{'uuid': SCOPE, 'users': []}]},
        {'nodes': [{'uuid': NODE, 'users': []}, {'uuid': NODE, 'users': []}]},
        {'nodes': [{'uuid': NODE, 'users': [{'id': 7, 'totalBytes': 100}, {'id': 8, 'totalBytes': -1}]}]},
    ],
)
async def test_invalid_response_cannot_partially_write(postgres_database, payload):
    async with postgres_session(postgres_database, TABLES) as db:
        pending = await poll(db)
        with pytest.raises(ValueError):
            await record_poll(db, pending.id, payload)
        assert not (await db.scalars(select(BsTrafficObservation))).all()
        assert pending.response_hash is None


async def test_poll_sequence_does_not_reuse_ids_after_journal_cleanup(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        old = await poll(db)
        old_id = old.id
        await record_poll(db, old.id, response(100))
        await db.delete(old)
        await db.commit()
        new = await poll(db)
        assert new.id > old_id
        await record_poll(db, new.id, response(200))
        assert (await state(db)).latest_bytes == 200
