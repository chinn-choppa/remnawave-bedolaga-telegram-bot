from dataclasses import replace

import pytest

from app.services.bs_traffic import cutover_scan
from app.services.bs_traffic.cutover_intent import cancel_pending_cutover, stage_cutover
from tests.database.test_bs_traffic_cutover_intent_postgres import staged
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_scan_returns_exact_owned_operation_and_excludes_other_panel(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, op = await staged(db, monkeypatch)
        window = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        page = await cutover_scan.cutover_page(db, window, limit=1)
        assert len(page.items) == 1 and page.next_after is None
        candidate = page.items[0]
        assert (candidate.user_id, candidate.subscription_id, candidate.account_generation, candidate.operation_id) == (
            keys['user_id'],
            keys['subscription_id'],
            keys['account_generation'],
            op,
        )
        foreign = await cutover_scan.start_cutover_scan(db, panel_scope='88888888-8888-4888-8888-888888888888')
        assert not (await cutover_scan.cutover_page(db, foreign)).items


async def test_cancelled_operation_and_replacement_after_window_are_not_returned(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await staged(db, monkeypatch)
        window = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        await cancel_pending_cutover(db, **keys, operation_id=op)
        replacement = await stage_cutover(db, reader, **keys, operation_key='next', plan=plan)
        await db.commit()
        assert not (await cutover_scan.cutover_page(db, window)).items
        fresh = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        assert [v.operation_id for v in (await cutover_scan.cutover_page(db, fresh)).items] == [replacement]


@pytest.mark.parametrize('bad', [0, 501, True, -1])
async def test_invalid_page_size_is_rejected(postgres_database, monkeypatch, bad):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await staged(db, monkeypatch)
        window = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        with pytest.raises(ValueError):
            await cutover_scan.cutover_page(db, window, limit=bad)
        with pytest.raises(ValueError):
            await cutover_scan.cutover_page(db, replace(window, upper_id=-1))


async def test_migration_installs_scan_index_on_existing_table(postgres_database):
    import sqlalchemy as sa
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.database.models import BsTrafficCutoverIntent
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0149_bs_cutover_intent.py')
    async with postgres_session(postgres_database, TABLES) as db:

        def install(conn):
            conn.execute(sa.text('DROP INDEX IF EXISTS ix_bs_cutover_scan'))
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
                migration.upgrade()
            indexes = {item['name']: item for item in sa.inspect(conn).get_indexes('bs_traffic_cutover_intents')}
            assert indexes['ix_bs_cutover_scan']['column_names'] == ['panel_scope', 'id']
            predicate = indexes['ix_bs_cutover_scan']['dialect_options']['postgresql_where']
            assert all(value in predicate for value in ('pending', 'attempted', 'verified'))
            assert 'activated' not in predicate
            assert 'ix_bs_cutover_scan' in {index.name for index in BsTrafficCutoverIntent.__table__.indexes}

        await (await db.connection()).run_sync(install)


async def clone_candidate(db, new_id):
    """Clone only rows needed by the read-only scan, not a payable subscription."""
    from sqlalchemy import insert, literal, select

    from app.database.models import (
        BsTrafficCutoverIntent,
        BsTrafficLedger,
        BsTrafficManagedTransfer,
        Subscription,
        User,
    )

    db.add(User(id=new_id))
    await db.flush()
    for model, key in [
        (Subscription, 'id'),
        (BsTrafficLedger, 'subscription_id'),
        (BsTrafficManagedTransfer, 'subscription_id'),
        (BsTrafficCutoverIntent, 'subscription_id'),
    ]:
        table = model.__table__
        changes = {key: new_id}
        if model is BsTrafficCutoverIntent:
            changes['id'] = new_id
        if model is BsTrafficLedger:
            changes['panel_user_id'] = 100 + new_id
        if model is Subscription:
            changes['user_id'] = new_id
            changes['remnawave_id'] = 100 + new_id
            changes['remnawave_short_id'] = f'synthetic-{new_id}'
            changes['remnawave_uuid'] = f'{new_id:08x}-0000-4000-8000-000000000000'
        await db.execute(
            insert(table).from_select(
                [c.name for c in table.c],
                select(*[literal(changes[c.name], type_=c.type) if c.name in changes else c for c in table.c]).where(
                    table.c[key] == 1
                ),
            )
        )
    await db.flush()


async def test_keyset_pages_never_repeat_and_do_not_read_terminal_history(postgres_database, monkeypatch):
    from sqlalchemy import update

    from app.database.models import BsTrafficCutoverIntent

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await staged(db, monkeypatch)
        for value in range(10, 14):
            await clone_candidate(db, value)
        await db.execute(
            update(BsTrafficCutoverIntent).where(BsTrafficCutoverIntent.id == 11).values(state='cancelled')
        )
        window = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        first = await cutover_scan.cutover_page(db, window, limit=2)
        assert [v.operation_id for v in first.items] == [1, 10]
        assert first.next_after == 10
        second = await cutover_scan.cutover_page(db, window, after_id=first.next_after, limit=2)
        assert [v.operation_id for v in second.items] == [12, 13]
        assert second.next_after is None


async def test_scan_excludes_replaced_account_generation(postgres_database, monkeypatch):
    from app.database.models import BsTrafficLedger

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await staged(db, monkeypatch)
        window = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        (await db.get(BsTrafficLedger, 1)).account_generation = 'b' * 64
        await db.flush()
        assert not (await cutover_scan.cutover_page(db, window)).items


async def test_page_query_can_use_partial_index_with_generic_plan(postgres_database, monkeypatch):
    import json

    from sqlalchemy import event, text

    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, _, _ = await staged(db, monkeypatch)
        window = await cutover_scan.start_cutover_scan(db, panel_scope=reader.panel_scope)
        captured = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith('SELECT subscriptions.user_id'):
                captured.append((statement, parameters))

        event.listen(db.bind.sync_engine, 'before_cursor_execute', capture)
        try:
            await cutover_scan.cutover_page(db, window, limit=1)
        finally:
            event.remove(db.bind.sync_engine, 'before_cursor_execute', capture)
        assert len(captured) == 1
        # Force index consideration on this tiny fixture. This checks predicate
        # compatibility, not real-world planner choice or throughput.
        await db.execute(text('SET LOCAL enable_seqscan = off'))
        await db.execute(text('SET LOCAL plan_cache_mode = force_generic_plan'))
        statement, parameters = captured[0]
        plan = (
            await (await db.connection()).exec_driver_sql('EXPLAIN (FORMAT JSON) ' + statement, parameters)
        ).scalar()
        if isinstance(plan, str):
            plan = json.loads(plan)

        def indexes(node):
            yield node.get('Index Name')
            for child in node.get('Plans', []):
                yield from indexes(child)

        assert 'ix_bs_cutover_scan' in set(indexes(plan[0]['Plan']))
