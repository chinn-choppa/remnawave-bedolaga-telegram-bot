from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, update

from app.database.models import BsTrafficDelivery as Delivery, BsTrafficLedger as Ledger, Subscription, User
from app.services.bs_traffic.recovery_scan import recovery_page, start_recovery_scan
from tests.database.test_bs_traffic_ledger_postgres import GENERATION, SCOPE
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
OTHER = '99999999-9999-4999-8999-999999999999'
NOW = datetime(2000, 1, 1, tzinfo=UTC)


async def seed_rows(db, states, *, first_id=1, scope=SCOPE):
    if first_id == 1:
        db.add_all([User(id=1), User(id=2)])
        await db.flush()
    ids = list(range(first_id, first_id + len(states)))
    await db.execute(
        insert(Subscription),
        [
            dict(
                id=i,
                user_id=1,
                remnawave_id=i,
                remnawave_short_id=f'synthetic-{i}',
                start_date=NOW,
                end_date=NOW,
            )
            for i in ids
        ],
    )
    await db.execute(
        insert(Ledger),
        [
            dict(
                subscription_id=i,
                panel_scope=scope,
                panel_user_id=i,
                account_generation=GENERATION,
                revision=0,
            )
            for i in ids
        ],
    )
    rows = []
    for i, state in zip(ids, states, strict=True):
        attempted = state not in ('pending', 'cancelled')
        rows.append(
            dict(
                id=i,
                subscription_id=i,
                operation_key=f'operation-{i}',
                revision=0,
                guard_hash='b' * 64,
                local_guard_hash='c' * 64,
                squads=[],
                state=state,
                token=str(uuid4()) if attempted else None,
                attempted_at=NOW if attempted else None,
                not_sent_reason='stale_evidence' if state == 'not_sent' else None,
                valid_until=NOW,
            )
        )
    await db.execute(insert(Delivery), rows)
    await db.commit()
    return ids


async def test_scope_state_live_attempt_filter_and_candidates_have_no_credentials(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed_rows(db, ['pending', 'attempted', 'attention', 'verified', 'not_sent', 'cancelled', 'attempted'])
        await seed_rows(db, ['attention'], first_id=8, scope=OTHER)
        await db.execute(update(Delivery).where(Delivery.id == 7).values(attempted_at=datetime.now(UTC)))
        await db.execute(update(Subscription).where(Subscription.id == 3).values(user_id=2))
        await db.commit()
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        page = await recovery_page(db, window)
        assert [item.operation_id for item in page.items] == [2, 3]
        assert [item.user_id for item in page.items] == [1, 2]
        assert all(item.account_generation == GENERATION for item in page.items)
        assert page.next_after is None
        assert set(page.items[0].__dataclass_fields__) == {
            'user_id',
            'subscription_id',
            'account_generation',
            'operation_id',
        }
        assert dict((await db.execute(select(Delivery.id, Delivery.state))).all())[2] == 'attempted'


async def test_keyset_progress_does_not_repeat_stuck_oldest_or_include_new_rows(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed_rows(db, ['attention'] * 5)
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        page = await recovery_page(db, window, limit=2)
        assert [item.operation_id for item in page.items] == [1, 2]
        assert page.next_after == 2
        await seed_rows(db, ['attention'], first_id=6)
        page = await recovery_page(db, window, after_id=page.next_after, limit=2)
        assert [item.operation_id for item in page.items] == [3, 4]
        page = await recovery_page(db, window, after_id=page.next_after, limit=2)
        assert [item.operation_id for item in page.items] == [5]
        assert page.next_after is None
        renewed = await start_recovery_scan(db, panel_scope=SCOPE)
        assert [item.operation_id for item in (await recovery_page(db, renewed)).items] == [1, 2, 3, 4, 5, 6]


async def test_terminal_transition_between_pages_is_not_returned(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed_rows(db, ['attention'] * 3)
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        first = await recovery_page(db, window, limit=1)
        await db.execute(update(Delivery).where(Delivery.id == 2).values(state='verified'))
        await db.commit()
        page = await recovery_page(db, window, after_id=first.next_after)
        assert [item.operation_id for item in page.items] == [3]


@pytest.mark.parametrize('limit', [0, -1, True, 501, 1.5])
async def test_invalid_limits(postgres_database, limit):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        with pytest.raises(ValueError):
            await recovery_page(db, window, limit=limit)


async def test_empty_scope(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        page = await recovery_page(db, window)
        assert page.items == () and page.next_after is None


async def test_ten_thousand_rows_are_traversed_in_bounded_pages(postgres_database):
    from time import perf_counter

    from sqlalchemy import literal, text

    from app.services.bs_traffic.recovery_scan import _page_query

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed_rows(db, ['attention' if i % 10 == 0 else 'verified' for i in range(10000)])
        for table in ('bs_traffic_deliveries', 'bs_traffic_ledgers', 'subscriptions'):
            await db.execute(text(f'ANALYZE {table}'))
        started = perf_counter()
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        cursor, visited, pages = 0, [], 0
        while True:
            page = await recovery_page(db, window, after_id=cursor, limit=100)
            assert len(page.items) <= 100
            visited.extend(item.operation_id for item in page.items)
            pages += 1
            if page.next_after is None:
                break
            assert page.next_after > cursor
            cursor = page.next_after
        elapsed = perf_counter() - started
        assert visited == list(range(1, 10001, 10)) and pages == 10
        query = _page_query(window, 0, 100).compile(
            dialect=db.get_bind().dialect, compile_kwargs={'literal_binds': True}
        )
        plan = await db.scalar(text(f'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}'))

        def indexes(node):
            result = [node['Index Name']] if 'Index Name' in node else []
            for child in node.get('Plans', []):
                result.extend(indexes(child))
            return result

        assert plan[0]['Plan']['Actual Rows'] == 101
        compiled = _page_query(window, 0, 100).compile(
            dialect=db.get_bind().dialect, compile_kwargs={'render_postcompile': True}
        )
        arguments = ', '.join(
            str(
                literal(compiled.params[key]).compile(
                    dialect=db.get_bind().dialect, compile_kwargs={'literal_binds': True}
                )
            )
            for key in compiled.positiontup
        )
        await db.execute(text('SET LOCAL plan_cache_mode = force_generic_plan'))
        await db.execute(text(f'PREPARE synthetic_recovery_scan AS {compiled}'))
        try:
            generic = await db.scalar(
                text(f'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE synthetic_recovery_scan({arguments})')
            )
        finally:
            await db.execute(text('DEALLOCATE synthetic_recovery_scan'))
        print(f'GENERIC query_ms={generic[0]["Execution Time"]} indexes={indexes(generic[0]["Plan"])}')
        assert 'ix_bs_delivery_recovery_scan' in indexes(generic[0]['Plan'])
        print(
            f'SCAN rows=10000 eligible=1000 pages={pages} elapsed_ms={elapsed * 1000:.2f} '
            f'query_ms={plan[0]["Execution Time"]} indexes={indexes(plan[0]["Plan"])}'
        )


@pytest.mark.parametrize('after', [-1, True, 1.5, 999])
async def test_invalid_cursor(postgres_database, after):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        with pytest.raises(ValueError):
            await recovery_page(db, window, after_id=after)


async def test_eligibility_cutoff_stays_fixed_between_pages(postgres_database):
    from dataclasses import replace
    from datetime import timedelta

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed_rows(db, ['attention'] * 3)
        window = await start_recovery_scan(db, panel_scope=SCOPE)
        first = await recovery_page(db, window, limit=1)
        # Simulate an attempt that becomes eligible only after this pass began.
        await db.execute(
            update(Delivery).where(Delivery.id == 2).values(attempted_at=window.eligible_before + timedelta(seconds=1))
        )
        await db.commit()
        remaining = await recovery_page(db, window, after_id=first.next_after)
        assert [item.operation_id for item in remaining.items] == [3]
        later = replace(window, eligible_before=window.eligible_before + timedelta(seconds=2))
        assert [item.operation_id for item in (await recovery_page(db, later)).items] == [1, 2, 3]
