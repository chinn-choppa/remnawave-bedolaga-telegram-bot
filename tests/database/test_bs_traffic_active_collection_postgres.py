import pytest
from sqlalchemy import select

from app.database.models import BsTrafficCollectionJob, BsTrafficLedger, Subscription
from app.services.bs_traffic.active_collection import enqueue_active_collection, start_active_collection
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import retire_binding
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_active_collection_is_bounded_idempotent_and_does_not_charge(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        before = await totals(db)
        window = await start_active_collection(db, panel_scope=reader.panel_scope)
        page = await enqueue_active_collection(db, reader, window, round_key='round')
        again = await enqueue_active_collection(db, reader, window, round_key='round')
        assert page == again
        assert page.accounts == 1 and len(page.job_ids) == 2 and page.next_after is None
        jobs = (await db.scalars(select(BsTrafficCollectionJob))).all()
        assert len(jobs) == 2
        for job in jobs:
            assert len(job.node_uuids) == 1
            assert job.identities == {'12': target['account_generation']}
        assert await totals(db) == before
        reader._api.update_internal_squads_once.assert_not_awaited()


@pytest.mark.parametrize('change', ['legacy', 'panel_id', 'generation'])
async def test_unconfirmed_ownership_not_collected(postgres_database, monkeypatch, change):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        window = await start_active_collection(db, panel_scope=reader.panel_scope)
        if change == 'legacy':
            (await db.get(Subscription, 1)).traffic_accounting_mode = 'legacy'
        elif change == 'panel_id':
            (await db.get(Subscription, 1)).remnawave_id = 99
        else:
            (await db.get(BsTrafficLedger, 1)).account_generation = 'b' * 64
        await db.flush()
        page = await enqueue_active_collection(db, reader, window, round_key='round')
        assert not page.job_ids and page.accounts == 0


async def test_retired_scope_rejected(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        window = await start_active_collection(db, panel_scope=reader.panel_scope)
        await retire_binding(db, panel_scope=reader.panel_scope)
        with pytest.raises(LedgerConflict):
            await enqueue_active_collection(db, reader, window, round_key='round')


async def test_page_highwater_and_continuation(postgres_database, monkeypatch):
    from sqlalchemy import insert, literal

    from app.database.models import TrafficAccountingState
    from tests.database.test_bs_traffic_cutover_scan_postgres import clone_candidate

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)

        async def clone(identifier):
            await clone_candidate(db, identifier)
            table = TrafficAccountingState.__table__
            await db.execute(
                insert(table).from_select(
                    [c.name for c in table.c],
                    select(
                        *[literal(identifier, type_=c.type) if c.name == 'subscription_id' else c for c in table.c]
                    ).where(table.c.subscription_id == 1),
                )
            )
            await db.flush()

        await clone(10)
        window = await start_active_collection(db, panel_scope=reader.panel_scope)
        await clone(11)
        first = await enqueue_active_collection(db, reader, window, round_key='pages', limit=1)
        assert first.accounts == 1 and first.next_after == 1
        second = await enqueue_active_collection(
            db, reader, window, round_key='pages', after_id=first.next_after, limit=1
        )
        assert second.accounts == 1 and second.next_after is None
        for identifier in second.job_ids:
            assert (await db.get(BsTrafficCollectionJob, identifier)).identities.keys() == {'110'}


async def test_round_keeps_its_day_across_midnight(postgres_database, monkeypatch):
    from dataclasses import replace
    from datetime import timedelta

    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, _, _ = await prepared_purchase(db, monkeypatch)
        window = await start_active_collection(db, panel_scope=reader.panel_scope)
        yesterday = window.day - timedelta(days=1)
        window = replace(window, day=yesterday)
        page = await enqueue_active_collection(db, reader, window, round_key='midnight')
        again = await enqueue_active_collection(db, reader, window, round_key='midnight')
        assert page == again
        for identifier in page.job_ids:
            assert (await db.get(BsTrafficCollectionJob, identifier)).day == yesterday
