"""Large retry history is scanned in bounded pages, never as one huge join."""

import time

import pytest
from sqlalchemy import text

from app.services.bs_traffic.purchase_delivery_worker import purchase_delivery_page
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_fifty_thousand_historical_attempts_keep_page_budget(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, _, purchase, _ = await purchased(db, monkeypatch)
        await db.execute(
            text("""
            INSERT INTO bs_traffic_deliveries
                (id, subscription_id, operation_key, revision, guard_hash, local_guard_hash, squads,
                 state, valid_until)
            SELECT n, original.subscription_id, original.operation_key || ':retry:' || (n - 1)::text,
                   original.revision, original.guard_hash, original.local_guard_hash, original.squads,
                   'cancelled', original.valid_until
            FROM bs_traffic_deliveries original
            CROSS JOIN generate_series(CAST(:first AS integer), CAST(:last AS integer)) n
            WHERE original.id = :root
        """),
            {'first': purchase.delivery_id + 1, 'last': purchase.delivery_id + 50000, 'root': purchase.delivery_id},
        )
        await db.commit()
        after, upper, scanned, candidates, pages = 0, None, 0, [], 0
        started = time.monotonic()
        while True:
            page = await purchase_delivery_page(
                db, panel_scope=reader.panel_scope, after_id=after, upper_id=upper, limit=100
            )
            assert page.scanned <= 100
            scanned += page.scanned
            pages += 1
            candidates.extend(page.items)
            if page.next_after is None:
                break
            after, upper = page.next_after, page.upper_id
        elapsed = time.monotonic() - started
        assert scanned == 50001 and pages == 501
        assert len(candidates) == 1 and candidates[0].operation_id == purchase.delivery_id
        print(f'PAID_DELIVERY_SCAN rows={scanned} pages={pages} candidates={len(candidates)} seconds={elapsed:.3f}')
