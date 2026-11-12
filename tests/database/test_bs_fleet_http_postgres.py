"""Loopback HTTP load: real client, real pacing, PostgreSQL, synthetic accounts."""

import asyncio
import time
from collections import Counter, deque
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web
from sqlalchemy import insert, select, true, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import settings
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficCutoverIntent,
    BsTrafficFleetWork,
    BsTrafficLedger,
    BsTrafficManagedTransfer,
    BsTrafficPanelBinding,
    BsTrafficUsageCursor,
)
from app.external.remnawave_api import RemnaWaveAPI
from app.services.bs_traffic import fleet_round
from app.services.bs_traffic.identity import account_generation
from app.services.bs_traffic.intake import attach_cursor
from app.services.bs_traffic.panel_binding import bind_reader, endpoint_fingerprint
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from tests.database.test_bs_fleet_round_postgres import bulk_fleet
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import BS, NODE_A, NODE_B, ORDINARY
from tests.database.test_bs_traffic_purchase_delivery_postgres import purchased
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.database.test_bs_traffic_runtime_postgres import runtime_for
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('reject_patch', [False, True])
async def test_100_exhaustions_real_http_pacing_and_retry_after(postgres_database, monkeypatch, reject_patch):
    accounts = 100
    async with postgres_session(postgres_database, TABLES) as db:
        original_reader, target, purchase, source_account = await purchased(db, monkeypatch)
        await advance_purchase_delivery(original_reader, **target, operation_id=purchase.delivery_id)
        _, day = await observe(db, original_reader, target, NODE_B, 100)
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        await bulk_fleet(db, accounts)
        series = __import__('sqlalchemy').func.generate_series(10, accounts + 8).table_valued('id').render_derived()
        for model in (BsTrafficAllowance, BsTrafficUsageCursor):
            table = model.__table__
            columns = [column for column in table.c if column.name != 'id']
            source = select(table).where(table.c.subscription_id == 1).subquery()
            await db.execute(
                insert(table).from_select(
                    [column.name for column in columns],
                    select(
                        *[
                            series.c.id if column.name == 'subscription_id' else source.c[column.name]
                            for column in columns
                        ]
                    ).select_from(source.join(series, true())),
                )
            )
        created = source_account.created_at
        ledgers = list(await db.scalars(select(BsTrafficLedger).order_by(BsTrafficLedger.subscription_id)))
        remote = {}
        for ledger in ledgers:
            identifier = ledger.panel_user_id
            generation = account_generation(
                panel_scope=original_reader.panel_scope, account=SimpleNamespace(id=identifier, created_at=created)
            )
            for model in (BsTrafficLedger, BsTrafficCutoverIntent, BsTrafficManagedTransfer, BsTrafficUsageCursor):
                await db.execute(
                    update(model)
                    .where(model.subscription_id == ledger.subscription_id)
                    .values(account_generation=generation)
                )
            remote[identifier] = dict(
                id=identifier,
                shortUuid=f'synthetic-{identifier}',
                username=f'synthetic-{identifier}',
                createdAt=created.isoformat(),
                updatedAt=created.isoformat(),
                expireAt=(datetime.now(UTC) + timedelta(days=30)).isoformat(),
                activeInternalSquads=[{'uuid': ORDINARY}, {'uuid': BS}],
            )
        amount = (await db.get(BsTrafficAllowance, purchase.grant_id)).remaining_bytes
        await db.commit()
        calls, patches, starts = Counter(), Counter(), []
        in_flight = maximum = 0
        first_patch = None
        patch_completed_count = None
        factory = None

        @web.middleware
        async def latency(request, handler):
            nonlocal in_flight, maximum
            starts.append(time.monotonic())
            in_flight += 1
            maximum = max(maximum, in_flight)
            try:
                await asyncio.sleep(0.02)
                return await handler(request)
            finally:
                in_flight -= 1

        async def configuration(request):
            calls['configuration'] += 1
            return web.json_response(
                {
                    'response': {
                        'service': {'disableUserUsageRecords': False, 'cleanUsageHistory': False},
                        'misc': {'userUsageIgnoreBelowBytes': 0},
                    }
                }
            )

        async def topology(request):
            squad = request.match_info['squad']
            calls['topology'] += 1
            return web.json_response(
                {
                    'response': {
                        'squadUuid': squad,
                        'accessibleNodes': [
                            {
                                'uuid': NODE_A if squad == ORDINARY else NODE_B,
                                'configProfileUuid': ORDINARY,
                                'activeInbounds': ['synthetic'],
                            }
                        ],
                    }
                }
            )

        async def usage(request):
            calls['usage'] += 1
            nodes = (await request.json())['nodesUuids']
            return web.json_response(
                {
                    'response': {
                        'nodes': [
                            {
                                'uuid': node,
                                'users': [
                                    {'id': identifier, 'totalBytes': 100 + (amount if node == NODE_B else 0)}
                                    for identifier in remote
                                ],
                            }
                            for node in nodes
                        ]
                    }
                }
            )

        async def get_account(request):
            calls['get'] += 1
            return web.json_response({'response': remote[int(request.match_info['identifier'])]})

        async def patch_account(request):
            nonlocal first_patch, patch_completed_count
            payload = await request.json()
            assert set(payload) == {'id', 'activeInternalSquads'}
            identifier = payload['id']
            patches[identifier] += 1
            if first_patch is None:
                first_patch = time.monotonic()
                async with factory() as check:
                    from sqlalchemy import func

                    patch_completed_count = await check.scalar(
                        select(func.count())
                        .select_from(BsTrafficFleetWork)
                        .where(BsTrafficFleetWork.completed_round.is_not(None))
                    )
            if reject_patch and identifier == 12:
                return web.json_response({'message': 'synthetic throttling'}, status=429, headers={'Retry-After': '1'})
            assert payload['activeInternalSquads'] == [ORDINARY]
            remote[identifier]['activeInternalSquads'] = [{'uuid': item} for item in payload['activeInternalSquads']]
            return web.json_response({'response': remote[identifier]})

        app = web.Application(middlewares=[latency])
        app.router.add_get('/api/system/configuration', configuration)
        app.router.add_get('/api/internal-squads/{squad}/accessible-nodes', topology)
        app.router.add_post('/api/bandwidth-stats/nodes/usage', usage)
        app.router.add_get('/api/users/{identifier}', get_account)
        app.router.add_patch('/api/users', patch_account)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        engine = create_async_engine(postgres_database, pool_size=20, max_overflow=0)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        base_url = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
        await db.execute(update(BsTrafficPanelBinding).values(endpoint_hash=endpoint_fingerprint(base_url)))
        await db.commit()
        # Exercise real client pacing with a compressed, real-clock test window:
        # 10 requests/second, not a claim about production's minute setting.
        monkeypatch.setattr(settings, 'REMNAWAVE_API_REQUESTS_PER_MINUTE', 10)
        monkeypatch.setattr('app.external.remnawave_api.PACE_WINDOW_SECONDS', 1.0)
        monkeypatch.setattr(RemnaWaveAPI, '_request_times', deque())
        monkeypatch.setattr(RemnaWaveAPI, '_throttled_until', 0.0)
        try:
            async with aiohttp.ClientSession() as session:
                api = RemnaWaveAPI(base_url, 'synthetic')
                api.session = session
                reader = await bind_reader(factory, api, panel_scope=original_reader.panel_scope)
                await fleet_round.plan_page(db, reader, limit=500)
                await db.commit()
                runtime = runtime_for(reader)
                started = time.monotonic()
                # Collection publishes all work. Warmed access proof is explicit
                # test setup; all following usage/PATCH/readbacks use real HTTP.
                await runtime._collection(reader)
                await db.execute(
                    update(BsTrafficFleetWork).values(
                        applied_squads=sorted([ORDINARY, BS]),
                        access_dirty=False,
                        calendar_due_at=datetime.now(UTC) + timedelta(days=1),
                        calendar_receipt_id=0,
                    )
                )
                await db.commit()
                expected = accounts - int(reject_patch)
                async with asyncio.timeout(120):
                    while (
                        sum(row['activeInternalSquads'] == [{'uuid': ORDINARY}] for row in remote.values()) < expected
                    ):
                        await runtime.run_once()
                await runtime.run_once()
                elapsed = time.monotonic() - started
                healthy = await runtime._health(reader)
                assert patch_completed_count < accounts
                assert maximum <= 4
                assert all(
                    ORDINARY in [squad['uuid'] for squad in row['activeInternalSquads']] for row in remote.values()
                )
                assert all(count == 1 for count in patches.values())
                assert (healthy is None) == reject_patch
                assert all(sum(begin <= stamp < begin + 0.99 for stamp in starts) <= 10 for begin in starts)
                print(
                    f'HTTP100 reject429={reject_patch}: first restriction {first_patch - started:.3f}s, '
                    f'completion {elapsed:.3f}s, maxHTTP={maximum}, PATCH={sum(patches.values())}, '
                    f'coverageHealthy={healthy is not None}, atFirstPatchCompleted={patch_completed_count}',
                    flush=True,
                )
        finally:
            await engine.dispose()
            await runner.cleanup()
