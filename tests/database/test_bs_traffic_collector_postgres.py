"""Collector boundaries use real transactions; HTTP is never inside one."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficObservation, BsTrafficPoll
from app.services.bs_traffic.collector import collect_daily_usage
from tests.database.test_bs_traffic_observation_store_postgres import DAY, NODE, SCOPE, TABLES, response
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def arguments():
    return dict(panel_scope=SCOPE, day=DAY, node_uuids=(NODE,), identities={7: 'generation-a'})


async def test_request_is_committed_and_session_closed_before_http(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        sessions = []

        def factory():
            session = maker()
            sessions.append(session)
            return session

        async def fetch(nodes, day, **limits):
            assert nodes == (NODE,) and day == DAY
            assert not sessions[0].in_transaction()
            async with maker() as reader:
                pending = (await reader.scalars(select(BsTrafficPoll))).one()
                assert pending.response_hash is None
                assert pending.identities == {'7': 'generation-a'}
            return response(100)

        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=fetch)
        poll_id = await collect_daily_usage(factory, api, **arguments())
        async with maker() as reader:
            assert (await reader.get(BsTrafficPoll, poll_id)).response_hash
            assert (await reader.scalars(select(BsTrafficObservation))).one().high_water_bytes == 100
        assert all(not session.in_transaction() for session in sessions)
        api.get_daily_nodes_usage.assert_awaited_once()


async def test_timeout_leaves_uncompleted_poll_without_zero_and_retry_uses_new_id(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        maker = async_sessionmaker(db.bind, expire_on_commit=False)

        async def hang(*args, **kwargs):
            await asyncio.sleep(10)

        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=hang)
        with pytest.raises(TimeoutError):
            await collect_daily_usage(maker, api, **arguments(), timeout_seconds=0.01)
        async with maker() as reader:
            failed = (await reader.scalars(select(BsTrafficPoll))).one()
            assert failed.response_hash is None
            assert not (await reader.scalars(select(BsTrafficObservation))).all()
        api.get_daily_nodes_usage = AsyncMock(return_value=response(200))
        new_id = await collect_daily_usage(maker, api, **arguments())
        assert new_id > failed.id
        async with maker() as reader:
            assert (await reader.get(BsTrafficPoll, failed.id)).response_hash is None
            assert (await reader.get(BsTrafficPoll, new_id)).response_hash


async def test_invalid_response_rolls_back_all_observations(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        maker = async_sessionmaker(db.bind, expire_on_commit=False)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(
            return_value={
                'nodes': [{'uuid': NODE, 'users': [{'id': 7, 'totalBytes': 100}, {'id': 8, 'totalBytes': -1}]}],
            }
        )
        with pytest.raises(ValueError):
            await collect_daily_usage(maker, api, **arguments())
        assert not (await db.scalars(select(BsTrafficObservation))).all()
        assert (await db.scalars(select(BsTrafficPoll))).one().response_hash is None


async def test_cancelled_fetch_does_not_record_or_replay(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        maker = async_sessionmaker(db.bind, expire_on_commit=False)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=asyncio.CancelledError)
        with pytest.raises(asyncio.CancelledError):
            await collect_daily_usage(maker, api, **arguments())
        api.get_daily_nodes_usage.assert_awaited_once()
        assert not (await db.scalars(select(BsTrafficObservation))).all()
        assert (await db.scalars(select(BsTrafficPoll))).one().response_hash is None
