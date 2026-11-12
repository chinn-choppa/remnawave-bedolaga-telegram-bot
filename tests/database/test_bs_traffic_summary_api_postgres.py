"""Owned persisted balances are readable without panel writes or new debits."""

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.cabinet.routes.subscription_modules import split_traffic
from app.database.models import Subscription
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    'owner,mode,status', [(1, 'split', 200), (2, 'split', 404), (1, 'legacy', 404), (1, 'split_pending', 404)]
)
async def test_owned_summary_never_calls_panel_or_changes_money(postgres_database, monkeypatch, owner, mode, status):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        subscription = await db.get(Subscription, target['subscription_id'])
        subscription.traffic_accounting_mode = mode
        await db.commit()
        before = await totals(db)
        app = FastAPI()
        app.include_router(split_traffic.router)
        app.dependency_overrides[get_current_cabinet_user] = lambda: SimpleNamespace(id=owner)
        app.dependency_overrides[get_cabinet_db] = lambda: db
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as http:
            response = await http.get('/traffic/split-summary', params={'subscription_id': target['subscription_id']})
        assert response.status_code == status, response.text
        if status == 200:
            data = response.json()
            assert data['subscription_id'] == target['subscription_id']
            assert set(data['groups']) == {'ordinary', 'bs'}
            assert data['groups']['bs']['mode'] == 'finite'
            assert isinstance(data['groups']['bs']['remaining_bytes'], str)
            assert 'panel_scope' not in response.text and 'account_generation' not in response.text
        assert await totals(db) == before
        reader._api.get_split_access_snapshot.assert_not_awaited()
        reader._api.update_internal_squads_once.assert_not_awaited()


@pytest.mark.parametrize('mode', ['finite', 'unlimited', 'disabled'])
async def test_summary_preserves_pending_and_does_not_present_it_as_spent(postgres_database, monkeypatch, mode):
    from datetime import timedelta

    from sqlalchemy import func, select

    from app.database.models import BsTrafficLedger, BsTrafficUsage
    from app.services.bs_traffic.customer_summary import customer_summary
    from app.services.bs_traffic.ledger import open_usage

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, target['subscription_id'])
        ledger.access_policy = dict(ledger.access_policy, bs_mode=mode)
        await db.flush()
        now = await db.scalar(select(func.clock_timestamp()))
        event_id = await open_usage(
            db,
            **target,
            group='bs',
            source_key='synthetic-pending',
            amount_bytes=123,
            interval_start=now - timedelta(seconds=1),
            interval_end=now,
            evidence_kind='raw_pending',
        )
        await db.flush()
        before = await totals(db)
        data = await customer_summary(db, user_id=target['user_id'], subscription_id=target['subscription_id'])
        group = data['groups']['bs']
        assert group['mode'] == mode
        assert group['pending_bytes'] == '123' and group['pending_operations'] == 1
        assert (group['remaining_bytes'] is None) == (mode != 'finite')
        assert (await db.get(BsTrafficUsage, event_id)).state == 'pending'
        assert await totals(db) == before


@pytest.mark.parametrize('broken', ['activation', 'tariff', 'policy'])
async def test_invalid_state_is_not_a_valid_customer_balance(postgres_database, monkeypatch, broken):
    from sqlalchemy import update

    from app.database.models import BsTrafficCutoverIntent, BsTrafficLedger
    from app.services.bs_traffic.customer_summary import customer_summary
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        ledger = await db.get(BsTrafficLedger, target['subscription_id'])
        if broken == 'activation':
            await db.execute(update(BsTrafficCutoverIntent).values(state='verified', activated_at=None))
        elif broken == 'tariff':
            ledger.access_policy = dict(ledger.access_policy, tariff_id=999)
        else:
            ledger.access_policy = {}
        await db.flush()
        with pytest.raises(LedgerConflict):
            await customer_summary(db, user_id=target['user_id'], subscription_id=target['subscription_id'])


async def test_large_exact_balance_and_grace_marker(postgres_database, monkeypatch):
    from datetime import timedelta

    from sqlalchemy import func, select, update

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.customer_summary import customer_summary

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        await db.execute(
            update(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'bs').values(expires_at=now)
        )
        exact = 9007199254740993
        db.add(
            BsTrafficAllowance(
                subscription_id=target['subscription_id'],
                traffic_group='bs',
                operation_key='large',
                granted_bytes=exact,
                remaining_bytes=exact,
                starts_at=now,
                expires_at=now + timedelta(days=1),
            )
        )
        await db.execute(
            update(Subscription).where(Subscription.id == target['subscription_id']).values(grace_session_open=True)
        )
        await db.flush()
        data = await customer_summary(db, user_id=target['user_id'], subscription_id=target['subscription_id'])
        assert data['groups']['bs']['remaining_bytes'] == '9007199254740993'
        assert data['grace_active'] is True
