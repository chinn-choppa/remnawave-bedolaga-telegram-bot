from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficPanelBinding, User
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import bind_reader, register_binding
from tests.database.test_bs_traffic_grace_transition_postgres import grace_session
from tests.database.test_bs_traffic_ledger_postgres import SCOPE
from tests.database.test_bs_traffic_prepare_postgres import client, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = [BsTrafficPanelBinding.__table__, BsTrafficCollectionBudget.__table__, User.__table__]


async def prepare(db):
    api = client()
    api.base_url = 'https://panel.example'
    target = await setup(db, api)
    await register_binding(db, panel_scope=SCOPE, endpoint=api.base_url)
    await db.commit()
    factory = async_sessionmaker(db.bind, expire_on_commit=False)
    reader = await bind_reader(factory, api, panel_scope=SCOPE)
    account = api.get_split_access_snapshot.return_value
    before = replace(
        grace_session().panel_before, squad_uuids=tuple(sorted(s['uuid'] for s in account.active_internal_squads))
    )
    from app.external.remnawave_api import SplitGraceSnapshot

    api.get_split_grace_snapshot = AsyncMock(
        return_value=SplitGraceSnapshot(
            account.id,
            account.created_at,
            account.active_internal_squads,
            before.status.upper(),
            before.expire_at,
            before.traffic_limit_bytes,
            'NO_RESET',
            before.used_traffic_bytes,
            2,
            before.external_squad_uuid,
            before.last_traffic_reset_at,
        )
    )
    return api, reader, target, before


async def test_capture_uses_actual_selection_and_live_topology(postgres_database):
    from app.services.bs_traffic.grace_capture import capture_grace_context

    async with postgres_session(postgres_database, TABLES) as db:
        api, reader, target, before = await prepare(db)
        result = await capture_grace_context(reader, **target, panel_before=before)
        assert result.panel_scope == SCOPE
        assert result.account_generation == target['account_generation']
        assert result.paid_squads == before.squad_uuids
        assert result.subscription_id == 1 and result.panel_user_id == 12
        api.get_split_grace_snapshot.assert_awaited_once()


@pytest.mark.parametrize('change', ['account', 'selection', 'policy'])
async def test_capture_rejects_divergent_live_source(postgres_database, change):
    from app.services.bs_traffic.grace_capture import capture_grace_context

    async with postgres_session(postgres_database, TABLES) as db:
        api, reader, target, before = await prepare(db)
        if change == 'account':
            api.get_split_grace_snapshot.return_value = replace(api.get_split_grace_snapshot.return_value, id=99)
        elif change == 'selection':
            before = replace(before, squad_uuids=())
        else:
            api.get_internal_squad_topology.side_effect = None
            api.get_internal_squad_topology.return_value = {}
        with pytest.raises(ValueError):
            await capture_grace_context(reader, **target, panel_before=before)


async def test_grace_start_during_capture_invalidates_result(postgres_database):
    from app.services.bs_traffic.grace_capture import capture_grace_context
    from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore

    async with postgres_session(postgres_database, TABLES) as db:
        api, reader, target, before = await prepare(db)
        account = api.get_split_grace_snapshot.return_value

        async def race(_):
            async with async_sessionmaker(db.bind, expire_on_commit=False)() as worker:
                await SQLAlchemyGraceSessionStore(worker).create(grace_session())
                await worker.commit()
            return account

        api.get_split_grace_snapshot.side_effect = race
        with pytest.raises(LedgerConflict):
            await capture_grace_context(reader, **target, panel_before=before)


async def test_captured_context_is_persisted_without_recomputing_history(postgres_database):
    from app.services.bs_traffic.grace_capture import capture_grace_context
    from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore

    async with postgres_session(postgres_database, TABLES) as db:
        from sqlalchemy import select

        from app.database.models import BsTrafficLedger
        from app.services.bs_traffic.evidence import collect_topology_evidence
        from app.services.bs_traffic.policy import configure_groups
        from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

        api, reader, target, before = await prepare(db)
        evidence = await collect_topology_evidence(
            api, panel_identity=SCOPE, ordinary_squads=(ORDINARY,), bs_squads=(BS,)
        )
        revision = await db.scalar(select(BsTrafficLedger.revision).where(BsTrafficLedger.subscription_id == 1))
        await configure_groups(
            db, **target, ordinary_mode='finite', bs_mode='finite', evidence=evidence, expected_revision=revision
        )
        await db.commit()
        captured = await capture_grace_context(reader, **target, panel_before=before)
        source = replace(grace_session(), panel_before=before, split_before=captured)
        saved = await SQLAlchemyGraceSessionStore(db).create(source)
        await db.commit()
        assert saved.split_before == captured
        assert (await SQLAlchemyGraceSessionStore(db).get_open(1)).split_before == captured


@pytest.mark.parametrize('change', ['status', 'expiry', 'quota', 'external', 'reset', 'counter', 'unknown'])
async def test_capture_rejects_changed_full_pre_grace_state(postgres_database, change):
    from datetime import timedelta

    from app.services.bs_traffic.grace_capture import capture_grace_context

    async with postgres_session(postgres_database, TABLES) as db:
        api, reader, target, before = await prepare(db)
        current = api.get_split_grace_snapshot.return_value
        changes = {
            'status': {'status': 'DISABLED'},
            'expiry': {'expire_at': current.expire_at + timedelta(seconds=1)},
            'quota': {'traffic_limit_bytes': current.traffic_limit_bytes + 1},
            'external': {'external_squad_uuid': '99999999-9999-4999-8999-999999999999'},
            'reset': {'last_traffic_reset_at': current.expire_at},
            'counter': {'used_traffic_bytes': current.used_traffic_bytes - 1},
        }
        if change == 'unknown':
            before = replace(before, traffic_is_known=False)
        else:
            api.get_split_grace_snapshot.return_value = replace(current, **changes[change])
        with pytest.raises(LedgerConflict):
            await capture_grace_context(reader, **target, panel_before=before)


async def test_normal_usage_growth_does_not_invalidate_same_pre_grace_source(postgres_database):
    from app.services.bs_traffic.grace_capture import capture_grace_context

    async with postgres_session(postgres_database, TABLES) as db:
        api, reader, target, before = await prepare(db)
        current = api.get_split_grace_snapshot.return_value
        api.get_split_grace_snapshot.return_value = replace(current, used_traffic_bytes=current.used_traffic_bytes + 1)
        captured = await capture_grace_context(reader, **target, panel_before=before)
        assert captured.subscription_id == 1
