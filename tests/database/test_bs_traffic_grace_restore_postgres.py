from dataclasses import replace

import pytest
from sqlalchemy import select

from app.database.models import Subscription, User
from app.services.bs_traffic.grace_restore import plan_grace_restore
from app.services.bs_traffic.ledger import summary
from app.services.bs_traffic.policy import project_access
from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore
from tests.database.test_bs_traffic_grace_transition_postgres import grace_session
from tests.database.test_bs_traffic_ledger_postgres import NOW, TARGET, issue, resolve, usage
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY, configure, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
OVERLAY = '99999999-9999-4999-8999-999999999999'


@pytest.mark.parametrize(
    'ordinary_mode,bs_mode,ordinary_bytes,bs_bytes,expected',
    [
        ('unlimited', 'finite', 0, 0, (ORDINARY,)),
        ('finite', 'unlimited', 0, 0, (BS,)),
        ('finite', 'finite', 100, 0, (ORDINARY,)),
        ('finite', 'finite', 0, 100, (BS,)),
        ('finite', 'finite', 100, 100, (ORDINARY, BS)),
        ('finite', 'finite', 0, 0, ()),
        ('disabled', 'unlimited', 0, 0, (BS,)),
        ('unlimited', 'disabled', 0, 0, (ORDINARY,)),
    ],
)
async def test_paid_restore_plan_uses_independent_balances_without_closing_grace(
    postgres_database, ordinary_mode, bs_mode, ordinary_bytes, bs_bytes, expected
):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        if ordinary_bytes:
            await issue(db, group='ordinary', amount=ordinary_bytes)
        if bs_bytes:
            await issue(db, amount=bs_bytes)
        evidence = await configure(db, ordinary=ordinary_mode, bs=bs_mode)
        store = SQLAlchemyGraceSessionStore(db)
        session = grace_session()
        await store.create(replace(session, overlay=replace(session.overlay, squad_uuids=(OVERLAY,))))
        before = await summary(db, **TARGET, now=NOW)
        # Explicit paid projection for the future grace-exit coordinator only.
        projected = await project_access(db, **TARGET, evidence=evidence, grace_active=False)
        result = plan_grace_restore(
            projected,
            topology=evidence.topology,
            current_squads=(OVERLAY,),
            overlay_squads=(OVERLAY,),
            paid_before_squads=(ORDINARY, BS),
            paid_before_fingerprint=evidence.fingerprint,
        )
        assert result.action == 'patch' and result.squads == expected
        assert await summary(db, **TARGET, now=NOW) == before
        assert await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == 1)) is True
        assert await store.get_open(1) is not None


async def test_pending_bs_restore_does_not_copy_grace_squad_or_debit_new_purchase(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        evidence = await configure(db, ordinary='unlimited')
        await issue(db, key='old-purchase', amount=100)
        old_usage = await usage(db, amount=150)
        await issue(db, key='new-purchase', amount=200)
        before = await summary(db, **TARGET, now=NOW)
        projected = await project_access(db, **TARGET, evidence=evidence, grace_active=False)
        assert projected.bs.state == 'active'
        result = plan_grace_restore(
            projected,
            topology=evidence.topology,
            current_squads=(OVERLAY,),
            overlay_squads=(OVERLAY,),
            paid_before_squads=(ORDINARY,),
            paid_before_fingerprint=evidence.fingerprint,
        )
        assert result.squads == (ORDINARY, BS)
        assert await summary(db, **TARGET, now=NOW) == before
        assert before['bs']['remaining_bytes'] == 300
        assert before['bs']['unencumbered_bytes'] == 200
        await resolve(db, old_usage)
        after = await summary(db, **TARGET, now=NOW)
        assert after['bs']['remaining_bytes'] == 200
        assert after['bs']['pending_operations'] == 0
