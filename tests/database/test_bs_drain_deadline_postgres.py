"""Paid BS horizon is frozen independently of later ordinary renewals."""

from datetime import datetime, timedelta

import pytest

from app.database.models import BsTrafficAllowance, BsTrafficLedger, Subscription
from app.services.bs_traffic.policy import configure_groups, set_bs_purchase_enabled
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('mode', ['finite', 'unlimited'])
async def test_paid_horizon_survives_ordinary_renewal_and_policy_refresh(postgres_database, monkeypatch, mode):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        await configure_groups(
            db,
            **target,
            ordinary_mode='finite',
            bs_mode=mode,
            evidence=args['evidence'],
            expected_revision=state.revision,
        )
        paid_end = sub.end_date
        await set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=state.revision)
        await db.commit()
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == paid_end
        sub.end_date += timedelta(days=365)
        await db.commit()
        await set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=state.revision)
        await configure_groups(
            db,
            **target,
            ordinary_mode='finite',
            bs_mode=mode,
            evidence=args['evidence'],
            expected_revision=state.revision,
        )
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == paid_end
        await set_bs_purchase_enabled(db, **target, enabled=True, expected_revision=state.revision)
        assert 'bs_drain_until' not in state.access_policy


async def test_longer_paid_bs_package_extends_snapshot_but_not_ordinary_package(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        for group, days in [('bs', 10), ('ordinary', 100)]:
            db.add(
                BsTrafficAllowance(
                    subscription_id=sub.id,
                    traffic_group=group,
                    operation_key=f'test-{group}',
                    granted_bytes=100,
                    remaining_bytes=100,
                    starts_at=sub.end_date - timedelta(days=1),
                    expires_at=sub.end_date + timedelta(days=days),
                )
            )
        await db.flush()
        await set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=state.revision)
        assert datetime.fromisoformat(state.access_policy['bs_drain_until']) == sub.end_date + timedelta(days=10)


async def test_expired_unlimited_bs_does_not_follow_renewed_ordinary_access(postgres_database, monkeypatch):
    from datetime import UTC

    from app.services.bs_traffic.customer_summary import customer_summary
    from app.services.bs_traffic.policy import project_access

    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, _ = await prepared_purchase(db, monkeypatch)
        state = await db.get(BsTrafficLedger, target['subscription_id'])
        await configure_groups(
            db,
            **target,
            ordinary_mode='unlimited',
            bs_mode='unlimited',
            evidence=args['evidence'],
            expected_revision=state.revision,
        )
        await set_bs_purchase_enabled(db, **target, enabled=False, expected_revision=state.revision)
        state.access_policy = dict(
            state.access_policy, bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        )
        await db.flush()
        projected = await project_access(db, **target, evidence=args['evidence'], grace_active=False)
        assert projected.ordinary.state == 'active'
        assert projected.bs.state == 'disabled'
        assert projected.bs.squads == ()
        assert projected.grace_candidate is None
        view = await customer_summary(db, user_id=target['user_id'], subscription_id=target['subscription_id'])
        assert view['groups']['ordinary']['mode'] == 'unlimited'
        assert view['groups']['bs']['mode'] == 'disabled'
