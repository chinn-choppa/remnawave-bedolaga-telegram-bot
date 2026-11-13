"""Removed nodes retain collection and cannot consume later paid grants."""

from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficCollectionJob, BsTrafficLedger, BsTrafficUsage
from app.services.bs_traffic import tariff_switch_checkout as checkout
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.intake import settle_reported_observation
from app.services.bs_traffic.observations import begin_poll, record_poll
from app.services.bs_traffic.policy import _topology
from tests.database.test_bs_tariff_switch_topology_postgres import request, topology_setup
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
REMOVED = '77777777-0000-4000-8000-000000000001'


async def removal_setup(db, monkeypatch, group='ordinary'):
    reader, owner, sub, tariff, remote = await topology_setup(db, monkeypatch)
    original = reader._api.get_internal_squad_topology.side_effect

    async def topology(squad):
        doc = await original(squad)
        if squad == (ORDINARY if group == 'ordinary' else BS):
            doc = dict(doc, accessibleNodes=[*doc['accessibleNodes'], dict(doc['accessibleNodes'][0], uuid=REMOVED)])
        return doc

    reader._api.get_internal_squad_topology.side_effect = topology
    evidence = await collect_topology_evidence(
        reader, panel_identity=reader.panel_scope, ordinary_squads=[ORDINARY], bs_squads=[BS]
    )
    state = await db.get(BsTrafficLedger, sub.id)
    state.access_policy = dict(
        state.access_policy, topology=_topology(evidence.topology), fingerprint=evidence.fingerprint
    )
    from app.database.models import BsTrafficManagedTransfer

    transfer = await db.get(BsTrafficManagedTransfer, sub.id)
    transfer.calendar_terms = dict(transfer.calendar_terms, topology=_topology(evidence.topology))
    await db.commit()
    return reader, owner, sub, tariff, remote


@pytest.mark.parametrize('group', ['ordinary', 'bs'])
@pytest.mark.parametrize('write_off', [False, True])
async def test_removal_collects_tail_and_multiple_late_polls_stay_raw(postgres_database, monkeypatch, write_off, group):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, remote = await removal_setup(db, monkeypatch, group)
        from app.config import settings

        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
        args = await request(reader, owner, tariff)
        paid = await checkout.checkout_tariff_switch(reader, **args)
        assert not paid.delivery_pending
        from app.database.models import BsTrafficNodeMembership
        from app.services.bs_traffic.node_tail import settle_tails
        from app.services.bs_traffic.node_tail_collection import enqueue_due_tails

        expiry = sub.end_date
        db.expire_all()
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        membership = await db.scalar(
            select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED)
        )
        assert membership.state == 'tail' and membership.transition_receipt_id == paid.intent_id
        assert membership.source_paid_end_at == expiry
        now = await db.scalar(select(func.clock_timestamp()))
        assert await enqueue_due_tails(db, reader, now=now) > 0
        jobs = list(await db.scalars(select(BsTrafficCollectionJob)))
        assert any(REMOVED in job.node_uuids for job in jobs)
        grants = list(await db.scalars(select(BsTrafficAllowance)))
        balances = {g.id: g.remaining_bytes for g in grants}
        for day, amount in [(now.date(), 20), (now.date(), 70), (now.date() + timedelta(days=1), 40)]:
            if amount == 70:
                from app.services.bs_traffic.intake import grant_with_reported_settlement

                topup = await grant_with_reported_settlement(
                    db,
                    **owner,
                    group=group,
                    operation_key='tail:later-paid-grant',
                    amount_bytes=123,
                    starts_at=now,
                    expires_at=expiry,
                )
                balances[topup] = 123
            poll = await begin_poll(
                db,
                panel_scope=state.panel_scope,
                day=day,
                node_uuids=(REMOVED,),
                identities={state.panel_user_id: state.account_generation},
            )
            await record_poll(
                db,
                poll.id,
                {'nodes': [{'uuid': REMOVED, 'users': [{'id': state.panel_user_id, 'totalBytes': amount}]}]},
            )
            await settle_tails(db, state=state, target=owner)
            await settle_reported_observation(db, **owner, node_uuid=REMOVED, day=day)
        usage = list(await db.scalars(select(BsTrafficUsage).where(BsTrafficUsage.source_key.like(f'raw:{REMOVED}:%'))))
        assert sum(u.amount_bytes for u in usage) == 110
        assert all(u.traffic_group == group for u in usage)
        assert all(
            u.evidence_kind == 'raw_pending' and u.state == ('written_off' if write_off else 'pending') for u in usage
        )
        assert {g.id: g.remaining_bytes for g in await db.scalars(select(BsTrafficAllowance))} == balances


async def test_horizon_parks_without_extending_on_renewal_or_forgiving_usage(postgres_database, monkeypatch):
    from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail, Subscription
    from app.services.bs_traffic.node_tail_collection import enqueue_due_tails

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, _ = await removal_setup(db, monkeypatch)
        expiry = sub.end_date
        paid = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        assert not paid.delivery_pending
        db.expire_all()
        sub = await db.get(Subscription, owner['subscription_id'])
        sub.end_date = expiry + timedelta(days=30)
        await db.flush()
        assert await enqueue_due_tails(db, reader, now=expiry) == 0
        member = await db.scalar(select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED))
        task = await db.scalar(select(BsTrafficNodeTail).where(BsTrafficNodeTail.membership_id == member.id))
        assert member.state == task.state == 'attention'
        assert member.source_paid_end_at == expiry
        assert task.last_error == 'horizon_or_owner_changed'
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsage)) == 0


async def test_removed_node_is_in_fleet_jobs_and_readd_requires_current_baseline(postgres_database, monkeypatch):
    from app.database.models import BsTrafficFleetMember, Tariff
    from app.services.bs_traffic.fleet_round import plan_page
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, _ = await removal_setup(db, monkeypatch)
        original_tariff = sub.tariff_id
        args = await request(reader, owner, tariff)
        paid = await checkout.checkout_tariff_switch(reader, **args)
        assert not paid.delivery_pending
        assert (await checkout.checkout_tariff_switch(reader, **args)).replayed
        assert (await plan_page(db, reader)).accounts == 1
        member = await db.scalar(select(BsTrafficFleetMember))
        assert REMOVED in member.nodes
        old = await db.get(Tariff, original_tariff)
        back = await request(reader, owner, old)
        # Fresh target topology can be quoted, but missing explicit current
        # node usage cannot be interpreted as an enrollment counter.
        from app.config import settings

        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
        from unittest.mock import AsyncMock

        reader._api.get_daily_nodes_usage = AsyncMock(return_value={'nodes': []})
        with pytest.raises(LedgerConflict, match='baseline missing'):
            await checkout.checkout_tariff_switch(reader, **back)


async def test_tail_capture_rolls_back_with_payment(postgres_database, monkeypatch):
    from app.database.models import BsTrafficNodeMembership, Transaction, User
    from app.services.bs_traffic import node_membership

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await removal_setup(db, monkeypatch)
        args = await request(reader, owner, tariff)
        before = (await db.get(User, owner['user_id'])).balance_kopeks
        original = node_membership.capture_memberships

        async def fail_after_capture(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError('synthetic rollback')

        monkeypatch.setattr(node_membership, 'capture_memberships', fail_after_capture)
        with pytest.raises(RuntimeError, match='synthetic rollback'):
            await checkout.checkout_tariff_switch(reader, **args)
        db.expire_all()
        assert (await db.get(User, owner['user_id'])).balance_kopeks == before
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficNodeMembership)) == 0


async def test_removal_keeps_preexisting_historical_day_polling(postgres_database, monkeypatch):
    from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail
    from app.services.bs_traffic.intake import attach_cursor

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await removal_setup(db, monkeypatch)
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        yesterday = (await db.scalar(select(func.clock_timestamp()))).date() - timedelta(days=1)
        poll = await begin_poll(
            db,
            panel_scope=state.panel_scope,
            day=yesterday,
            node_uuids=(REMOVED,),
            identities={state.panel_user_id: state.account_generation},
        )
        await record_poll(
            db, poll.id, {'nodes': [{'uuid': REMOVED, 'users': [{'id': state.panel_user_id, 'totalBytes': 100}]}]}
        )
        await attach_cursor(db, **owner, group='ordinary', node_uuid=REMOVED, day=yesterday)
        await db.commit()
        paid = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        assert not paid.delivery_pending
        days = list(
            await db.scalars(
                select(BsTrafficNodeTail.day)
                .join(BsTrafficNodeMembership, BsTrafficNodeMembership.id == BsTrafficNodeTail.membership_id)
                .where(BsTrafficNodeMembership.node_uuid == REMOVED)
            )
        )
        assert yesterday in days


@pytest.mark.parametrize('fault', ['scope', 'generation', 'group', 'receipt'])
async def test_forged_membership_cannot_authorize_removal_delivery(postgres_database, monkeypatch, fault):
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficNodeMembership
    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await removal_setup(db, monkeypatch)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        paid = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        member = await db.scalar(select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED))
        if fault == 'scope':
            member.panel_scope = '00000000-0000-4000-8000-000000000000'
        elif fault == 'generation':
            member.account_generation = 'f' * 64
        elif fault == 'group':
            member.traffic_group = 'bs'
        else:
            member.transition_receipt_id = None
        await db.commit()
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) in (
            'obsolete',
            'unconfirmed',
        )
        reader._api.restore_split_user_once.assert_not_awaited()


async def test_historical_tail_scheduler_shares_node_request_and_enforces_budget(postgres_database, monkeypatch):
    from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail
    from app.services.bs_traffic.fleet_round import enqueue_node, plan_page
    from app.services.bs_traffic.node_tail_collection import enqueue_due_tails

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await removal_setup(db, monkeypatch)
        await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        await plan_page(db, reader)
        for _ in range(10):
            more = await enqueue_node(db, reader)
            await db.flush()
            if not more:
                break
        else:
            pytest.fail('Fleet enqueue did not converge')
        before = await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob))
        now = await db.scalar(select(func.clock_timestamp()))
        assert await enqueue_due_tails(db, reader, now=now) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == before
        member = await db.scalar(select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED))
        task = await db.scalar(select(BsTrafficNodeTail).where(BsTrafficNodeTail.membership_id == member.id))
        assert task.job_id is not None


async def test_node_tail_migration_matches_models(postgres_database):
    from importlib import util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import inspect

    from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail

    async with postgres_session(postgres_database, TABLES) as db:
        connection = await db.connection()

        def migrate(sync_connection):
            BsTrafficNodeTail.__table__.drop(sync_connection)
            BsTrafficNodeMembership.__table__.drop(sync_connection)
            spec = util.spec_from_file_location(
                'node_tail_migration', Path('migrations/alembic/versions/0167_bs_node_tails.py')
            )
            module = util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.op = Operations(MigrationContext.configure(sync_connection))
            module.upgrade()
            spec = util.spec_from_file_location(
                'node_epoch_migration', Path('migrations/alembic/versions/0168_bs_node_epochs.py')
            )
            module = util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.op = Operations(MigrationContext.configure(sync_connection))
            module.upgrade()
            inspector = inspect(sync_connection)
            for model in (BsTrafficNodeMembership, BsTrafficNodeTail):
                assert {c['name'] for c in inspector.get_columns(model.__tablename__)} == set(
                    model.__table__.columns.keys()
                )
                assert {c['name'] for c in inspector.get_check_constraints(model.__tablename__)} == {
                    c.name for c in model.__table__.constraints if c.__class__.__name__ == 'CheckConstraint'
                }

        await connection.run_sync(migrate)


@pytest.mark.parametrize('grace', [False, True])
async def test_removal_receipt_successor_preserves_tail_and_delivery(postgres_database, monkeypatch, grace):
    from app.database.models import BsTrafficNodeMembership, BsTrafficRenewalIntent
    from app.services.bs_traffic.grace_lifecycle import start_split_grace
    from app.services.bs_traffic.renewal import renew_from_balance
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from app.services.grace_access_service import GraceReason, GraceSessionState
    from tests.database import test_bs_topology_grace_postgres as fixtures

    monkeypatch.setattr(fixtures, 'topology_setup', removal_setup)
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, _, _, old, policy = await fixtures.expired_transition(db, monkeypatch)
        original = await db.get(BsTrafficRenewalIntent, old.intent_id)
        assert original.parameters['switch_terms']['topology_transition']['version'] == 3
        member = await db.scalar(select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED))
        horizon = member.source_paid_end_at
        await db.commit()
        if grace:
            started = await start_split_grace(reader, **owner, reason=GraceReason.EXPIRED, policy=policy)
            assert started.session.state is GraceSessionState.ACTIVE
        paid = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=4000, operation_key='removed-successor'
        )
        await db.commit()
        successor = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        if not grace:
            assert successor.parameters['topology_transition']['manifest']['version'] == 4
        assert await advance_paid_renewal(reader, **owner, operation_id=paid.intent_id) == 'verified'
        await db.refresh(member)
        assert member.source_paid_end_at == horizon and member.transition_receipt_id == old.intent_id
        assert member.state == 'tail'
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('budget', ['jobs', 'backpressure', 'days'])
async def test_tail_scheduler_fixed_budgets_preserve_unresolved_tasks(postgres_database, monkeypatch, budget):
    from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail
    from app.services.bs_traffic.node_tail_collection import enqueue_due_tails
    from app.services.bs_traffic.queue import enqueue_job

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await removal_setup(db, monkeypatch)
        await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        member = await db.scalar(select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED))
        now = await db.scalar(select(func.clock_timestamp()))
        count = 127 if budget == 'days' else 7
        for number in range(1, count + 1):
            db.add(
                BsTrafficNodeTail(
                    membership_id=member.id,
                    day=now.date() - timedelta(days=number),
                    state='pending',
                    next_poll_at=now - timedelta(minutes=number),
                )
            )
        await db.flush()
        if budget == 'backpressure':
            for number in range(16):
                await enqueue_job(
                    db,
                    operation_key=f'budget:{number}',
                    panel_scope=member.panel_scope,
                    day=now.date() - timedelta(days=number + 20),
                    node_uuids=(REMOVED,),
                    identities={member.panel_user_id: member.account_generation},
                )
        jobs = await enqueue_due_tails(db, reader, now=now + timedelta(days=1) if budget == 'days' else now)
        if budget == 'jobs':
            assert jobs == 4
            assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 4
            # Later pages rotate to still-due older work, instead of busy-polling leased tasks.
            assert await enqueue_due_tails(db, reader, now=now) == 0
            from sqlalchemy import update

            await db.execute(update(BsTrafficCollectionJob).values(state='complete'))
            assert await enqueue_due_tails(db, reader, now=now + timedelta(seconds=2)) == 4
        elif budget == 'backpressure':
            assert jobs == 0
            assert await db.scalar(select(func.count()).select_from(BsTrafficCollectionJob)) == 16
        else:
            assert jobs == 0 and member.state == 'attention'
            assert await db.scalar(select(func.count()).select_from(BsTrafficNodeTail)) == 128
            assert (
                await db.scalar(
                    select(BsTrafficNodeTail.last_error).where(BsTrafficNodeTail.state == 'attention').limit(1)
                )
                == 'day_budget_exceeded'
            )


async def test_tail_scheduler_shares_at_most_500_owners_per_job(postgres_database, monkeypatch):
    from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail, Subscription
    from app.services.bs_traffic.node_tail_collection import enqueue_due_tails

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await removal_setup(db, monkeypatch)
        await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        original = await db.scalar(select(BsTrafficNodeMembership).where(BsTrafficNodeMembership.node_uuid == REMOVED))
        now = await db.scalar(select(func.clock_timestamp()))
        for number in range(500):
            identifier = 1000 + number
            db.add(
                Subscription(
                    id=identifier,
                    user_id=owner['user_id'],
                    end_date=original.source_paid_end_at,
                    status='expired',
                    remnawave_id=identifier,
                    remnawave_short_id=f'tail-{identifier}',
                )
            )
        await db.flush()
        for number in range(500):
            identifier = 1000 + number
            db.add(
                BsTrafficLedger(
                    subscription_id=identifier,
                    panel_scope=original.panel_scope,
                    panel_user_id=identifier,
                    account_generation=original.account_generation,
                    ownership_cycle=original.ownership_cycle,
                )
            )
        await db.flush()
        for number in range(500):
            identifier = 1000 + number
            member = BsTrafficNodeMembership(
                subscription_id=identifier,
                panel_scope=original.panel_scope,
                panel_user_id=identifier,
                account_generation=original.account_generation,
                ownership_cycle=original.ownership_cycle,
                node_uuid=REMOVED,
                traffic_group='ordinary',
                source_fingerprint=original.source_fingerprint,
                source_paid_end_at=original.source_paid_end_at,
                effective_at=now,
                retired_at=now,
                state='tail',
            )
            db.add(member)
            await db.flush()
            db.add(BsTrafficNodeTail(membership_id=member.id, day=now.date(), state='pending', next_poll_at=now))
        await db.flush()
        assert await enqueue_due_tails(db, reader, now=now, limit=1) == 1
        first = await db.scalar(select(BsTrafficCollectionJob))
        assert len(first.identities) == 500
        assert await enqueue_due_tails(db, reader, now=now, limit=1) == 1
        jobs = list(await db.scalars(select(BsTrafficCollectionJob).order_by(BsTrafficCollectionJob.id)))
        assert [len(job.identities) for job in jobs] == [500, 1]


async def test_reenrollment_with_retired_nodes_rolls_back_new_money(postgres_database, monkeypatch):
    from app.database.crud.transaction import create_transaction
    from app.database.crud.user import subtract_user_balance
    from app.database.models import (
        BsTrafficEnrollment,
        BsTrafficNodeMembership,
        PaymentMethod,
        Transaction,
        TransactionType,
    )
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.ledger import LedgerConflict
    from tests.database.test_bs_enrollment_postgres import paid
    from tests.database.test_bs_traffic_ledger_postgres import GENERATION, SCOPE

    async with postgres_session(postgres_database, TABLES) as db:
        user, _, sub, _ = await paid(db, monkeypatch)
        sub.traffic_accounting_mode = 'managed'
        db.add(
            BsTrafficLedger(subscription_id=sub.id, panel_scope=SCOPE, panel_user_id=12, account_generation=GENERATION)
        )
        await db.flush()
        now = await db.scalar(select(func.clock_timestamp()))
        db.add(
            BsTrafficNodeMembership(
                subscription_id=sub.id,
                panel_scope=SCOPE,
                panel_user_id=12,
                account_generation=GENERATION,
                ownership_cycle=0,
                node_uuid=REMOVED,
                traffic_group='ordinary',
                source_fingerprint='a' * 64,
                effective_at=now,
                retired_at=now,
                source_paid_end_at=sub.end_date,
                state='attention',
            )
        )
        await db.commit()
        with pytest.raises(LedgerConflict, match='Historical node re-entry'):
            async with db.begin_nested():
                assert await subtract_user_balance(db, user, 2000, 'synthetic re-entry', commit=False)
                tx = await create_transaction(
                    db,
                    user_id=user.id,
                    type=TransactionType.SUBSCRIPTION_PAYMENT,
                    amount_kopeks=2000,
                    description='synthetic re-entry',
                    payment_method=PaymentMethod.BALANCE,
                    external_id='tail:reentry',
                    commit=False,
                )
                await stage_paid_enrollment(db, sub, transaction=tx)
        await db.commit()
        await db.refresh(user)
        assert user.balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 0
