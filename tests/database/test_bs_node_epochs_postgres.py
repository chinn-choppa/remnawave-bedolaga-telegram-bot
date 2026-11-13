"""Node additions and changed group epochs retain measured boundary provenance."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficLedger,
    BsTrafficNodeMembership,
    BsTrafficUsage,
    BsTrafficUsageCursor,
)
from app.services.bs_traffic import tariff_switch_checkout as checkout
from app.services.bs_traffic.intake import settle_reported_observation
from tests.database.test_bs_tariff_switch_modes_postgres import mode_observation
from tests.database.test_bs_tariff_switch_topology_postgres import NEW_BS, NEW_ORDINARY, request, topology_setup
from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
ADDED = '77777777-0000-4000-8000-000000000009'


async def changed_nodes(db, monkeypatch, kind):
    reader, owner, sub, tariff, remote = await topology_setup(db, monkeypatch)
    original = reader._api.get_internal_squad_topology.side_effect

    async def topology(squad):
        doc = await original(squad)
        if squad == NEW_ORDINARY:
            doc = dict(
                doc,
                accessibleNodes=[dict(doc['accessibleNodes'][0], uuid=ADDED)]
                if kind == 'add'
                else [dict(doc['accessibleNodes'][0], uuid=NODE_B)],
            )
        if squad == NEW_BS and kind == 'move':
            doc = dict(doc, accessibleNodes=[dict(doc['accessibleNodes'][0], uuid=NODE_A)])
        return doc

    reader._api.get_internal_squad_topology.side_effect = topology

    ledger = await db.get(BsTrafficLedger, sub.id)
    panel_id = ledger.panel_user_id

    async def usage_response(node_uuids, day, **kwargs):
        return {
            'nodes': [
                {
                    'uuid': node,
                    'users': [{'id': panel_id, 'totalBytes': 500 if kind == 'add' else 100 if node == NODE_A else 0}],
                }
                for node in node_uuids
            ]
        }

    reader._api.get_daily_nodes_usage = AsyncMock(side_effect=usage_response)
    await db.commit()
    return reader, owner, sub, tariff, remote


@pytest.mark.parametrize('kind', ['add', 'move'])
@pytest.mark.parametrize('writeoff', [False, True])
async def test_changed_node_boundary_is_raw_then_fresh_delta_charges_target_epoch(
    postgres_database, monkeypatch, kind, writeoff
):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, _ = await changed_nodes(db, monkeypatch, kind)
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', writeoff)
        args = await request(reader, owner, tariff)
        paid = await checkout.checkout_tariff_switch(reader, **args)
        assert not paid.delivery_pending
        db.expire_all()
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        node = ADDED if kind == 'add' else NODE_B
        day = (await db.scalar(select(func.clock_timestamp()))).date()
        cursor = await db.get(BsTrafficUsageCursor, (owner['subscription_id'], node, day))
        baseline = 500 if kind == 'add' else 0
        assert cursor.baseline_bytes == baseline and cursor.traffic_group == 'ordinary'
        base = await db.get(BsTrafficAllowance, paid.grants['ordinary'])
        initial = base.remaining_bytes
        for amount in (baseline + 30, baseline + 50):
            now = await db.scalar(select(func.clock_timestamp()))
            await mode_observation(db, state, node, now, amount)
            await settle_reported_observation(db, **owner, node_uuid=node, day=day)
        await db.refresh(base)
        assert base.remaining_bytes == initial - 20
        events = list(
            await db.scalars(
                select(BsTrafficUsage).where(BsTrafficUsage.source_key.like(f'%:{node}:%')).order_by(BsTrafficUsage.id)
            )
        )
        assert [(event.amount_bytes, event.evidence_kind, event.state) for event in events] == [
            (30, 'raw_pending', 'written_off' if writeoff else 'pending'),
            (20, 'reported', 'charged'),
        ]
        epochs = list(
            await db.scalars(
                select(BsTrafficNodeMembership)
                .where(BsTrafficNodeMembership.node_uuid == node)
                .order_by(BsTrafficNodeMembership.id)
            )
        )
        assert len(epochs) == (1 if kind == 'add' else 2)
        assert epochs[-1].state == 'active' and epochs[-1].entry_evidence['baseline_bytes'] == baseline
        if kind == 'move':
            assert epochs[0].state == 'tail' and epochs[0].traffic_group == 'bs'
        await db.commit()
        assert (await checkout.checkout_tariff_switch(reader, **args)).replayed


async def test_addition_combined_calendar_and_group_mode_delivers_then_refreshes(postgres_database, monkeypatch):
    from datetime import datetime

    from app.database.models import BsTrafficRenewalIntent
    from app.services.bs_traffic.calendar_renewal import refresh_calendar_base

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, remote = await changed_nodes(db, monkeypatch, 'add')
        from dataclasses import replace

        from app.services.bs_traffic.renewal import renew_from_balance

        await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='epoch:calendar-seed'
        )
        remote['state'] = replace(remote['state'], expire_at=sub.end_date)
        tariff.traffic_reset_mode = 'DAY'
        tariff.bs_traffic_settings = dict(tariff.bs_traffic_settings, limit_gb=0, topup_enabled=False)
        await db.commit()
        paid = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        assert not paid.delivery_pending
        db.expire_all()
        receipt = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        assert set(paid.grants) == {'ordinary'}
        assert receipt.parameters['mode_boundary']['ordinary']['nodes'][0]['baseline_bytes'] == 500
        due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        renewed = await refresh_calendar_base(db, **owner, now=due)
        assert set(renewed.grants) == {'ordinary'}
        assert (await db.get(BsTrafficAllowance, renewed.grants['ordinary'])).remaining_bytes == 20 * 1024**3


async def test_readded_node_creates_new_epoch_and_tail_does_not_consume_current_bucket(postgres_database, monkeypatch):
    from app.database.models import Tariff
    from app.services.bs_traffic.node_tail import settle_tails

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, _ = await changed_nodes(db, monkeypatch, 'add')
        old_id = sub.tariff_id
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
        first = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        assert not first.delivery_pending
        db.expire_all()
        old = await db.get(Tariff, old_id)
        back = await checkout.checkout_tariff_switch(reader, **await request(reader, owner, old))
        assert not back.delivery_pending
        await db.rollback()
        db.expire_all()
        epochs = list(
            await db.scalars(
                select(BsTrafficNodeMembership)
                .where(BsTrafficNodeMembership.node_uuid == NODE_A)
                .order_by(BsTrafficNodeMembership.id)
            )
        )
        assert [(epoch.state, epoch.traffic_group) for epoch in epochs] == [
            ('tail', 'ordinary'),
            ('active', 'ordinary'),
        ]
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        now = await db.scalar(select(func.clock_timestamp()))
        base = await db.get(BsTrafficAllowance, back.grants['ordinary'])
        before = base.remaining_bytes
        await mode_observation(db, state, NODE_A, now, 540)
        assert await settle_tails(db, state=state, target=owner)
        boundary_id = await settle_reported_observation(db, **owner, node_uuid=NODE_A, day=now.date())
        assert (await db.get(BsTrafficUsage, boundary_id)).evidence_kind == 'raw_pending'
        now = await db.scalar(select(func.clock_timestamp()))
        await mode_observation(db, state, NODE_A, now, 555)
        await settle_reported_observation(db, **owner, node_uuid=NODE_A, day=now.date())
        assert base.remaining_bytes == before - 15
        await db.commit()
        await db.refresh(tariff)
        assert (
            await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        ).delivery_pending is False
        db.expire_all()
        epochs = list(
            await db.scalars(
                select(BsTrafficNodeMembership)
                .where(BsTrafficNodeMembership.node_uuid == NODE_A)
                .order_by(BsTrafficNodeMembership.id)
            )
        )
        assert [epoch.state for epoch in epochs] == ['tail', 'tail']


@pytest.mark.parametrize(
    ('writeoff', 'late_unknown', 'new_topology'),
    [(False, False, False), (True, False, False), (True, True, False), (True, False, True)],
)
async def test_paid_reenrollment_with_historical_node_adopts_fresh_epoch(
    postgres_database, monkeypatch, writeoff, late_unknown, new_topology
):
    from datetime import timedelta

    from app.database.crud.transaction import create_transaction
    from app.database.crud.user import subtract_user_balance
    from app.database.models import BsTrafficEnrollment, PaymentMethod, Subscription, TransactionType, User
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_delivery import _transfer, advance_enrollment
    from app.services.bs_traffic.enrollment_source import prepare_ordinary_source
    from app.services.bs_traffic.reverse_delivery import advance_reverse
    from app.services.traffic_accounting_sync import stage_managed_sync
    from tests.database.test_bs_enrollment_postgres import ORDINARY, bound_panel, paid
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe

    async with postgres_session(postgres_database, TABLES) as db:
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
        user, tariff, sub, tx = await paid(db, monkeypatch)
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        reader, remote = await bound_panel(db, sub, monkeypatch)
        reader._factory.configure(autoflush=False)
        reader._api.return_split_to_managed_once = reader._api.restore_split_user_once
        reader._api.get_system_configuration = AsyncMock(
            return_value={
                'service': {'disableUserUsageRecords': False, 'cleanUsageHistory': False},
                'misc': {'userUsageIgnoreBelowBytes': 0},
            }
        )
        reader._api.get_internal_squad_topology = AsyncMock(
            side_effect=lambda squad: {
                'squadUuid': squad,
                'accessibleNodes': [
                    {
                        'uuid': NODE_A if squad == ORDINARY else NODE_B,
                        'configProfileUuid': '66666666-6666-4666-8666-666666666666',
                        'activeInbounds': ['ordinary' if squad == ORDINARY else 'bs'],
                    }
                ],
            }
        )
        await prepare_ordinary_source(reader, job.id)
        await _transfer(reader, job.id)
        await db.refresh(job)
        owner = dict(user_id=user.id, subscription_id=sub.id, account_generation=job.account_generation)
        for node in (NODE_A, NODE_B):
            await observe(db, reader, owner, node=node, amount=0)
        await db.commit()
        assert await advance_enrollment(reader, enrollment_id=job.id) == 'verified'
        await db.rollback()
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        now = await db.scalar(select(func.clock_timestamp()))
        state.access_policy = dict(
            state.access_policy, bs_purchase_enabled=False, bs_drain_until=(now - timedelta(seconds=1)).isoformat()
        )
        old_cutoff = (now - timedelta(days=2)).isoformat()
        if new_topology:
            state.access_policy = dict(state.access_policy, finite_since={'ordinary': old_cutoff})
        for grant in await db.scalars(select(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'bs')):
            grant.starts_at = now - timedelta(days=1)
            grant.expires_at = now - timedelta(seconds=1)
        db.add(
            BsTrafficNodeMembership(
                subscription_id=state.subscription_id,
                panel_scope=state.panel_scope,
                panel_user_id=state.panel_user_id,
                account_generation=state.account_generation,
                ownership_cycle=0,
                node_uuid=NODE_A,
                traffic_group='ordinary',
                source_fingerprint=state.access_policy['fingerprint'],
                effective_at=now - timedelta(hours=2),
                retired_at=now - timedelta(hours=1),
                source_paid_end_at=now - timedelta(seconds=1),
                state='attention',
            )
        )
        db.add(
            BsTrafficNodeMembership(
                subscription_id=state.subscription_id,
                panel_scope=state.panel_scope,
                panel_user_id=state.panel_user_id,
                account_generation=state.account_generation,
                ownership_cycle=0,
                node_uuid=NODE_B,
                traffic_group='bs',
                source_fingerprint=state.access_policy['fingerprint'],
                effective_at=now - timedelta(hours=2),
                source_paid_end_at=now - timedelta(seconds=1),
                state='active',
            )
        )
        if late_unknown:
            from app.services.bs_traffic.intake import attach_cursor

            db.add(
                BsTrafficNodeMembership(
                    subscription_id=state.subscription_id,
                    panel_scope=state.panel_scope,
                    panel_user_id=state.panel_user_id,
                    account_generation=state.account_generation,
                    ownership_cycle=0,
                    node_uuid=ADDED,
                    traffic_group='ordinary',
                    source_fingerprint=state.access_policy['fingerprint'],
                    effective_at=now - timedelta(hours=2),
                    retired_at=now - timedelta(hours=1),
                    source_paid_end_at=now - timedelta(seconds=1),
                    state='attention',
                )
            )
            await observe(db, reader, owner, node=ADDED, amount=0)
            await attach_cursor(db, **owner, group='ordinary', node_uuid=ADDED, day=now.date())
        await db.commit()
        assert await advance_reverse(reader, **owner) == 'managed'
        if new_topology:
            from app.database.models import BsTrafficOwnershipCycle

            historical = await db.get(BsTrafficOwnershipCycle, (owner['subscription_id'], 0))
            assert historical.reverse_manifest['policy']['finite_since'] == {'ordinary': old_cutoff}
            original_topology = reader._api.get_internal_squad_topology.side_effect

            def changed_topology(squad):
                document = original_topology(squad)
                if squad == ORDINARY:
                    document = dict(document, accessibleNodes=[dict(document['accessibleNodes'][0], uuid=ADDED)])
                return document

            reader._api.get_internal_squad_topology.side_effect = changed_topology
        await db.rollback()
        sub = await db.get(Subscription, owner['subscription_id'])
        user = await db.get(User, owner['user_id'])
        assert await subtract_user_balance(db, user, 2000, 'Synthetic reentry', commit=False)
        tx = await create_transaction(
            db,
            user_id=user.id,
            type=TransactionType.SUBSCRIPTION_PAYMENT,
            amount_kopeks=2000,
            description='Synthetic reentry',
            payment_method=PaymentMethod.BALANCE,
            external_id='epoch:reentry',
            commit=False,
        )
        sub.end_date += timedelta(days=30)
        await stage_managed_sync(db, sub, only_fields={'expire_at', 'status'})
        job2 = await stage_paid_enrollment(db, sub, transaction=tx)
        assert job2.terms['node_history']['count'] == (3 if late_unknown else 2)
        await db.commit()
        await prepare_ordinary_source(reader, job2.id)
        await _transfer(reader, job2.id)
        await db.rollback()
        ordinary_node = ADDED if new_topology else NODE_A
        for node in (ordinary_node, NODE_B):
            await observe(db, reader, owner, node=node, amount=40)
        await db.commit()
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', writeoff)
        restore_start = reader._api.restore_split_user_once.await_count
        if late_unknown:
            original_restore = reader._api.restore_split_user_once.side_effect
            original_get = reader._api.get_split_grace_snapshot.side_effect

            async def uncertain(target):
                await original_restore(target)
                remote['fail_read_once'] = True
                raise TimeoutError

            def read_later(panel_id):
                if remote.pop('fail_read_once', False):
                    raise TimeoutError
                return original_get(panel_id)

            reader._api.restore_split_user_once.side_effect = uncertain
            reader._api.get_split_grace_snapshot.side_effect = read_later
        result = await advance_enrollment(reader, enrollment_id=job2.id)
        if late_unknown:
            assert result == 'unconfirmed'
            # A queued poll for an old retired-only node completes after the
            # cutover PATCH; it changes raw evidence, not the frozen ledger.
            await observe(db, reader, owner, node=ADDED, amount=70)
            await db.commit()
            result = await advance_enrollment(reader, enrollment_id=job2.id)
            assert result == 'verified'
            assert reader._api.restore_split_user_once.await_count == restore_start + 1
            assert (
                await db.scalar(select(BsTrafficUsage.id).where(BsTrafficUsage.source_key.like(f'raw:{ADDED}:%')))
                is None
            )

        if not writeoff:
            # Existing transfer policy waits for explicit operator acceptance;
            # paid receipt and ordinary rights remain durable throughout.
            assert result == 'blocked'
            monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
            result = await advance_enrollment(reader, enrollment_id=job2.id)
        assert result == 'verified'
        assert await advance_enrollment(reader, enrollment_id=job2.id) == 'already_verified'
        await db.rollback()
        state = await db.get(BsTrafficLedger, owner['subscription_id'], populate_existing=True)
        assert state.ownership_cycle == 1
        assert state.access_policy['bs_purchase_enabled'] is True
        assert 'bs_drain_until' not in state.access_policy
        from app.services.bs_traffic.evidence import collect_topology_evidence
        from app.services.bs_traffic.policy import project_access

        current_evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=state.access_policy['topology']['ordinary_squads'],
            bs_squads=state.access_policy['topology']['bs_squads'],
        )
        assert (await project_access(db, **owner, evidence=current_evidence, grace_active=False)).bs.state == 'active'
        if new_topology:
            assert 'finite_since' not in state.access_policy
        member = await db.scalar(
            select(BsTrafficNodeMembership).where(
                BsTrafficNodeMembership.node_uuid == ordinary_node, BsTrafficNodeMembership.state == 'active'
            )
        )
        assert member.ownership_cycle == 1 and member.entry_evidence['baseline_bytes'] == 40
        archived = await db.scalar(
            select(BsTrafficNodeMembership).where(
                BsTrafficNodeMembership.node_uuid == NODE_B, BsTrafficNodeMembership.ownership_cycle == 0
            )
        )
        assert archived.state == 'archived'
        assert await db.scalar(
            select(func.count())
            .select_from(BsTrafficNodeMembership)
            .where(BsTrafficNodeMembership.state == 'attention')
        ) == (2 if late_unknown else 1)
        old = await db.scalar(
            select(BsTrafficUsage).where(
                BsTrafficUsage.ownership_cycle == 0, BsTrafficUsage.evidence_kind == 'raw_pending'
            )
        )
        assert old is not None and old.amount_bytes == 40 and old.charged_bytes == 0
        assert old.state == 'written_off'
        base = await db.scalar(
            select(BsTrafficAllowance).where(
                BsTrafficAllowance.ownership_cycle == 1,
                BsTrafficAllowance.traffic_group == 'ordinary',
                BsTrafficAllowance.retired_at.is_(None),
            )
        )
        before = base.remaining_bytes
        await db.commit()
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', writeoff)
        for amount in (55, 65):
            await observe(db, reader, owner, node=ordinary_node, amount=amount)
            await settle_reported_observation(
                db, **owner, node_uuid=ordinary_node, day=(await db.scalar(select(func.clock_timestamp()))).date()
            )
        assert base.remaining_bytes == before - 10
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 2


async def test_daily_node_change_keeps_funded_day_and_prorated_full_rate(postgres_database, monkeypatch):
    from uuid import uuid4

    from app.services.bs_traffic.daily_funding import resolve_daily_funding
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from app.services.bs_traffic.tariff_switch import quote_tariff_switch, switch_tariff_from_balance
    from tests.database.test_bs_daily_switch_postgres import setup_daily
    from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _, tariff, evidence = await setup_daily(db, monkeypatch)
        end, charged_at = sub.end_date, sub.last_daily_charge_at
        tariff.allowed_squads = [NEW_ORDINARY]
        original = reader._api.get_internal_squad_topology.side_effect

        async def topology(squad):
            document = await original(ORDINARY if squad == NEW_ORDINARY else squad)
            if squad == NEW_ORDINARY:
                document = dict(
                    document, squadUuid=squad, accessibleNodes=[dict(document['accessibleNodes'][0], uuid=ADDED)]
                )
            return document

        reader._api.get_internal_squad_topology.side_effect = topology
        target = await collect_topology_evidence(
            reader, panel_identity=reader.panel_scope, ordinary_squads=[NEW_ORDINARY], bs_squads=[BS]
        )
        state = await db.get(BsTrafficLedger, sub.id)
        await mode_observation(db, state, ADDED, await db.scalar(select(func.clock_timestamp())), 500)
        await db.flush()
        quote = await quote_tariff_switch(
            db, **owner, new_tariff_id=tariff.id, evidence=evidence, target_evidence=target, current_squads=(ORDINARY,)
        )
        assert quote.price_kopeks == 49
        args = dict(
            **owner,
            new_tariff_id=tariff.id,
            evidence=evidence,
            target_evidence=target,
            current_squads=(ORDINARY,),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        result = await switch_tariff_from_balance(db, **args)
        assert sub.end_date == end and sub.last_daily_charge_at == charged_at
        funding = await resolve_daily_funding(db, state=state, sub=sub)
        assert funding.full_rate_kopeks == 150
        await db.commit()
        assert (await switch_tariff_from_balance(db, **args)).replayed
        assert result.charged_kopeks == 49


async def test_repeated_successor_keeps_actual_membership_root(postgres_database, monkeypatch):
    from tests.database import test_bs_topology_grace_postgres as grace
    from tests.database.test_bs_node_tail_postgres import removal_setup
    from tests.database.test_bs_topology_renewal_guard_postgres import (
        test_repeated_expired_unsent_successors_keep_flat_removal_proof,
    )

    monkeypatch.setattr(grace, 'topology_setup', removal_setup)
    await test_repeated_expired_unsent_successors_keep_flat_removal_proof(postgres_database, monkeypatch)


async def test_old_day_uses_epoch_that_existed_before_later_group_move(postgres_database, monkeypatch):
    from datetime import timedelta

    from app.services.bs_traffic.node_membership import retired_membership
    from app.services.bs_traffic.node_tail import stage_tail

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await changed_nodes(db, monkeypatch, 'move')
        assert not (
            await checkout.checkout_tariff_switch(reader, **await request(reader, owner, tariff))
        ).delivery_pending
        db.expire_all()
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        now = await db.scalar(select(func.clock_timestamp()))
        epochs = list(
            await db.scalars(
                select(BsTrafficNodeMembership)
                .where(BsTrafficNodeMembership.node_uuid == NODE_B)
                .order_by(BsTrafficNodeMembership.id)
            )
        )
        epochs[0].effective_at, epochs[0].retired_at = now - timedelta(days=4), now - timedelta(days=2)
        epochs[1].effective_at, epochs[1].retired_at, epochs[1].state = now - timedelta(days=1), now, 'tail'
        await db.flush()
        old_day = (now - timedelta(days=3)).date()
        selected = await retired_membership(db, state, NODE_B, old_day)
        assert selected.id == epochs[0].id and selected.traffic_group == 'bs'
        await mode_observation(db, state, NODE_B, now - timedelta(days=3), 70)
        identifier = await stage_tail(db, state=state, target=owner, membership=selected, day=old_day)
        event = await db.get(BsTrafficUsage, identifier)
        assert event.traffic_group == 'bs' and event.evidence_kind == 'raw_pending' and event.charged_bytes == 0


async def test_epoch_migration_preserves_history_and_only_one_active_owner(postgres_database, monkeypatch):
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError

    from tests.database.test_bs_tariff_switch_postgres import setup_switch

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, _ = await setup_switch(db, monkeypatch)
        state = await db.get(BsTrafficLedger, sub.id)
        now = await db.scalar(select(func.clock_timestamp()))
        values = dict(
            subscription_id=sub.id,
            panel_scope=state.panel_scope,
            panel_user_id=state.panel_user_id,
            account_generation=state.account_generation,
            ownership_cycle=0,
            node_uuid=ADDED,
            traffic_group='ordinary',
            source_fingerprint=state.access_policy['fingerprint'],
            effective_at=now,
            source_paid_end_at=sub.end_date,
        )
        member = BsTrafficNodeMembership(**values, state='tail', retired_at=now)
        db.add(member)
        await db.commit()
        member_id = member.id
        async with db.bind.begin() as connection:
            await connection.execute(text('DROP INDEX uq_bs_node_active_membership'))
            await connection.execute(text('ALTER TABLE bs_traffic_node_memberships DROP COLUMN entry_evidence'))
            await connection.execute(
                text(
                    'ALTER TABLE bs_traffic_node_memberships ADD CONSTRAINT uq_bs_node_membership UNIQUE(subscription_id,node_uuid)'
                )
            )
            path = Path('migrations/alembic/versions/0168_bs_node_epochs.py')
            spec = spec_from_file_location('epoch_migration', path)
            migration = module_from_spec(spec)
            spec.loader.exec_module(migration)

            def upgrade(sync):
                with Operations.context(MigrationContext.configure(sync)):
                    migration.upgrade()

            await connection.run_sync(upgrade)
        await db.refresh(member)
        assert member.id == member_id and member.state == 'tail' and member.entry_evidence is None
        member.state = 'archived'
        db.add_all(
            [
                BsTrafficNodeMembership(**values, state='tail', retired_at=now),
                BsTrafficNodeMembership(**values, state='active'),
            ]
        )
        await db.commit()
        with pytest.raises(IntegrityError):
            async with db.begin_nested():
                db.add(BsTrafficNodeMembership(**values, state='active'))
                await db.flush()
        assert await db.scalar(select(func.count()).select_from(BsTrafficNodeMembership)) == 3


async def test_reentry_reconciles_large_history_in_committed_bounded_pages(postgres_database, monkeypatch):
    from datetime import timedelta
    from types import SimpleNamespace

    from app.services.bs_traffic.node_reentry import stage_enrollment_history
    from tests.database.test_bs_tariff_switch_postgres import setup_switch

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _, _ = await setup_switch(db, monkeypatch)
        state = await db.get(BsTrafficLedger, owner['subscription_id'])
        state.ownership_cycle = 1
        now = await db.scalar(select(func.clock_timestamp()))
        for number in range(201):
            db.add(
                BsTrafficUsage(
                    subscription_id=state.subscription_id,
                    traffic_group='ordinary',
                    source_key=f'raw:historical:{number}',
                    request_hash='a' * 64,
                    ownership_cycle=0,
                    account_generation=state.account_generation,
                    evidence_kind='raw_pending',
                    interval_start=now - timedelta(days=1),
                    interval_end=now,
                    amount_bytes=1,
                    eligible_grants=[],
                    allocations=[],
                    state='pending',
                    charged_bytes=0,
                    unallocated_bytes=0,
                )
            )
        await db.commit()
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
        item = SimpleNamespace(terms={'node_history': {'version': 1}})
        assert await stage_enrollment_history(db, keys=owner, item=item) is False
        await db.commit()
        assert (
            await db.scalar(
                select(func.count()).select_from(BsTrafficUsage).where(BsTrafficUsage.state == 'written_off')
            )
            == 200
        )
        assert await stage_enrollment_history(db, keys=owner, item=item) is True
        await db.commit()
        assert (
            await db.scalar(select(func.count()).select_from(BsTrafficUsage).where(BsTrafficUsage.state == 'pending'))
            == 0
        )
        assert await db.scalar(select(func.sum(BsTrafficUsage.charged_bytes))) == 0


@pytest.mark.parametrize('version', [1, 2, 3, 4])
@pytest.mark.parametrize('changed_modes', [False, True])
def test_legacy_same_mode_manifest_keeps_source_mode_consistency(version, changed_modes):
    import hashlib
    import json
    from types import SimpleNamespace

    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.tariff_switch_topology import capture_transition, receipt_transition
    from tests.services.test_bs_traffic_squad_transition import X, values

    args = values()
    value = capture_transition(
        args['source_projection'],
        source_evidence=args['source_evidence'],
        target_evidence=args['target_evidence'],
        current_squads=args['current_squads'],
        now=args['now'],
        panel_identity=args['panel_identity'],
    )
    value['version'] = version
    value.pop('source_modes')
    value.pop('added_nodes')
    if version in (1, 2):
        value.pop('removed_nodes')
    else:
        value['source']['topology']['ordinary_nodes'].append(X)
        value['removed_nodes'] = {'ordinary': [X], 'bs': []}
    if changed_modes:
        value['source_projection']['ordinary'].update(mode='finite', remaining_bytes=10)
    terms = dict(account_generation='a' * 64, ownership_cycle=0, ordinary_mode='unlimited', bs_mode='finite')
    if version in (2, 4):
        value['allow_empty_source'] = True
        terms.update(version=1, manifest=value, predecessor_id=1)
        parameters = dict(
            topology_transition=terms,
            topology_transition_hash=hashlib.sha256(
                json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()
            ).hexdigest(),
        )
    else:
        terms['topology_transition'] = value
        parameters = dict(
            kind='tariff_switch',
            switch_terms=terms,
            switch_terms_hash=hashlib.sha256(
                json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()
            ).hexdigest(),
        )
    row = SimpleNamespace(id=2, parameters=parameters, account_generation='a' * 64)
    policy = dict(value['target'], ordinary_mode='unlimited', bs_mode='finite')
    if changed_modes:
        with pytest.raises(LedgerConflict):
            receipt_transition(row, policy, ownership_cycle=0)
    else:
        assert receipt_transition(row, policy, ownership_cycle=0) is not None
