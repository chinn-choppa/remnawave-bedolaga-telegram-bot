"""Mode transition baselines preserve old provenance and exclude unlimited gaps."""

from datetime import timedelta
from importlib import import_module, util

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficLedger, BsTrafficManagedTransfer, BsTrafficUsageCursor, Tariff
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.observations import begin_poll, record_poll
from app.services.bs_traffic.policy import configure_groups
from tests.database.test_bs_tariff_switch_postgres import setup_switch
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def boundary():
    assert util.find_spec('app.services.bs_traffic.tariff_switch_boundary') is not None, (
        'mode baseline adoption missing'
    )
    return import_module('app.services.bs_traffic.tariff_switch_boundary')


async def unlimited_source(db, monkeypatch, group='bs'):
    reader, target, sub, new, evidence = await setup_switch(db, monkeypatch)
    old = await db.get(Tariff, sub.tariff_id)
    if group == 'bs':
        old.bs_traffic_settings = dict(old.bs_traffic_settings, limit_gb=0, topup_enabled=False)
    else:
        old.traffic_limit_gb = 0
        # An originally unlimited source has no funded finite calendar.
        # Match capture_initial_calendar rather than retaining the finite seed.
        transfer = await db.get(BsTrafficManagedTransfer, sub.id)
        transfer.calendar_terms = None
        transfer.source_phase = 'unlimited'
    ledger = await db.get(BsTrafficLedger, sub.id)
    await configure_groups(
        db,
        **target,
        ordinary_mode='unlimited' if group == 'ordinary' else 'finite',
        bs_mode='unlimited' if group == 'bs' else 'finite',
        evidence=evidence,
        expected_revision=ledger.revision,
    )
    await db.commit()
    now = await db.scalar(select(func.clock_timestamp()))
    node = ledger.access_policy['topology'][f'{group}_nodes'][0]
    return reader, target, sub, new, ledger, node, now


async def mode_observation(db, ledger, node, now, amount, *, generation=None):
    poll = await begin_poll(
        db,
        panel_scope=ledger.panel_scope,
        day=now.date(),
        node_uuids=(node,),
        identities={ledger.panel_user_id: generation or ledger.account_generation},
    )
    await record_poll(
        db, poll.id, {'nodes': [{'uuid': node, 'users': [{'id': ledger.panel_user_id, 'totalBytes': amount}]}]}
    )
    return poll


@pytest.mark.parametrize('amount', [0, 5000])
async def test_new_finite_baseline_excludes_known_unlimited_counter(postgres_database, monkeypatch, amount):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, _, ledger, node, now = await unlimited_source(db, monkeypatch)
        await mode_observation(db, ledger, node, now, amount)
        manifest = await boundary().adopt_mode_baselines(db, **target, new_modes={'ordinary': 'finite', 'bs': 'finite'})
        cursor = await db.get(BsTrafficUsageCursor, (sub.id, node, now.date()))
        assert cursor.baseline_bytes == amount and cursor.processed_bytes == amount
        assert manifest['bs']['nodes'][0]['baseline_bytes'] == amount
        assert ledger.access_policy['bs_mode'] == 'unlimited'  # Coordinator alone changes paid policy.


@pytest.mark.parametrize('fault', ['missing', 'stale', 'foreign_generation', 'reset', 'old_owner'])
async def test_unproved_baseline_never_advances_cursor(postgres_database, monkeypatch, fault):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, _, ledger, node, now = await unlimited_source(db, monkeypatch)
        if fault != 'missing':
            poll = await mode_observation(
                db, ledger, node, now, 5000, generation='f' * 64 if fault == 'foreign_generation' else None
            )
            if fault == 'stale':
                poll.created_at = now - timedelta(minutes=1)
            if fault == 'reset':
                await mode_observation(db, ledger, node, now, 4000)
            if fault == 'old_owner':
                ledger.ownership_started_at = await db.scalar(select(func.clock_timestamp()))
        await db.flush()
        before = await db.get(BsTrafficUsageCursor, (sub.id, node, now.date()))
        old_position = before.processed_bytes if before else None
        with pytest.raises(LedgerConflict):
            await boundary().adopt_mode_baselines(db, **target, new_modes={'ordinary': 'finite', 'bs': 'finite'})
        after = await db.get(BsTrafficUsageCursor, (sub.id, node, now.date()))
        assert (after.processed_bytes if after else None) == old_position


async def test_late_pre_finite_poll_cannot_consume_preserved_topup(postgres_database, monkeypatch):
    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.intake import attach_cursor, settle_reported_observation
    from app.services.bs_traffic.ledger import grant

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, _, ledger, node, now = await unlimited_source(db, monkeypatch)
        identifier = await grant(
            db,
            **target,
            group='bs',
            operation_key='paid-topup:before-unlimited',
            amount_bytes=1000,
            starts_at=now - timedelta(hours=3),
            expires_at=now + timedelta(days=2),
        )
        first = await mode_observation(db, ledger, node, now, 100)
        first.created_at = now - timedelta(hours=2)
        await db.flush()
        await attach_cursor(db, **target, group='bs', node_uuid=node, day=now.date())
        second = await mode_observation(db, ledger, node, now, 160)
        second.created_at = now - timedelta(hours=1)
        ledger.access_policy = dict(ledger.access_policy, bs_mode='finite', finite_since={'bs': now.isoformat()})
        await db.flush()
        await settle_reported_observation(db, **target, node_uuid=node, day=now.date())
        assert (await db.get(BsTrafficAllowance, identifier)).remaining_bytes == 1000


async def test_mode_preflight_collects_only_new_finite_nodes_without_money(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.database.models import Transaction

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, _, ledger, node, _ = await unlimited_source(db, monkeypatch)
        reader._api.get_daily_nodes_usage = AsyncMock(return_value={'nodes': []})
        await db.commit()
        module = import_module('app.services.bs_traffic.mode_collection')
        collect = getattr(module, 'collect_mode_baselines', None)
        assert collect is not None, 'bounded mode preflight missing'
        jobs = await collect(
            reader, user_id=target['user_id'], subscription_id=sub.id, new_modes={'ordinary': 'finite', 'bs': 'finite'}
        )
        assert len(jobs) == 1
        reader._api.get_daily_nodes_usage.assert_awaited_once()
        assert reader._api.get_daily_nodes_usage.call_args.args[0] == (node,)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('enabled', [False, True])
async def test_omitted_mode_baseline_requires_exact_job_and_explicit_policy(postgres_database, monkeypatch, enabled):
    from app.config import settings
    from app.services.bs_traffic.queue import claim_job, enqueue_job, finish_job

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, _, ledger, node, now = await unlimited_source(db, monkeypatch)
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', enabled)
        identifier = await enqueue_job(
            db,
            operation_key='mode-omission',
            panel_scope=ledger.panel_scope,
            day=now.date(),
            node_uuids=(node,),
            identities={ledger.panel_user_id: ledger.account_generation},
        )
        claim = await claim_job(db, ledger.panel_scope, job_id=identifier)
        await finish_job(db, claim, {'nodes': []})
        args = dict(**target, new_modes={'ordinary': 'finite', 'bs': 'finite'}, collection_job_ids=(identifier,))
        if not enabled:
            with pytest.raises(LedgerConflict):
                await boundary().adopt_mode_baselines(db, **args)
        else:
            manifest = await boundary().adopt_mode_baselines(db, **args)
            cursor = await db.get(BsTrafficUsageCursor, (sub.id, node, now.date()))
            assert cursor.bootstrap_evidence['kind'] == 'omitted'
            assert cursor.bootstrap_evidence['first_observed_poll_id'] is None
            assert manifest['bs']['nodes'][0]['kind'] == 'omitted'


@pytest.mark.parametrize('to_unlimited', [False, True])
@pytest.mark.parametrize('group', ['ordinary', 'bs'])
async def test_periodic_mode_switch_preserves_paid_topup_and_expiry(
    postgres_database, monkeypatch, to_unlimited, group
):
    from app.database.models import BsTrafficAllowance, BsTrafficRenewalIntent
    from app.services.bs_traffic.ledger import grant
    from tests.database.test_bs_tariff_switch_postgres import core, quote_and_args

    async with postgres_session(postgres_database, TABLES) as db:
        if to_unlimited:
            _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
            ledger = await db.get(BsTrafficLedger, sub.id)
            if group == 'bs':
                new.bs_traffic_settings = dict(new.bs_traffic_settings, limit_gb=0, topup_enabled=False)
            else:
                new.traffic_limit_gb = 0
            now = await db.scalar(select(func.clock_timestamp()))
        else:
            reader, target, sub, new, ledger, node, now = await unlimited_source(db, monkeypatch, group)
            from app.services.bs_traffic.evidence import collect_topology_evidence

            evidence = await collect_topology_evidence(
                reader,
                panel_identity=reader.panel_scope,
                ordinary_squads=ledger.access_policy['topology']['ordinary_squads'],
                bs_squads=ledger.access_policy['topology']['bs_squads'],
            )
            await mode_observation(db, ledger, node, now, 5000)
        identifier = await grant(
            db,
            **target,
            group=group,
            operation_key='paid-topup:preserved',
            amount_bytes=999,
            starts_at=now - timedelta(hours=1),
            expires_at=now + timedelta(days=2),
        )
        end_date = sub.end_date
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        topup = await db.get(BsTrafficAllowance, identifier)
        assert topup.remaining_bytes == 999 and topup.retired_at is None
        assert topup.expires_at == now + timedelta(days=2) and sub.end_date == end_date
        assert ledger.access_policy[f'{group}_mode'] == ('unlimited' if to_unlimited else 'finite')
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        if to_unlimited:
            assert group not in result.grants
        else:
            assert receipt.parameters['mode_boundary'][group]['nodes'][0]['baseline_bytes'] == 5000


@pytest.mark.parametrize('reset', [False, True])
async def test_finite_unlimited_finite_keeps_only_confirmed_consumption(postgres_database, monkeypatch, reset):
    from app.config import settings
    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.ledger import open_usage, resolve_usage
    from app.services.bs_traffic.renewal import renew_from_balance
    from tests.database.test_bs_tariff_switch_postgres import core, quote_and_args

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        first = await renew_from_balance(db, **target, period_days=30, expected_price_kopeks=2000, operation_key='seed')
        old = await db.get(Tariff, sub.tariff_id)
        original = await db.get(BsTrafficAllowance, first.grants['bs'])
        original.remaining_bytes = 2 * 1024**3  # Three GB confirmed before unlimited access.
        pending = await open_usage(
            db,
            **target,
            group='bs',
            source_key='before-unlimited-pending',
            amount_bytes=1024**3,
            interval_start=original.starts_at,
            interval_end=original.starts_at + timedelta(microseconds=1),
        )
        new.bs_traffic_settings = dict(new.bs_traffic_settings, limit_gb=0, topup_enabled=False)
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', reset)
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        await core().switch_tariff_from_balance(db, **args)
        await db.refresh(original)
        assert original.retired_at is not None
        ledger = await db.get(BsTrafficLedger, sub.id)
        node = ledger.access_policy['topology']['bs_nodes'][0]
        now = await db.scalar(select(func.clock_timestamp()))
        await mode_observation(db, ledger, node, now, 50 * 1024**3)
        _, args = await quote_and_args(db, target, old, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        restored = await db.get(BsTrafficAllowance, result.grants['bs'])
        assert restored.remaining_bytes == (5 if reset else 2) * 1024**3
        await resolve_usage(db, **target, usage_id=pending, decision='charge')
        await db.refresh(restored)
        assert restored.remaining_bytes == (5 if reset else 2) * 1024**3


async def test_missing_mode_baseline_rolls_back_money_policy_and_receipt(postgres_database, monkeypatch):
    from app.database.models import BsTrafficRenewalIntent, Transaction, User
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from tests.database.test_bs_tariff_switch_postgres import core, quote_and_args

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, ledger, _, _ = await unlimited_source(db, monkeypatch)
        evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=ledger.access_policy['topology']['ordinary_squads'],
            bs_squads=ledger.access_policy['topology']['bs_squads'],
        )
        user = await db.get(User, target['user_id'])
        old_balance, old_tariff, old_policy = user.balance_kopeks, sub.tariff_id, dict(ledger.access_policy)
        _, args = await quote_and_args(db, target, new, evidence)
        with pytest.raises(LedgerConflict):
            await core().switch_tariff_from_balance(db, **args)
        await db.refresh(user)
        await db.refresh(sub)
        await db.refresh(ledger)
        assert user.balance_kopeks == old_balance and sub.tariff_id == old_tariff
        assert ledger.access_policy == old_policy
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0


async def test_old_unlimited_days_do_not_fill_purchase_settlement_budget(postgres_database, monkeypatch):
    from app.database.models import BsTrafficObservation, BsTrafficPoll, BsTrafficUsage
    from app.services.bs_traffic.intake import settle_known_group

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, _, ledger, node, now = await unlimited_source(db, monkeypatch)
        for offset in range(1, 403):
            day = (now - timedelta(days=offset)).date()
            poll = BsTrafficPoll(
                panel_scope=ledger.panel_scope,
                day=day,
                node_uuids=[node],
                identities={},
                selected_accounts=1,
                response_hash='a' * 64,
                response_nodes=[node],
                observed_pairs=1,
                created_at=now - timedelta(days=offset),
            )
            db.add(poll)
            await db.flush()
            db.add(
                BsTrafficObservation(
                    panel_scope=ledger.panel_scope,
                    panel_user_id=ledger.panel_user_id,
                    account_generation=ledger.account_generation,
                    node_uuid=node,
                    day=day,
                    high_water_bytes=100,
                    latest_bytes=100,
                    latest_poll_id=poll.id,
                    needs_review=False,
                )
            )
            db.add(
                BsTrafficUsageCursor(
                    subscription_id=sub.id,
                    node_uuid=node,
                    day=day,
                    account_generation=ledger.account_generation,
                    ownership_cycle=ledger.ownership_cycle,
                    traffic_group='bs',
                    baseline_bytes=0,
                    processed_bytes=0,
                    needs_review=False,
                )
            )
        ledger.access_policy = dict(ledger.access_policy, bs_mode='finite', finite_since={'bs': now.isoformat()})
        await db.flush()
        await settle_known_group(db, **target, group='bs')
        assert await db.scalar(select(func.count()).select_from(BsTrafficUsage)) == 0


async def test_both_groups_can_switch_to_unlimited_and_back(postgres_database, monkeypatch):
    from tests.database.test_bs_tariff_switch_postgres import core, quote_and_args

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        old = await db.get(Tariff, sub.tariff_id)
        new.traffic_limit_gb = 0
        new.bs_traffic_settings = dict(new.bs_traffic_settings, limit_gb=0, topup_enabled=False)
        _, args = await quote_and_args(db, target, new, evidence)
        unlimited = await core().switch_tariff_from_balance(db, **args)
        assert unlimited.grants == {}
        ledger = await db.get(BsTrafficLedger, sub.id)
        now = await db.scalar(select(func.clock_timestamp()))
        for group in ('ordinary', 'bs'):
            await mode_observation(db, ledger, ledger.access_policy['topology'][f'{group}_nodes'][0], now, 5000)
        _, args = await quote_and_args(db, target, old, evidence)
        finite = await core().switch_tariff_from_balance(db, **args)
        assert set(finite.grants) == {'ordinary', 'bs'}
        assert set(ledger.access_policy['finite_since']) == {'ordinary', 'bs'}


@pytest.mark.parametrize('write_off', [False, True])
async def test_first_post_mode_delta_never_spends_grant_before_adopted_sample(
    postgres_database, monkeypatch, write_off
):
    from app.config import settings
    from app.database.models import BsTrafficAllowance, BsTrafficUsage
    from app.services.bs_traffic.intake import settle_reported_observation
    from app.services.bs_traffic.ledger import grant

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, _, _, ledger, node, now = await unlimited_source(db, monkeypatch)
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
        initial = await mode_observation(db, ledger, node, now, 100)
        initial.created_at = now - timedelta(seconds=1)
        await db.flush()
        manifest = await boundary().adopt_mode_baselines(db, **target, new_modes={'ordinary': 'finite', 'bs': 'finite'})
        ledger.access_policy = dict(
            ledger.access_policy, bs_mode='finite', finite_since={'bs': manifest['bs']['finite_since']}
        )
        await db.flush()  # grant re-locks/populates state; persist the coordinator's boundary first.
        identifier = await grant(
            db,
            **target,
            group='bs',
            operation_key='base:mode-concurrent-sample',
            amount_bytes=1000,
            starts_at=now - timedelta(seconds=2),
            expires_at=now + timedelta(days=1),
        )
        following = await mode_observation(db, ledger, node, now, 160)
        following.created_at = await db.scalar(select(func.clock_timestamp()))
        await db.flush()
        event_id = await settle_reported_observation(db, **target, node_uuid=node, day=now.date())
        assert (await db.get(BsTrafficAllowance, identifier)).remaining_bytes == 1000
        event = await db.get(BsTrafficUsage, event_id)
        assert event.evidence_kind == 'raw_pending'
        assert event.state == ('written_off' if write_off else 'pending')
        assert event.charged_bytes == 0
        following = await mode_observation(db, ledger, node, now, 200)
        following.created_at = await db.scalar(select(func.clock_timestamp()))
        await db.flush()
        await settle_reported_observation(db, **target, node_uuid=node, day=now.date())
        assert (await db.get(BsTrafficAllowance, identifier)).remaining_bytes == 960
