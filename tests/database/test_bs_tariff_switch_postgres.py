"""Split switching keeps money, rights and retry receipts in one transaction."""

from importlib import import_module, util
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficLedger,
    BsTrafficManagedTransfer,
    BsTrafficRenewalIntent,
    Tariff,
    TrafficAccountingState,
    Transaction,
    User,
)
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def core():
    assert util.find_spec('app.services.bs_traffic.tariff_switch') is not None, 'split switch coordinator is missing'
    return import_module('app.services.bs_traffic.tariff_switch')


async def setup_switch(db, monkeypatch):
    reader, target, sub = await prepared(db, monkeypatch)
    old = await db.get(Tariff, sub.tariff_id)
    # This synthetic transferred account funds NO_RESET, matching the tariff.
    source = await db.get(TrafficAccountingState, sub.id)
    source.business_strategy = 'NO_RESET'
    source.next_refresh_at = None
    transfer = await db.get(BsTrafficManagedTransfer, sub.id)
    if transfer.calendar_terms is not None:
        transfer.calendar_terms = dict(
            transfer.calendar_terms,
            calendar_strategy='NO_RESET',
            source_next_refresh_at=None,
            next_refresh_at=None,
        )
    new = Tariff(
        name='Switch target',
        is_active=True,
        period_prices={'30': 4000},
        traffic_limit_gb=20,
        device_limit=4,
        allowed_squads=list(old.allowed_squads),
        traffic_reset_mode='NO_RESET',
        bs_traffic_settings=dict(old.bs_traffic_settings, limit_gb=10),
    )
    db.add(new)
    monkeypatch.setattr(settings, 'TARIFF_SWITCH_UPGRADE_ENABLED', True)
    monkeypatch.setattr(settings, 'TARIFF_SWITCH_DOWNGRADE_ENABLED', True)
    monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', True)
    await db.commit()
    from app.services.bs_traffic.evidence import collect_topology_evidence

    policy = (await db.get(BsTrafficLedger, sub.id)).access_policy
    evidence = await collect_topology_evidence(
        reader,
        panel_identity=reader.panel_scope,
        ordinary_squads=policy['topology']['ordinary_squads'],
        bs_squads=policy['topology']['bs_squads'],
    )
    return reader, target, sub, new, evidence


async def quote_and_args(db, target, new, evidence):
    quote = await core().quote_tariff_switch(db, **target, new_tariff_id=new.id, evidence=evidence)
    return quote, dict(
        **target,
        new_tariff_id=new.id,
        evidence=evidence,
        expected_price_kopeks=quote.price_kopeks,
        expected_terms_hash=quote.terms_hash,
        operation_key=str(uuid4()),
    )


async def test_switch_atomic_replay_keeps_expiry_and_changes_both_bases(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        before = sub.end_date
        quote, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        await db.commit()
        assert sub.tariff_id == new.id and sub.end_date == before
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000 - quote.price_kopeks
        assert (await db.get(BsTrafficAllowance, result.grants['ordinary'])).remaining_bytes == 20 * 1024**3
        assert (await db.get(BsTrafficAllowance, result.grants['bs'])).remaining_bytes == 10 * 1024**3
        replay = await core().switch_tariff_from_balance(db, **args)
        assert replay.replayed and replay.transaction_id == result.transaction_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('fault', ['price', 'terms', 'balance', 'foreign', 'topology', 'daily', 'sales_mode'])
async def test_invalid_switch_rolls_back_all_rights(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        _, args = await quote_and_args(db, target, new, evidence)
        old_id = sub.tariff_id
        user = await db.get(User, sub.user_id)
        if fault == 'price':
            args['expected_price_kopeks'] += 1
        if fault == 'terms':
            args['expected_terms_hash'] = 'a' * 64
        if fault == 'balance':
            user.balance_kopeks = 0
        if fault == 'foreign':
            args['user_id'] = 2
        if fault == 'topology':
            new.allowed_squads = [str(uuid4())]
        if fault == 'daily':
            new.is_daily = True
        if fault == 'sales_mode':
            monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: False)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await core().switch_tariff_from_balance(db, **args)
        await db.commit()
        await db.refresh(sub)
        assert sub.tariff_id == old_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0


@pytest.mark.parametrize('renew_between', [False, True])
async def test_no_reset_carries_consumed_excess_through_downgrade_and_keeps_topups(
    postgres_database, monkeypatch, renew_between
):
    from datetime import timedelta

    from app.services.bs_traffic.ledger import grant
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        first = await renew_from_balance(db, **target, period_days=30, expected_price_kopeks=2000, operation_key='seed')
        ordinary = await db.get(BsTrafficAllowance, first.grants['ordinary'])
        ordinary.remaining_bytes = 2 * 1024**3  # Eight GB of confirmed usage.
        now = await db.scalar(select(func.clock_timestamp()))
        topup_id = await grant(
            db,
            **target,
            group='ordinary',
            operation_key='topup:test',
            amount_bytes=3 * 1024**3,
            starts_at=now,
            expires_at=now + timedelta(days=10),
        )
        topup = await db.get(BsTrafficAllowance, topup_id)
        topup_terms = (topup.remaining_bytes, topup.expires_at, topup.retired_at)
        new.traffic_limit_gb = 5
        new.period_prices = {'30': 1000}
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', False)
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        low = await core().switch_tariff_from_balance(db, **args)
        assert (await db.get(BsTrafficAllowance, low.grants['ordinary'])).remaining_bytes == 0
        if renew_between:
            monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', False)
            await renew_from_balance(
                db, **target, period_days=30, expected_price_kopeks=1000, operation_key='extend-low'
            )
        old = await db.get(Tariff, 1)
        _, args_back = await quote_and_args(db, target, old, evidence)
        high = await core().switch_tariff_from_balance(db, **args_back)
        assert (await db.get(BsTrafficAllowance, high.grants['ordinary'])).remaining_bytes == 2 * 1024**3
        assert (topup.remaining_bytes, topup.expires_at, topup.retired_at) == topup_terms


async def test_new_switch_supersedes_unsent_same_expiry_receipt_without_old_write(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        old = await db.get(Tariff, sub.tariff_id)
        _, args = await quote_and_args(db, target, new, evidence)
        first = await core().switch_tariff_from_balance(db, **args)
        _, back = await quote_and_args(db, target, old, evidence)
        second = await core().switch_tariff_from_balance(db, **back)
        await db.commit()
        remote = {'state': reader._api.get_split_grace_snapshot.return_value}
        reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

        async def write(target):
            remote['state'] = target

        reader._api.restore_split_user_once = AsyncMock(side_effect=write)
        assert await advance_renewal_delivery(reader, **target, operation_id=first.intent_id) == 'obsolete'
        reader._api.restore_split_user_once.assert_not_awaited()
        assert await advance_renewal_delivery(reader, **target, operation_id=second.intent_id) == 'verified'
        row = await db.get(BsTrafficRenewalIntent, first.intent_id, populate_existing=True)
        assert row.state == 'superseded'
        assert row.parameters['superseded_by_intent_id'] == second.intent_id


async def test_pending_old_usage_never_charges_new_switch_base(postgres_database, monkeypatch):
    from datetime import timedelta

    from app.services.bs_traffic.ledger import open_usage, resolve_usage
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        first = await renew_from_balance(db, **target, period_days=30, expected_price_kopeks=2000, operation_key='seed')
        old = await db.get(BsTrafficAllowance, first.grants['ordinary'])
        pending = await open_usage(
            db,
            **target,
            group='ordinary',
            source_key='old-uncertain',
            amount_bytes=1024**3,
            interval_start=old.starts_at,
            interval_end=old.starts_at + timedelta(microseconds=1),
        )
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', False)
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        await resolve_usage(db, **target, usage_id=pending, decision='charge')
        assert (await db.get(BsTrafficAllowance, result.grants['ordinary'])).remaining_bytes == 20 * 1024**3
        from app.database.models import BsTrafficUsage

        # The imported pre-renewal base is also eligible at this boundary;
        # FIFO may charge its small residual before the newer old base.
        assert (await db.get(BsTrafficUsage, pending)).charged_bytes == 1024**3
        assert old.remaining_bytes < 10 * 1024**3


async def test_receipt_failure_rolls_back_debit_tariff_and_grants(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        _, args = await quote_and_args(db, target, new, evidence)
        old_id = sub.tariff_id
        count = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        monkeypatch.setattr(core(), 'create_transaction', AsyncMock(side_effect=RuntimeError('receipt failed')))
        with pytest.raises(RuntimeError, match='receipt failed'):
            await core().switch_tariff_from_balance(db, **args)
        await db.commit()
        await db.refresh(sub)
        assert sub.tariff_id == old_id
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == count
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0


async def test_concurrent_same_uuid_switch_only_charges_once(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        quote, args = await quote_and_args(db, target, new, evidence)
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        await db.commit()

        async def apply():
            async with factory() as session, session.begin():
                return await core().switch_tariff_from_balance(session, **args)

        results = await asyncio.gather(apply(), apply())
        assert sorted(row.replayed for row in results) == [False, True]
        assert results[0].transaction_id == results[1].transaction_id
        await db.refresh(sub)
        assert (await db.get(User, sub.user_id, populate_existing=True)).balance_kopeks == 10000 - quote.price_kopeks
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_historical_retry_works_after_disabled_sale_and_rejects_changed_target(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        _, args = await quote_and_args(db, target, new, evidence)
        first = await core().switch_tariff_from_balance(db, **args)
        sub.traffic_accounting_mode = 'managed'
        sub.status = 'disabled'
        new.is_active = False
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 0
        await db.commit()
        historical = {key: value for key, value in args.items() if key not in ('evidence', 'account_generation')}
        again = await core().find_tariff_switch(db, **historical)
        assert again.replayed and again.transaction_id == first.transaction_id
        with pytest.raises(LedgerConflict):
            await core().find_tariff_switch(db, **(historical | {'new_tariff_id': 999}))
        assert user.balance_kopeks == 0


async def test_calendar_anchor_survives_switch_and_next_interval_uses_new_terms(postgres_database, monkeypatch):
    from datetime import timedelta

    from app.database.models import TrafficAccountingState
    from app.services.bs_traffic.calendar_renewal import refresh_calendar_base

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        old = await db.get(Tariff, sub.tariff_id)
        old.traffic_reset_mode = new.traffic_reset_mode = 'DAY'
        from app.services.bs_traffic.renewal import renew_from_balance

        await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='calendar-seed'
        )
        source = await db.get(TrafficAccountingState, sub.id)
        before = source.calendar_anchor, source.calendar_timezone, sub.end_date
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        from datetime import datetime

        boundary = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        refreshed = await refresh_calendar_base(db, **target, now=boundary + timedelta(seconds=1))
        assert refreshed.status == 'refreshed'
        assert (await db.get(BsTrafficAllowance, refreshed.grants['ordinary'])).granted_bytes == 20 * 1024**3
        assert (await db.get(BsTrafficAllowance, refreshed.grants['bs'])).granted_bytes == 10 * 1024**3
        assert (source.calendar_anchor, source.calendar_timezone, sub.end_date) == before


async def test_switch_quote_repairs_grace_echo_before_pricing_paid_days(postgres_database, monkeypatch):
    from dataclasses import replace
    from datetime import timedelta

    from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway, SQLAlchemyGraceSessionStore
    from app.services.grace_access_service import GracePanelSnapshot, GraceReason, GraceSessionState
    from tests.database.test_bs_traffic_grace_transition_postgres import grace_session
    from tests.services.test_grace_split_snapshot import context

    async with postgres_session(postgres_database, TABLES) as db:
        reader, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        paid_end = sub.end_date
        sub.connected_squads = list((await db.get(Tariff, sub.tariff_id)).allowed_squads)
        sub.traffic_limit_gb = 10
        await db.flush()
        current = reader._api.get_split_grace_snapshot.return_value
        source = grace_session()
        ledger = await db.get(BsTrafficLedger, sub.id)
        source = replace(
            source,
            reason=GraceReason.EXPIRED,
            split_before=context(
                panel_scope=reader.panel_scope,
                account_generation=target['account_generation'],
                ledger_revision=ledger.revision,
                topology_fingerprint=ledger.access_policy['fingerprint'],
                tariff_id=sub.tariff_id,
            ),
            billing_before=await SQLAlchemyGraceBillingGateway(db).get_subscription(sub.id),
            panel_before=GracePanelSnapshot(
                current.id, 'ACTIVE', paid_end, 10 * 1024**3, 0, tuple(sub.connected_squads)
            ),
            overlay=replace(source.overlay, expire_at=paid_end + timedelta(days=5)),
        )
        store = SQLAlchemyGraceSessionStore(db)
        saved = await store.create(source)
        saved = await store.save(replace(saved, state=GraceSessionState.ACTIVE))
        sub.end_date = saved.overlay.expire_at
        sub.connected_squads = list(saved.overlay.squad_uuids)
        sub.traffic_limit_gb = saved.overlay.traffic_limit_bytes // 1024**3
        await db.commit()
        quote, _ = await quote_and_args(db, target, new, evidence)
        assert quote.expected_end_date == paid_end
        assert sub.end_date == paid_end
