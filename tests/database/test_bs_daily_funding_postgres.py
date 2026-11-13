"""A daily source must prove the paid window without consulting mutable prices."""

from datetime import timedelta
from importlib import import_module, util

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficLedger, BsTrafficRenewalIntent, Transaction
from app.services.bs_traffic.daily_renewal import renew_daily_from_balance
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_daily_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def core():
    assert util.find_spec('app.services.bs_traffic.daily_funding') is not None, 'funded daily source missing'
    return import_module('app.services.bs_traffic.daily_funding')


@pytest.mark.parametrize('legacy', [False, True])
async def test_funded_daily_rate_survives_tariff_edit_and_replay(postgres_database, monkeypatch, legacy):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, tariff = await prepared(db, monkeypatch)
        due = sub.end_date
        paid = await renew_daily_from_balance(db, **owner, expected_due_at=due)
        receipt = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        if legacy:
            receipt.parameters = {
                k: v for k, v in receipt.parameters.items() if k not in ('daily_funding', 'daily_funding_hash')
            }
        else:
            assert 'daily_funding' in receipt.parameters
        tariff.daily_price_kopeks = 999
        await db.commit()
        state = await db.get(BsTrafficLedger, sub.id)
        source = await core().resolve_daily_funding(db, state=state, sub=sub)
        assert source.full_rate_kopeks == 100
        assert source.window_end == sub.end_date
        assert source.window_start == sub.last_daily_charge_at
        assert source.window_end - source.window_start == timedelta(days=1)
        replay = await renew_daily_from_balance(db, **owner, expected_due_at=due)
        assert replay.intent_id == paid.intent_id and replay.replayed
        assert (await core().resolve_daily_funding(db, state=state, sub=sub)) == source


@pytest.mark.parametrize(
    'fault',
    [
        'hash',
        'amount',
        'owner',
        'generation',
        'window',
        'last_charge',
        'device_version',
        'kind',
        'malformed_snapshot',
        'missing_source',
    ],
)
async def test_changed_funding_fails_closed(postgres_database, monkeypatch, fault):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        paid = await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date)
        receipt = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        tx = await db.get(Transaction, paid.transaction_id)
        if fault == 'hash':
            receipt.parameters = dict(receipt.parameters, daily_funding_hash='invalid')
        elif fault == 'amount':
            tx.amount_kopeks -= 1
        elif fault == 'owner':
            sub.user_id = 999
        elif fault == 'generation':
            receipt.account_generation = 'different-generation'
        elif fault == 'window':
            sub.end_date += timedelta(seconds=1)
        elif fault == 'last_charge':
            sub.last_daily_charge_at += timedelta(seconds=1)
        elif fault == 'device_version':
            sub.daily_devices_billing_version = 1 - sub.daily_devices_billing_version
        elif fault in ('malformed_snapshot', 'missing_source'):
            from app.services.bs_traffic.enrollment import terms_hash

            snapshot = (
                []
                if fault == 'malformed_snapshot'
                else {k: v for k, v in receipt.parameters['daily_funding'].items() if k != 'source'}
            )
            receipt.parameters = dict(
                receipt.parameters, daily_funding=snapshot, daily_funding_hash=terms_hash(snapshot)
            )
        else:
            receipt.parameters = dict(receipt.parameters, kind='tariff_switch')
        with db.no_autoflush:
            state = await db.get(BsTrafficLedger, sub.id)
        with db.no_autoflush, pytest.raises(LedgerConflict):
            await core().resolve_daily_funding(db, state=state, sub=sub)


async def test_enrollment_snapshot_is_hashed_replayable_and_rollback_atomic(postgres_database, monkeypatch):
    from datetime import UTC, datetime

    from sqlalchemy import func

    from app.database.models import BsTrafficEnrollment
    from app.services.bs_traffic.enrollment import stage_paid_enrollment, validate_funding
    from tests.database.test_bs_enrollment_postgres import paid

    async with postgres_session(postgres_database, TABLES) as db:
        _, tariff, sub, tx = await paid(db, monkeypatch)
        tariff.is_daily = True
        sub.end_date = datetime.now(UTC) + timedelta(days=1)
        sub.last_daily_charge_at = datetime.now(UTC)
        snapshot = core().enrollment_daily_funding(sub=sub, transaction=tx)
        job = await stage_paid_enrollment(db, sub, transaction=tx, daily_funding=snapshot)
        assert job.terms['daily_funding']['full_rate_kopeks'] == 2000
        assert job.terms['daily_funding']['window_start'] != sub.start_date.isoformat()
        await validate_funding(db, job)
        repeated = await stage_paid_enrollment(db, sub, transaction=tx, daily_funding=snapshot)
        assert repeated.id == job.id
        job.terms = dict(job.terms, daily_funding=dict(snapshot, full_rate_kopeks=9999))
        with pytest.raises(LedgerConflict):
            await validate_funding(db, job)
        await db.rollback()
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 0
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('legacy', [False, True])
async def test_resume_preserves_funded_price_and_new_window(postgres_database, monkeypatch, legacy):
    from uuid import uuid4

    from app.services.bs_traffic.daily_control import quote_daily_resume, resume_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, tariff = await prepared(db, monkeypatch)
        sub.is_daily_paused = True
        await db.commit()
        quote = await quote_daily_resume(db, **owner)
        result = await resume_daily_from_balance(
            db,
            **owner,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        if legacy:
            receipt.parameters = {
                k: v for k, v in receipt.parameters.items() if k not in ('daily_funding', 'daily_funding_hash')
            }
        tariff.daily_price_kopeks = 4567
        await db.commit()
        source = await core().resolve_daily_funding(db, state=await db.get(BsTrafficLedger, sub.id), sub=sub)
        assert source.full_rate_kopeks == 100
        assert source.window_start == sub.last_daily_charge_at
        assert source.window_end == result.new_end_date


@pytest.mark.parametrize('extra_seconds', [0, 3600])
async def test_purchase_window_attestation_excludes_bonus_but_accepts_clock_drift(
    postgres_database, monkeypatch, extra_seconds
):
    from datetime import UTC, datetime

    from tests.database.test_bs_enrollment_postgres import paid

    async with postgres_session(postgres_database, TABLES) as db:
        _, tariff, sub, tx = await paid(db, monkeypatch)
        tariff.is_daily = True
        started = datetime.now(UTC)
        sub.end_date = started + timedelta(days=1, microseconds=100, seconds=extra_seconds)
        sub.last_daily_charge_at = started + timedelta(microseconds=200)
        snapshot = core().purchase_daily_funding(
            sub=sub,
            tariff=tariff,
            transaction=tx,
            period_days=1,
            checkout_started_at=started,
            checkout_finished_at=started + timedelta(seconds=1),
        )
        if extra_seconds:
            assert snapshot is None
        else:
            assert snapshot['full_rate_kopeks'] == 2000
            assert snapshot['last_daily_charge_at'] == sub.last_daily_charge_at.isoformat()
            assert snapshot['window_start'] == (sub.end_date - timedelta(days=1)).isoformat()


@pytest.mark.parametrize('entry', ['cabinet', 'device_cart', 'daily_cart', 'telegram', 'miniapp'])
async def test_real_daily_purchase_writes_one_day_funding(postgres_database, monkeypatch, entry):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from app.cabinet.routes.subscription_modules import purchase
    from app.database.crud.user import get_user_by_id
    from app.database.models import BsTrafficEnrollment, Tariff, User
    from app.services import subscription_auto_purchase_service as auto
    from tests.database.test_bs_enrollment_postgres import BS, ORDINARY
    from tests.database.test_tariff_purchase_devices_postgres import checkout_context

    monkeypatch.setattr(auto, '_delete_cart_for_subscription', AsyncMock())
    monkeypatch.setattr(auto, 'clear_subscription_checkout_draft', AsyncMock())
    async with checkout_context(postgres_database, monkeypatch) as checkout:
        async with checkout.maker() as db:
            tariff = await db.get(Tariff, 7)
            tariff.is_daily = True
            tariff.daily_price_kopeks = 1000
            tariff.device_price_kopeks = 3000
            tariff.allowed_squads = [ORDINARY]
            tariff.bs_traffic_settings = dict(enabled=True, squad_uuids=[BS], limit_gb=5)
            (await db.get(User, 1)).balance_kopeks = 0 if entry == 'device_cart' else 100000
            await db.commit()
        parameters = dict(tariff_id=7, period_days=1, selected_device_limit=3)
        if entry == 'telegram':
            import inspect
            from types import SimpleNamespace

            from app.handlers.subscription import tariff_purchase as telegram
            from tests.handlers.test_tariff_purchase_device_selection import context

            monkeypatch.setattr(telegram, 'should_create_panel_account', AsyncMock(return_value=True))
            monkeypatch.setattr(telegram.user_cart_service, 'delete_user_cart', AsyncMock())
            async with checkout.maker() as db:
                callback = SimpleNamespace(
                    data='daily_tariff_confirm:7',
                    answer=AsyncMock(),
                    bot=SimpleNamespace(send_message=AsyncMock()),
                    message=SimpleNamespace(edit_text=AsyncMock()),
                )
                await inspect.unwrap(telegram.confirm_daily_tariff_purchase)(
                    callback, await get_user_by_id(db, 1), db, context()
                )
        elif entry == 'miniapp':
            from app.webapi.routes import miniapp

            async with checkout.maker() as db:
                user = await get_user_by_id(db, 1)
                monkeypatch.setattr(miniapp, '_authorize_miniapp_user', AsyncMock(return_value=user))
                await miniapp.purchase_tariff_endpoint(
                    miniapp.MiniAppTariffPurchaseRequest(initData='test', tariffId=7, periodDays=1), db
                )
        elif entry == 'daily_cart':
            async with checkout.maker() as db:
                assert await auto._auto_purchase_daily_tariff(db, await get_user_by_id(db, 1), {'tariff_id': 7})
        else:
            preview = await checkout.send('/purchase-tariff/preview', parameters)
            assert preview.status_code == 200, preview.text
            body = parameters | dict(quote_id=preview.json()['quote_id'], checkout_id=str(uuid4()))
            response = await checkout.send('/purchase-tariff', body)
            if entry == 'device_cart':
                assert response.status_code == 402, response.text
                cart = purchase.user_cart_service.save_user_cart.await_args.args[1]
                async with checkout.maker() as db:
                    user = await get_user_by_id(db, 1)
                    user.balance_kopeks = 100000
                    await db.commit()
                    assert await auto._process_single_cart(db, user, cart)
            else:
                assert response.status_code == 200, response.text
        async with checkout.maker() as db:
            job = await db.scalar(select(BsTrafficEnrollment))
            tx = await db.get(Transaction, job.transaction_id)
            snapshot = job.terms['daily_funding']
            assert snapshot['full_rate_kopeks'] == -tx.amount_kopeks
            assert snapshot['full_rate_kopeks'] == (1000 if entry in ('daily_cart', 'telegram', 'miniapp') else 1200)
            assert snapshot['daily_devices_billing_version'] == (
                0 if entry in ('daily_cart', 'telegram', 'miniapp') else 1
            )


@pytest.mark.parametrize('fault', [None, 'legacy', 'hash', 'cycle', 'window'])
async def test_enrollment_resolver_requires_current_explicit_paid_day(postgres_database, monkeypatch, fault):
    from datetime import UTC, datetime

    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from tests.database.test_bs_enrollment_postgres import paid

    async with postgres_session(postgres_database, TABLES) as db:
        _, tariff, sub, tx = await paid(db, monkeypatch)
        tariff.is_daily = True
        sub.end_date = datetime.now(UTC) + timedelta(days=1)
        sub.last_daily_charge_at = datetime.now(UTC)
        snapshot = core().enrollment_daily_funding(sub=sub, transaction=tx)
        job = await stage_paid_enrollment(
            db, sub, transaction=tx, daily_funding=None if fault == 'legacy' else snapshot
        )
        state = BsTrafficLedger(
            subscription_id=sub.id,
            panel_scope='synthetic-scope',
            panel_user_id=1,
            account_generation='synthetic-generation',
            ownership_cycle=0,
        )
        db.add(state)
        job.account_generation, job.panel_scope = state.account_generation, state.panel_scope
        if fault == 'hash':
            job.terms_hash = 'invalid'
        elif fault == 'cycle':
            job.ownership_cycle = 1
        elif fault == 'window':
            sub.end_date += timedelta(hours=1)
        tariff.daily_price_kopeks = 9999
        await db.commit()
        if fault:
            with pytest.raises(LedgerConflict):
                await core().resolve_daily_funding(db, state=state, sub=sub)
        else:
            resolved = await core().resolve_daily_funding(db, state=state, sub=sub)
            assert resolved.full_rate_kopeks == 2000
            assert resolved.window_start == sub.end_date - timedelta(days=1)
            assert resolved.last_daily_charge_at == sub.last_daily_charge_at


@pytest.mark.parametrize('split_enabled', [False, True])
async def test_legacy_daily_cart_without_charge_anchor_keeps_purchase_without_attestation(
    postgres_database, monkeypatch, split_enabled
):
    from unittest.mock import AsyncMock

    from app.database.crud.user import get_user_by_id
    from app.database.models import BsTrafficEnrollment, Subscription, Tariff, User
    from app.services import subscription_auto_purchase_service as auto
    from tests.database.test_bs_enrollment_postgres import BS, ORDINARY
    from tests.database.test_tariff_purchase_devices_postgres import checkout_context

    monkeypatch.setattr(auto, '_delete_cart_for_subscription', AsyncMock())
    monkeypatch.setattr(auto, 'clear_subscription_checkout_draft', AsyncMock())
    async with checkout_context(postgres_database, monkeypatch) as checkout:
        async with checkout.maker() as db:
            tariff = await db.get(Tariff, 7)
            tariff.is_daily = True
            tariff.daily_price_kopeks = 1000
            tariff.allowed_squads = [ORDINARY]
            if split_enabled:
                tariff.bs_traffic_settings = dict(enabled=True, squad_uuids=[BS], limit_gb=5)
            await db.commit()
        async with checkout.maker() as db:
            user = await get_user_by_id(db, 1)
            assert await auto._auto_purchase_tariff(
                db, user, dict(tariff_id=7, period_days=1, cart_mode='tariff_purchase')
            )
        async with checkout.maker() as db:
            sub = (await db.scalars(select(Subscription))).one()
            assert sub.last_daily_charge_at is None
            assert (await db.get(User, 1)).balance_kopeks == 99000
            payment = (await db.scalars(select(Transaction))).one()
            assert payment.amount_kopeks == -1000 and payment.is_completed
            with pytest.raises(LedgerConflict):
                core().enrollment_daily_funding(sub=sub, transaction=payment)
            job = await db.scalar(select(BsTrafficEnrollment))
            if split_enabled:
                assert job is not None and 'daily_funding' not in job.terms
            else:
                assert job is None
