"""Money, period, both base grants and delivery receipt commit together."""

from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficRenewalIntent, Subscription, Tariff, Transaction, User
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepared(db, monkeypatch):
    _, reader, target, _ = await prepared_purchase(db, monkeypatch)
    sub = await db.get(Subscription, target['subscription_id'])
    tariff = await db.get(Tariff, sub.tariff_id)
    tariff.period_prices = {'30': 2000, '10000': 2000}
    tariff.traffic_reset_mode = 'NO_RESET'
    monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', True)
    await db.commit()
    return reader, target, sub


async def test_paid_renewal_replay_keeps_one_charge_and_one_period(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        before = sub.end_date
        result = await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='renew:one'
        )
        await db.commit()
        assert (await db.get(User, sub.user_id)).balance_kopeks == 8000
        assert sub.end_date == before + timedelta(days=30)
        assert (await db.get(BsTrafficRenewalIntent, result.intent_id)).intended_expires_at == sub.end_date
        assert set(result.grants) == {'ordinary', 'bs'}
        again = await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='renew:one'
        )
        await db.commit()
        assert again.replayed and again.transaction_id == result.transaction_id
        assert (await db.get(User, sub.user_id)).balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert sub.end_date == before + timedelta(days=30)


@pytest.mark.parametrize('fault', ['price', 'balance', 'foreign'])
async def test_rejected_renewal_does_not_change_money_or_expiry(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        user = await db.get(User, sub.user_id)
        if fault == 'balance':
            user.balance_kopeks = 1
            await db.commit()
        before, balance = sub.end_date, user.balance_kopeks
        if fault == 'foreign':
            target['user_id'] = 2
        with pytest.raises(LedgerConflict):
            await renew_from_balance(
                db,
                **target,
                period_days=30,
                expected_price_kopeks=2001 if fault == 'price' else 2000,
                operation_key='renew:reject',
            )
        await db.commit()
        await db.refresh(sub)
        await db.refresh(user)
        assert user.balance_kopeks == balance and sub.end_date == before
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_disabled_bs_renews_only_ordinary_and_keeps_old_bs_horizon(postgres_database, monkeypatch):
    from app.database.crud.tariff import update_tariff
    from app.database.models import BsTrafficLedger
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        before = sub.end_date
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        result = await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='renew:ordinary'
        )
        await db.commit()
        assert set(result.grants) == {'ordinary'}
        assert (await db.get(BsTrafficLedger, sub.id)).access_policy['bs_drain_until'] == before.isoformat()


async def test_common_finalizer_uses_durable_split_renewal(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.pricing_engine import pricing_engine
    from app.services.subscription_renewal_service import SubscriptionRenewalService
    from app.services.subscription_service import SubscriptionService

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        await db.refresh(sub, ['tariff'])
        user = await db.get(User, sub.user_id)
        price = await pricing_engine.calculate_renewal_price(db, sub, 30, user=user)
        monkeypatch.setattr(SubscriptionService, 'update_remnawave_user', AsyncMock(return_value=None))
        monkeypatch.setattr(SubscriptionService, 'create_remnawave_user', AsyncMock(return_value=None))
        monkeypatch.setattr(
            'app.services.subscription_renewal_service.emit_transaction_side_effects', AsyncMock(), raising=False
        )
        monkeypatch.setattr(
            'app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False), raising=False
        )
        result = await SubscriptionRenewalService().finalize(db, user, sub, price, operation_key='adapter:one')
        assert result.charged_from_balance_kopeks == 2000
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 1


async def test_partial_external_payment_does_not_charge_its_funded_part_again(postgres_database, monkeypatch):
    from app.database.models import PaymentMethod
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        result = await renew_from_balance(
            db,
            **target,
            period_days=30,
            expected_price_kopeks=2000,
            operation_key='provider:one',
            charge_balance_amount=500,
            payment_method=PaymentMethod.CRYPTOBOT,
        )
        await db.commit()
        assert result.charged_kopeks == 500
        assert (await db.get(User, sub.user_id)).balance_kopeks == 9500
        assert (await db.get(Transaction, result.transaction_id)).amount_kopeks == -2000
        replay = await renew_from_balance(
            db,
            **target,
            period_days=30,
            expected_price_kopeks=2000,
            operation_key='provider:one',
            charge_balance_amount=500,
            payment_method=PaymentMethod.CRYPTOBOT,
        )
        assert replay.replayed and (await db.get(User, sub.user_id)).balance_kopeks == 9500


async def test_configured_very_long_period_is_supported(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        before = sub.end_date
        await renew_from_balance(db, **target, period_days=10000, expected_price_kopeks=2000, operation_key='long:one')
        assert sub.end_date == before + timedelta(days=10000)


async def test_grant_failure_rolls_back_debit_even_if_caller_catches(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        before = sub.end_date
        monkeypatch.setattr(
            'app.services.bs_traffic.renewal.reset_renewal_bases', AsyncMock(side_effect=RuntimeError('grant failed'))
        )
        with pytest.raises(RuntimeError):
            await renew_from_balance(
                db, **target, period_days=30, expected_price_kopeks=2000, operation_key='failed:one'
            )
        await db.commit()
        await db.refresh(sub)
        assert sub.end_date == before
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('owner_mode', ['split', 'managed', 'legacy'])
async def test_balance_receipt_replays_before_changed_tariff_and_empty_balance(
    postgres_database, monkeypatch, owner_mode
):
    from app.services.bs_traffic.renewal import find_balance_renewal, renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        first = await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='retry:one'
        )
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 0
        user.restriction_subscription = True
        sub.traffic_accounting_mode = owner_mode
        sub.status = 'expired'
        (await db.get(Tariff, sub.tariff_id)).period_prices = {'30': 9000}
        await db.commit()
        replay = await find_balance_renewal(
            db,
            user_id=sub.user_id,
            subscription_id=sub.id,
            period_days=30,
            operation_key='retry:one',
            expected_price_kopeks=2000,
        )
        assert replay.replayed and replay.transaction_id == first.transaction_id
        with pytest.raises(LedgerConflict):
            await find_balance_renewal(
                db,
                user_id=sub.user_id,
                subscription_id=sub.id,
                period_days=30,
                operation_key='retry:one',
                expected_price_kopeks=9000,
            )
        assert (
            await find_balance_renewal(
                db,
                user_id=2,
                subscription_id=sub.id,
                period_days=30,
                operation_key='retry:one',
                expected_price_kopeks=2000,
            )
            is None
        )
        assert user.balance_kopeks == 0
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('owner_mode', ['split', 'managed'])
async def test_cabinet_paid_retry_precedes_repricing_and_low_balance(postgres_database, monkeypatch, owner_mode):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from app.cabinet.routes.subscription_modules import helpers
    from app.cabinet.routes.subscription_modules.renewal import renew_subscription
    from app.cabinet.schemas.subscription import RenewalRequest
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        operation = uuid4()
        await renew_from_balance(db, **target, period_days=30, expected_price_kopeks=2000, operation_key=str(operation))
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 0
        user.restriction_subscription = True
        sub.traffic_accounting_mode = owner_mode
        (await db.get(Tariff, sub.tariff_id)).period_prices = {}
        await db.commit()
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        response = await renew_subscription(
            RenewalRequest(period_days=30, operation_id=operation, expected_price_kopeks=2000),
            user=user,
            db=db,
            subscription_id=sub.id,
        )
        assert response['amount_paid_kopeks'] == 2000
        assert response['delivery_pending'] is True
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_cabinet_does_not_charge_repriced_confirmation(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from fastapi import HTTPException

    from app.cabinet.routes.subscription_modules import helpers
    from app.cabinet.routes.subscription_modules.renewal import renew_subscription
    from app.cabinet.schemas.subscription import RenewalRequest

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        await db.refresh(sub, ['tariff'])
        user = await db.get(User, sub.user_id)
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        with pytest.raises(HTTPException) as error:
            await renew_subscription(
                RenewalRequest(period_days=30, operation_id=uuid4(), expected_price_kopeks=1000),
                user=user,
                db=db,
                subscription_id=sub.id,
            )
        assert error.value.status_code == 409
        await db.commit()
        assert user.balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('entry', ['monitor', 'topup'])
async def test_monitor_autopay_uses_split_receipt_once(postgres_database, monkeypatch, entry):
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock

    from app.services import monitoring_service as monitoring

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        sub.end_date = datetime.now(UTC) + timedelta(hours=6)
        if entry == 'topup':
            sub.end_date = datetime.now(UTC) - timedelta(minutes=30)
            sub.status = 'expired'
        sub.updated_at = datetime.now(UTC) - timedelta(days=2)
        sub.autopay_enabled = True
        sub.autopay_period_days = 30
        sub.is_trial = False
        await db.commit()
        before = sub.end_date
        monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
        monkeypatch.setattr(settings, 'ADMIN_NOTIFICATIONS_ENABLED', False)
        monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
        monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False))
        monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
        monkeypatch.setattr(monitoring.notification_delivery_service, 'notify_autopay_success', AsyncMock())
        service = monitoring.MonitoringService(bot=None)
        service.subscription_service.update_remnawave_user = AsyncMock(return_value=None)
        if entry == 'topup':
            from app.services.subscription_auto_purchase_service import try_auto_extend_expired_after_topup

            user = await db.get(User, sub.user_id)
            assert await try_auto_extend_expired_after_topup(db, user)
            assert not await try_auto_extend_expired_after_topup(db, user)
        else:
            await service._process_autopayments(db)
            await service._process_autopayments(db)
        await db.refresh(sub)
        if entry == 'topup':
            assert sub.end_date > before + timedelta(days=30)
        else:
            assert sub.end_date == before + timedelta(days=30)
        assert (await db.get(User, sub.user_id)).balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 1
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        service.subscription_service.update_remnawave_user.assert_not_called()


@pytest.mark.parametrize('price_changed', [False, True])
async def test_saved_split_cart_uses_confirmed_price_and_one_receipt(postgres_database, monkeypatch, price_changed):
    from unittest.mock import AsyncMock

    from app.services import subscription_auto_purchase_service as auto

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        user = await db.get(User, sub.user_id)
        before = sub.end_date
        cart = {
            'subscription_id': sub.id,
            'tariff_id': sub.tariff_id,
            'period_days': 30,
            'total_price': 1000 if price_changed else 2000,
            '_cart_revision': 'a' * 32,
        }
        monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
        monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
        monkeypatch.setattr(auto, '_reject_unavailable_saved_squads', AsyncMock(return_value=False))
        monkeypatch.setattr(auto, '_delete_cart_for_subscription', AsyncMock())
        monkeypatch.setattr(auto, 'clear_subscription_checkout_draft', AsyncMock())
        monkeypatch.setattr(auto, '_notify_email_user_auto_purchase', AsyncMock())
        monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False))
        monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
        result = await auto._auto_extend_subscription(db, user, cart)
        assert result is (not price_changed)
        if not price_changed:
            user.balance_kopeks = 0
            sub.traffic_accounting_mode = 'managed'
            (await db.get(Tariff, sub.tariff_id)).period_prices = {}
            await db.commit()
            assert await auto._auto_extend_subscription(db, user, cart)
        await db.refresh(sub)
        assert sub.end_date == before + timedelta(days=0 if price_changed else 30)
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == int(not price_changed)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == int(not price_changed)


async def test_cabinet_low_balance_cart_and_retry_share_payment_identity(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from fastapi import HTTPException

    from app.cabinet.routes.subscription_modules import helpers
    from app.cabinet.routes.subscription_modules.renewal import renew_subscription
    from app.cabinet.schemas.subscription import RenewalRequest
    from app.services import subscription_auto_purchase_service as auto
    from app.services.user_cart_service import user_cart_service

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        await db.refresh(sub, ['tariff'])
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 0
        await db.commit()
        monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
        monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
        monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
        monkeypatch.setattr(helpers, 'resolve_subscription', AsyncMock(return_value=sub))
        save = AsyncMock()
        monkeypatch.setattr(user_cart_service, 'save_user_cart', save)
        request = RenewalRequest(period_days=30, operation_id=uuid4(), expected_price_kopeks=2000)
        with pytest.raises(HTTPException) as error:
            await renew_subscription(request, user=user, db=db, subscription_id=sub.id)
        assert error.value.status_code == 402
        cart = save.call_args.args[1]
        assert cart['renewal_operation_key'] == str(request.operation_id)
        user.balance_kopeks = 10000
        await db.commit()
        monkeypatch.setattr(auto, '_reject_unavailable_saved_squads', AsyncMock(return_value=False))
        monkeypatch.setattr(auto, '_delete_cart_for_subscription', AsyncMock())
        monkeypatch.setattr(auto, 'clear_subscription_checkout_draft', AsyncMock())
        monkeypatch.setattr(auto, '_notify_email_user_auto_purchase', AsyncMock())
        monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', AsyncMock(return_value=False))
        monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
        assert await auto._auto_extend_subscription(db, user, cart)
        response = await renew_subscription(request, user=user, db=db, subscription_id=sub.id)
        assert response['amount_paid_kopeks'] == 2000
        assert user.balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('expired', [False, True])
async def test_first_paid_trial_conversion_refreshes_base_even_when_reset_disabled(
    postgres_database, monkeypatch, expired
):
    from datetime import UTC, datetime

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.base_renewal import reset_renewal_bases
    from app.services.bs_traffic.renewal import renew_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub = await prepared(db, monkeypatch)
        now = datetime.now(UTC)
        initial = await reset_renewal_bases(
            db,
            **target,
            operation_key='trial-initial',
            amounts={'ordinary': 1024, 'bs': 1024},
            starts_at=now,
            expires_at=now + timedelta(days=2),
        )
        for grant_id in initial.values():
            (await db.get(BsTrafficAllowance, grant_id)).remaining_bytes = 10
        sub.is_trial = True
        sub.status = 'expired' if expired else 'trial'
        sub.end_date = now + timedelta(days=-1 if expired else 1)
        await db.commit()
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', False)
        result = await renew_from_balance(
            db,
            **target,
            period_days=30,
            expected_price_kopeks=2000,
            operation_key='trial-paid',
        )
        for group, grant_id in result.grants.items():
            assert grant_id != initial[group]
            grant = await db.get(BsTrafficAllowance, grant_id)
            assert grant.remaining_bytes == grant.granted_bytes
        assert not sub.is_trial
