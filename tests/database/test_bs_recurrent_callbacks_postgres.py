"""Real recurring adapters atomically grant split rights or preserve collected money."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import (
    BsTrafficRenewalIntent,
    CasheraSubscription,
    CisPaySubscription,
    LavaSubscription,
    PlategaSubscription,
    Tariff,
    Transaction,
    User,
)
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
METHODS = ['lava', 'platega', 'cashera', 'cispay']
REMOTE = '00000000-0000-4000-8000-000000000002'


async def adapter(db, monkeypatch, method, sub, *, payer=None):
    from app.database.crud import transaction
    from app.services import subscription_service, traffic_base_grant_service
    from app.services.bs_traffic import renewal
    from app.services.notification_delivery_service import notification_delivery_service

    monkeypatch.setattr(notification_delivery_service, 'notify_balance_topup', AsyncMock())
    delivery = AsyncMock(return_value=False)
    monkeypatch.setattr(renewal, 'deliver_renewal', delivery)
    monkeypatch.setattr(transaction, 'emit_transaction_side_effects', AsyncMock())
    monkeypatch.setattr(
        subscription_service.SubscriptionService,
        'update_remnawave_user',
        AsyncMock(side_effect=AssertionError('legacy panel')),
    )
    monkeypatch.setattr(
        traffic_base_grant_service, 'stage_paid_base_grant', AsyncMock(side_effect=AssertionError('legacy grant'))
    )
    common = dict(
        user_id=payer or sub.user_id,
        subscription_id=sub.id,
        tariff_id=sub.tariff_id,
        charge_days=30,
        amount_kopeks=2000,
        status='ACTIVE',
    )
    if method == 'lava':
        from app.services.payment import lava

        record = LavaSubscription(
            **common, order_id='test-order', lava_product_id='test-product', lava_subscription_id=REMOTE
        )
        agent = lava.LavaPaymentMixin()
        monkeypatch.setattr(agent, '_notify_lava_recurring', AsyncMock())
        monkeypatch.setattr(lava.lava_service, 'unsubscribe_recurrent', AsyncMock())

        async def call(number, *, session=None):
            return await agent.process_lava_subscription_callback(
                session or db,
                dict(order_id=record.order_id, invoice_id=f'charge-{number}', status='success', amount='20.00'),
            )
    elif method == 'platega':
        from types import SimpleNamespace

        from app.services.payment import platega

        record = PlategaSubscription(**common, interval=3, platega_subscription_id=REMOTE)
        agent = platega.PlategaPaymentMixin()
        agent.platega_service = SimpleNamespace(cancel_subscription=AsyncMock())
        monkeypatch.setattr(agent, '_notify_sbp_recurring', AsyncMock())

        async def call(number, *, session=None):
            return await agent.process_platega_subscription_callback(
                session or db, dict(SubscriptionId=REMOTE, Id=f'charge-{number}', Status='CONFIRMED')
            )
    elif method == 'cashera':
        from app.services.payment import cashera

        record = CasheraSubscription(
            **common, interval='monthly', external_id='test-order', cashera_subscription_uuid=REMOTE
        )
        agent = cashera.CasheraPaymentMixin()
        monkeypatch.setattr(agent, '_notify_cashera_recurring', AsyncMock())
        monkeypatch.setattr(cashera.cashera_service, 'cancel_subscription', AsyncMock())

        async def call(number, *, session=None):
            return await agent.process_cashera_recurring_charge(
                session or db,
                dict(
                    subscription={'uuid': REMOTE},
                    transaction={'uuid': f'charge-{number}', 'status': 'paid', 'currency': 'RUB', 'amount': 2000},
                ),
            )
    else:
        from app.config import settings
        from app.services import cispay_recurrent_service as cispay
        from app.services.cispay_recurrent import cispay_tariff_terms
        from app.services.notification_delivery_service import notification_delivery_service

        monkeypatch.setattr(settings, 'CISPAY_SHOP_ID', 'shop')
        monkeypatch.setattr(notification_delivery_service, 'notify_autopay_success', AsyncMock())
        monkeypatch.setattr(notification_delivery_service, 'notify_balance_topup', AsyncMock())
        record = CisPaySubscription(
            user_id=payer or sub.user_id,
            owner_id=payer or sub.user_id,
            subscription_id=sub.id,
            shop_id='shop',
            provider_id=REMOTE,
            checkout_id='00000000-0000-4000-8000-000000000001',
            status='ACTIVE',
            purpose='Test',
            snapshot={
                'tariff_id': sub.tariff_id,
                'device_limit': sub.device_limit,
                'period_days': 30,
                'amount_kopeks': 2000,
                'tariff_terms': cispay_tariff_terms(await db.get(Tariff, sub.tariff_id)),
            },
        )

        async def call(number, *, session=None):
            return await cispay.process_cispay_subscription_charge(
                session or db,
                dict(
                    id=f'00000000-0000-4000-8000-{number:012d}',
                    subscription_id=REMOTE,
                    store_id='shop',
                    payment_method='SBP',
                    status='PAID',
                    amount=2000,
                    currency='RUB',
                    is_sandbox=False,
                    paid_at=sub.start_date.isoformat(),
                ),
            )

    if method == 'lava':
        call.cancel = lava.lava_service.unsubscribe_recurrent
    elif method == 'platega':
        call.cancel = agent.platega_service.cancel_subscription
    elif method == 'cashera':
        call.cancel = cashera.cashera_service.cancel_subscription
    db.add(record)
    await db.commit()
    return call, record, delivery


@pytest.mark.parametrize('method', METHODS)
async def test_callback_grants_once_and_old_redelivery_preserves_wallet(postgres_database, monkeypatch, method):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        call, record, delivery = await adapter(db, monkeypatch, method, sub)
        await call(11)
        await call(11)
        await call(12)
        await call(11)
        await db.refresh(sub)
        assert sub.end_date == old_end + timedelta(days=60)
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 2
        assert delivery.await_count == 2
        rows = (await db.scalars(select(Transaction))).all()
        assert all(t.amount_kopeks == -2000 and t.payment_method == method for t in rows)
        if method == 'cispay':
            assert all(t.external_id.startswith('cisrec:') for t in rows)


@pytest.mark.parametrize('method', METHODS)
@pytest.mark.parametrize('conflict', ['pending', 'policy', 'foreign'])
async def test_collected_payment_is_credited_once_when_target_changed(postgres_database, monkeypatch, method, conflict):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        payer = sub.user_id
        if conflict == 'foreign':
            user = User(id=99, telegram_id=9002, status='active', balance_kopeks=10000)
            db.add(user)
            await db.flush()
            payer = user.id
        call, record, delivery = await adapter(db, monkeypatch, method, sub, payer=payer)
        if conflict == 'pending':
            sub.traffic_accounting_mode = 'split_pending'
        elif conflict == 'policy':
            tariff = await db.get(Tariff, sub.tariff_id)
            tariff.allowed_squads = []
        await db.commit()
        await call(21)
        await call(21)
        await db.refresh(sub)
        assert sub.end_date == old_end
        assert (await db.get(User, payer)).balance_kopeks == 12000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        tx = await db.scalar(select(Transaction))
        assert tx.type == 'deposit' and tx.amount_kopeks == 2000 and tx.user_id == payer
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0
        assert record.status == ('CANCEL_PENDING' if method == 'cispay' else 'CANCELLED')
        delivery.assert_not_awaited()
        from app.services.notification_delivery_service import notification_delivery_service

        notification_delivery_service.notify_balance_topup.assert_awaited_once()


@pytest.mark.parametrize('method', METHODS)
async def test_parallel_callback_serializes_account_before_agreement(postgres_database, monkeypatch, method):
    import asyncio

    from sqlalchemy.ext.asyncio import AsyncSession

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        call, _, delivery = await adapter(db, monkeypatch, method, sub)
        async with AsyncSession(db.bind, expire_on_commit=False) as other:
            await asyncio.wait_for(asyncio.gather(call(31), call(31, session=other)), timeout=10)
        await db.refresh(sub)
        assert sub.end_date == old_end + timedelta(days=30)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 1
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        delivery.assert_awaited_once()


@pytest.mark.parametrize('method', ['lava', 'platega', 'cashera'])
@pytest.mark.parametrize('period', [1, 30])
async def test_daily_recurrent_uses_daily_reset_policy_and_agreed_period(
    postgres_database, monkeypatch, method, period
):
    from app.database.models import BsTrafficAllowance
    from tests.database.test_bs_daily_renewal_postgres import prepared as daily_prepared

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, _ = await daily_prepared(db, monkeypatch, reset=True, strategy='DAY')
        before = {row.id: row.remaining_bytes for row in (await db.scalars(select(BsTrafficAllowance))).all()}
        call, record, _ = await adapter(db, monkeypatch, method, sub)
        record.charge_days = period
        await db.commit()
        await call(41)
        await db.refresh(sub)
        assert sub.last_daily_charge_at is not None
        assert sub.end_date == sub.last_daily_charge_at + timedelta(days=period)
        assert {row.id: row.remaining_bytes for row in (await db.scalars(select(BsTrafficAllowance))).all()} == before
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        receipt = await db.scalar(select(BsTrafficRenewalIntent))
        assert receipt.parameters['kind'] == 'provider_daily'


@pytest.mark.parametrize('method', METHODS)
async def test_callback_commit_failure_rolls_back_period_receipt_and_provider_metadata(
    postgres_database, monkeypatch, method
):
    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        old_end, sid, uid = sub.end_date, sub.id, sub.user_id
        call, record, delivery = await adapter(db, monkeypatch, method, sub)
        record_id = record.id
        real_commit = db.commit
        monkeypatch.setattr(db, 'commit', AsyncMock(side_effect=RuntimeError('synthetic precommit failure')))
        with pytest.raises(RuntimeError, match='synthetic precommit failure'):
            await call(51)
        await db.rollback()
        monkeypatch.setattr(db, 'commit', real_commit)
        assert (await db.get(type(sub), sid)).end_date == old_end
        assert (await db.get(User, uid)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0
        await db.refresh(record)
        if method != 'cispay':
            assert record.last_charge_external_id is None and record.charges_success == 0
        delivery.assert_not_awaited()
        assert record.id == record_id
        await call(51)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_platega_late_activation_does_not_undo_credit_cancellation(postgres_database, monkeypatch):
    from app.services.payment.platega import PlategaPaymentMixin

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        call, record, _ = await adapter(db, monkeypatch, 'platega', sub)
        sub.traffic_accounting_mode = 'split_pending'
        await db.commit()
        await call(61)
        await PlategaPaymentMixin().process_platega_subscription_callback(
            db,
            {'SubscriptionId': REMOTE, 'Status': 'SUBSCRIPTION_ACTIVATED'},
        )
        assert record.status == 'CANCELLED'
        assert (await db.get(User, sub.user_id)).balance_kopeks == 12000


async def test_cashera_history_replay_uses_same_split_receipt(postgres_database, monkeypatch):
    from app.services.payment import cashera

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        call, record, delivery = await adapter(db, monkeypatch, 'cashera', sub)
        monkeypatch.setattr(
            cashera.cashera_service,
            'list_subscription_charges',
            AsyncMock(
                return_value=[
                    {'uuid': 'charge-71', 'status': 'paid', 'currency': 'RUB', 'amount': 2000},
                ]
            ),
        )
        agent = cashera.CasheraPaymentMixin()
        monkeypatch.setattr(agent, '_notify_cashera_recurring', AsyncMock())
        assert await agent.replay_missed_cashera_charges(db, record_id=record.id) == 1
        await call(71)
        assert sub.end_date == old_end + timedelta(days=30)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        delivery.assert_awaited_once()


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_platega_metrics_cannot_create_synthetic_split_payment(postgres_database, monkeypatch, mode):
    from datetime import UTC, datetime

    from app.services.payment.platega import replay_missed_platega_charges

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        old_end = sub.end_date
        call, record, delivery = await adapter(db, monkeypatch, 'platega', sub)
        sub.traffic_accounting_mode = mode
        await db.commit()
        remote = {
            'chargeMetrics': {'chargesSuccess': 1, 'lastChargeAt': (datetime.now(UTC) - timedelta(days=1)).isoformat()}
        }
        assert await replay_missed_platega_charges(db, record, remote) == 0
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert sub.end_date == old_end
        await call(81)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        if mode == 'split':
            assert sub.end_date == old_end + timedelta(days=30)
            delivery.assert_awaited_once()
        else:
            assert sub.end_date == old_end
            assert (await db.get(User, sub.user_id)).balance_kopeks == 12000
            delivery.assert_not_awaited()


@pytest.mark.parametrize('method', ['lava', 'platega', 'cashera'])
async def test_credit_survives_remote_cancel_failure_and_remains_in_retry_sweep(postgres_database, monkeypatch, method):
    from datetime import UTC, datetime
    from importlib import import_module

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub = await prepared(db, monkeypatch)
        call, record, delivery = await adapter(db, monkeypatch, method, sub)
        call.cancel.side_effect = TimeoutError
        sub.traffic_accounting_mode = 'split_pending'
        await db.commit()
        await call(91)
        await call(91)
        assert record.status == 'CANCELLED'
        assert (await db.get(User, sub.user_id)).balance_kopeks == 12000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        crud = import_module(f'app.database.crud.{method}_subscription')
        sweep = getattr(crud, f'list_recently_cancelled_{method}_subscriptions')
        assert record in await sweep(db, datetime.now(UTC) - timedelta(days=1))
        call.cancel.assert_awaited_once()
        delivery.assert_not_awaited()
