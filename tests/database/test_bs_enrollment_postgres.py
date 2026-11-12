"""Paid enrollment freezes its funding/terms in the same real SQL transaction."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.crud.transaction import create_transaction
from app.database.crud.user import subtract_user_balance
from app.database.models import Base, PaymentMethod, Subscription, Tariff, TransactionType, User
from app.services.bs_traffic.ledger import LedgerConflict
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
TABLES = list(Base.metadata.sorted_tables)
ORDINARY = '11111111-1111-4111-8111-111111111111'
BS = '22222222-2222-4222-8222-222222222222'


async def paid(db, monkeypatch):
    monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: True)
    monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: True)
    user = User(id=1, status='active', balance_kopeks=10000)
    tariff = Tariff(
        id=1,
        name='Synthetic paid BS tariff',
        traffic_limit_gb=10,
        device_limit=2,
        allowed_squads=[ORDINARY],
        period_prices={'30': 2000},
        traffic_reset_mode='DAY',
        bs_traffic_settings={'enabled': True, 'squad_uuids': [BS], 'limit_gb': 5},
    )
    db.add_all([user, tariff])
    await db.flush()
    sub = Subscription(
        user_id=user.id,
        tariff_id=tariff.id,
        status='active',
        is_trial=False,
        traffic_limit_gb=10,
        device_limit=2,
        connected_squads=[ORDINARY],
        start_date=datetime.now(UTC),
        end_date=datetime.now(UTC) + timedelta(days=30),
    )
    db.add(sub)
    await db.flush()
    assert await subtract_user_balance(db, user, 2000, 'Synthetic purchase', commit=False)
    tx = await create_transaction(
        db,
        user_id=user.id,
        type=TransactionType.SUBSCRIPTION_PAYMENT,
        amount_kopeks=2000,
        description='Synthetic purchase',
        payment_method=PaymentMethod.BALANCE,
        external_id='synthetic-enrollment',
        commit=False,
    )
    return user, tariff, sub, tx


async def test_paid_enrollment_and_frozen_terms_commit_with_money(postgres_database, monkeypatch):
    from app.database.models import BsTrafficEnrollment
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        user, tariff, sub, tx = await paid(db, monkeypatch)
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        await db.commit()
        assert job.state == 'pending' and job.transaction_id == tx.id
        assert job.terms['bs_limit_bytes'] == 5 * 1024**3
        assert job.terms['paid_until'] == sub.end_date.isoformat()
        assert user.balance_kopeks == 8000 and sub.traffic_accounting_mode == 'legacy'
        from app.services.traffic_accounting_service import traffic_accounting_summary

        assert (await traffic_accounting_summary(db, sub.id))['traffic_accounting_mode'] == 'split_pending'
        tariff.bs_traffic_settings = dict(tariff.bs_traffic_settings, limit_gb=99)
        await db.commit()
        again = await stage_paid_enrollment(db, sub, transaction=tx)
        assert again.id == job.id and again.terms['bs_limit_bytes'] == 5 * 1024**3
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 1


async def test_rollback_removes_money_and_enrollment_together(postgres_database, monkeypatch):
    from app.database.models import BsTrafficEnrollment
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, tx = await paid(db, monkeypatch)
        await stage_paid_enrollment(db, sub, transaction=tx)
        await db.rollback()
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 0
        assert await db.scalar(select(func.count()).select_from(User)) == 0


@pytest.mark.parametrize('change', ['trial', 'disabled_bs', 'classic'])
async def test_ineligible_purchase_does_not_enroll(postgres_database, monkeypatch, change):
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        _, tariff, sub, tx = await paid(db, monkeypatch)
        if change == 'trial':
            sub.is_trial = True
        elif change == 'disabled_bs':
            tariff.bs_traffic_settings = dict(tariff.bs_traffic_settings, enabled=False)
        else:
            sub.tariff_id = None
        assert await stage_paid_enrollment(db, sub, transaction=tx) is None


@pytest.mark.parametrize('change', ['foreign', 'unconfirmed', 'deposit'])
async def test_invalid_payment_cannot_finance_bs(postgres_database, monkeypatch, change):
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, tx = await paid(db, monkeypatch)
        if change == 'foreign':
            db.add(User(id=2, status='active'))
            await db.flush()
            tx.user_id = 2
        elif change == 'unconfirmed':
            tx.is_completed = False
        else:
            tx.type = 'deposit'
        with pytest.raises(LedgerConflict):
            await stage_paid_enrollment(db, sub, transaction=tx)


async def bound_panel(db, sub, monkeypatch, *, used=0):
    from dataclasses import replace
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.external.remnawave_api import (
        RemnaWaveUser,
        SplitAccessSnapshot,
        SplitGraceSnapshot,
        SplitTransferSnapshot,
        TrafficLimitStrategy,
        UserStatus,
        UserTraffic,
    )
    from app.services.bs_traffic.panel_binding import bind_reader, register_binding
    from tests.database.test_bs_traffic_ledger_postgres import SCOPE

    sub.remnawave_id = 12
    now = datetime.now(UTC) - timedelta(days=2)
    panel = RemnaWaveUser(
        id=12,
        short_uuid='synthetic-short',
        username='synthetic-account',
        status=UserStatus.ACTIVE,
        traffic_limit_bytes=sub.traffic_limit_gb * 1024**3,
        traffic_limit_strategy=TrafficLimitStrategy.NO_RESET,
        expire_at=sub.end_date,
        telegram_id=None,
        email=None,
        hwid_device_limit=2,
        description=None,
        tag=None,
        subscription_url='https://subscription.example/synthetic',
        active_internal_squads=[{'uuid': ORDINARY}],
        created_at=now,
        updated_at=now,
        user_traffic=UserTraffic(used, used),
        last_traffic_reset_at=now,
        last_traffic_reset_at_known=True,
    )
    remote = {'state': panel}

    def transfer(_):
        p = remote['state']
        return SplitTransferSnapshot(
            p.id,
            p.created_at,
            p.active_internal_squads,
            p.used_traffic_bytes,
            p.lifetime_used_traffic_bytes,
            p.traffic_limit_bytes,
            getattr(p.traffic_limit_strategy, 'value', p.traffic_limit_strategy),
        )

    def grace(_):
        p = remote['state']
        return SplitGraceSnapshot(
            p.id,
            p.created_at,
            p.active_internal_squads,
            getattr(p.status, 'value', p.status),
            p.expire_at,
            p.traffic_limit_bytes,
            getattr(p.traffic_limit_strategy, 'value', p.traffic_limit_strategy),
            p.used_traffic_bytes,
            p.hwid_device_limit,
            p.external_squad_uuid,
            p.last_traffic_reset_at,
        )

    async def update(**kwargs):
        assert kwargs.pop('user_id') == 12
        if 'active_internal_squads' in kwargs:
            kwargs['active_internal_squads'] = [
                {'uuid': x} if isinstance(x, str) else x for x in kwargs['active_internal_squads']
            ]
        remote['state'] = replace(remote['state'], **kwargs)
        return remote['state']

    async def restore(target):
        p = remote['state']
        remote['state'] = replace(
            p,
            status=UserStatus(target.status),
            expire_at=target.expire_at,
            traffic_limit_bytes=target.traffic_limit_bytes,
            traffic_limit_strategy=TrafficLimitStrategy(target.traffic_limit_strategy),
            active_internal_squads=target.active_internal_squads,
            hwid_device_limit=target.hwid_device_limit,
            external_squad_uuid=target.external_squad_uuid,
        )

    async def squads(user_id, values):
        return await update(user_id=user_id, active_internal_squads=list(values))

    api = SimpleNamespace(
        base_url='https://panel.example',
        get_user_devices_all=AsyncMock(return_value={'devices': [], 'total': 0}),
        get_user_by_id=AsyncMock(side_effect=lambda _: remote['state']),
        update_user=AsyncMock(side_effect=update),
        restore_split_user_once=AsyncMock(side_effect=restore),
        update_internal_squads_once=AsyncMock(side_effect=squads),
        get_split_transfer_snapshot=AsyncMock(side_effect=transfer),
        get_split_grace_snapshot=AsyncMock(side_effect=grace),
        get_split_access_snapshot=AsyncMock(
            side_effect=lambda _: SplitAccessSnapshot(12, now, remote['state'].active_internal_squads)
        ),
    )
    await register_binding(db, panel_scope=SCOPE, endpoint=api.base_url)
    await db.commit()
    reader = await bind_reader(async_sessionmaker(db.bind, expire_on_commit=False), api, panel_scope=SCOPE)
    return reader, remote


@pytest.mark.parametrize('ordinary_gb', [10, 0])
async def test_ordinary_source_preserves_counters_and_applies_funded_base(postgres_database, monkeypatch, ordinary_gb):
    from app.database.models import TrafficAccountingState, TrafficAllowance
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_source import prepare_ordinary_source

    async with postgres_session(postgres_database, TABLES) as db:
        _, tariff, sub, tx = await paid(db, monkeypatch)
        tariff.traffic_limit_gb = sub.traffic_limit_gb = ordinary_gb
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        reader, remote = await bound_panel(db, sub, monkeypatch, used=12 * 1024**3)
        await prepare_ordinary_source(reader, job.id)
        await db.refresh(job)
        await db.refresh(sub)
        assert job.state == 'provisioned' and sub.traffic_accounting_mode == 'managed'
        assert remote['state'].used_traffic_bytes == 12 * 1024**3
        assert remote['state'].lifetime_used_traffic_bytes == 12 * 1024**3
        assert remote['state'].traffic_limit_strategy.value == 'NO_RESET'
        assert remote['state'].traffic_limit_bytes == (22 * 1024**3 if ordinary_gb else 0)
        before = remote['state'].traffic_limit_bytes
        await prepare_ordinary_source(reader, job.id)
        assert remote['state'].traffic_limit_bytes == before
        source = await db.get(TrafficAccountingState, sub.id, populate_existing=True)
        assert source.phase == ('ready' if ordinary_gb else 'unlimited')
        if ordinary_gb:
            assert await db.scalar(select(func.sum(TrafficAllowance.remaining_bytes))) == 10 * 1024**3


@pytest.mark.parametrize('stop_bs_sales', [False, True])
@pytest.mark.parametrize('unknown_cutover', [False, True])
@pytest.mark.parametrize('multi_mode', [False, True])
async def test_funded_enrollment_resumes_cutover_and_delivers_once(
    postgres_database, monkeypatch, stop_bs_sales, unknown_cutover, multi_mode
):
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_delivery import _transfer, advance_enrollment
    from app.services.bs_traffic.enrollment_source import prepare_ordinary_source
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        user, tariff, sub, tx = await paid(db, monkeypatch)
        monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: multi_mode)
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        reader, remote = await bound_panel(db, sub, monkeypatch, used=12 * 1024**3)
        if not multi_mode:
            user.remnawave_id = sub.remnawave_id
            await db.commit()
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
        if stop_bs_sales:
            tariff.bs_traffic_settings = dict(
                tariff.bs_traffic_settings, enabled=False, limit_gb=99, disabled_at=datetime.now(UTC).isoformat()
            )
            await db.commit()
        await prepare_ordinary_source(reader, job.id)
        await _transfer(reader, job.id)
        await db.refresh(job)
        keys = dict(user_id=user.id, subscription_id=sub.id, account_generation=job.account_generation)
        # Missing statistics cannot silently become a zero baseline.
        assert await advance_enrollment(reader, enrollment_id=job.id) != 'verified'
        for node in (NODE_A, NODE_B):
            await observe(db, reader, keys, node=node, amount=0)
        await db.commit()
        if unknown_cutover:
            original_restore = reader._api.restore_split_user_once.side_effect
            original_get = reader._api.get_split_grace_snapshot.side_effect

            async def unknown(target):
                await original_restore(target)
                remote['fail_read_once'] = True
                raise TimeoutError

            async def recover_later(panel_id):
                if remote.pop('fail_read_once', False):
                    raise TimeoutError
                return original_get(panel_id)

            reader._api.get_split_grace_snapshot.side_effect = recover_later

            reader._api.restore_split_user_once.side_effect = unknown
            assert await advance_enrollment(reader, enrollment_id=job.id) == 'unconfirmed'
        result = await advance_enrollment(reader, enrollment_id=job.id)
        assert result == 'verified'
        assert reader._api.restore_split_user_once.await_count == 1
        await db.refresh(sub)
        await db.refresh(job)
        assert sub.traffic_accounting_mode == 'split' and job.state == 'verified'
        assert remote['state'].used_traffic_bytes == 12 * 1024**3
        assert remote['state'].traffic_limit_bytes == 0
        assert {s['uuid'] for s in remote['state'].active_internal_squads} == {ORDINARY, BS}
        assert (
            await db.scalar(
                select(func.sum(BsTrafficAllowance.remaining_bytes)).where(BsTrafficAllowance.traffic_group == 'bs')
            )
            == 5 * 1024**3
        )
        assert await advance_enrollment(reader, enrollment_id=job.id) == 'already_verified'
        assert user.balance_kopeks == 8000

        from app.services.bs_traffic.calendar_renewal import refresh_calendar_base

        if stop_bs_sales:
            from app.services.bs_traffic.renewal import renew_from_balance

            renewed = await renew_from_balance(
                db,
                **keys,
                period_days=30,
                expected_price_kopeks=2000,
                operation_key='synthetic-ordinary-only-after-enrollment',
            )
            await db.commit()
            assert renewed.intent_id is not None
            await db.refresh(sub)
            assert sub.end_date > datetime.fromisoformat(job.terms['paid_until'])

        due = datetime.fromisoformat(job.calendar_terms['next_refresh_at'])
        result = await refresh_calendar_base(db, **keys, now=due + timedelta(seconds=1))
        assert set(result.grants) == {'ordinary', 'bs'}
        assert result.next_refresh_at is not None
        bs_grant = await db.get(BsTrafficAllowance, result.grants['bs'])
        assert bs_grant.expires_at <= datetime.fromisoformat(job.terms['paid_until'])
        await db.commit()
        again = await refresh_calendar_base(db, **keys, now=due + timedelta(seconds=2))
        assert not again.grants


async def test_cabinet_checkout_freezes_enrollment_and_retry_keeps_single_payment(postgres_database, monkeypatch):
    from app.database.models import BsTrafficEnrollment, Transaction
    from tests.database.test_tariff_purchase_devices_postgres import checkout_context, order

    async with checkout_context(postgres_database, monkeypatch) as checkout:
        async with checkout.maker() as db:
            tariff = await db.get(Tariff, 7)
            tariff.allowed_squads = [ORDINARY]
            tariff.bs_traffic_settings = {'enabled': True, 'squad_uuids': [BS], 'limit_gb': 5}
            await db.commit()
        body = await order(checkout)
        first = await checkout.send('/purchase-tariff', body)
        assert first.status_code == 200, first.text
        assert first.json()['delivery_pending'] is True
        assert first.json()['subscription']['traffic_accounting_mode'] == 'split_pending'
        checkout.stored.clear()
        retry = await checkout.send('/purchase-tariff', body)
        assert retry.status_code == 200, retry.text
        assert retry.json()['delivery_pending'] is True
        async with checkout.maker() as db:
            job = await db.scalar(select(BsTrafficEnrollment))
            assert job.state == 'pending' and job.terms['bs_limit_bytes'] == 5 * 1024**3
            payment = await db.get(Transaction, job.transaction_id)
            assert payment.is_completed and payment.type == 'subscription_payment'
            assert (await db.get(User, 1)).balance_kopeks == 50000
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
            # Another checkout cannot mutate the still-unresolved funded target.
            second = await checkout.send(
                '/purchase-tariff/preview',
                {
                    'tariff_id': 7,
                    'period_days': 30,
                    'selected_device_limit': 3,
                },
            )
            assert second.status_code == 409, second.text


async def test_guest_receipt_and_admin_issue_have_distinct_real_provenance(postgres_database, monkeypatch):
    from app.database.models import GuestPurchase
    from app.services.bs_traffic.enrollment import stage_admin_enrollment, stage_guest_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        user, tariff, sub, _ = await paid(db, monkeypatch)
        guest = GuestPurchase(
            token='synthetic-paid-guest',
            contact_type='email',
            contact_value='buyer@example.com',
            user_id=user.id,
            tariff_id=tariff.id,
            period_days=30,
            amount_kopeks=2000,
            payment_method='platega',
            payment_id='synthetic-guest-charge',
            paid_at=datetime.now(UTC),
            status='paid',
            currency='RUB',
        )
        db.add(guest)
        await db.flush()
        job = await stage_guest_enrollment(db, sub, purchase=guest)
        assert job.source_kind == 'guest' and job.transaction_id is None
        assert job.guest_purchase_id == guest.id
        await db.rollback()

        user, _, sub, _ = await paid(db, monkeypatch)
        with pytest.raises(LedgerConflict, match='Actor cannot'):
            await stage_admin_enrollment(db, sub, actor=user)
        await db.rollback()
        user, _, sub, _ = await paid(db, monkeypatch)
        # Real environment-admin RBAC, not a mocked permission verdict.
        user.telegram_id = 123456789
        monkeypatch.setattr(settings, 'ADMIN_IDS', '123456789')
        job = await stage_admin_enrollment(db, sub, actor=user)
        assert job.source_kind == 'admin' and job.transaction_id is None
        assert job.actor_id == user.id
        await db.commit()


async def test_guest_activation_commits_pending_enrollment_before_panel_and_is_idempotent(
    postgres_database, monkeypatch
):
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficEnrollment, GuestPurchase, Transaction
    from app.services import guest_purchase_service as service
    from app.services.subscription_service import SubscriptionService

    monkeypatch.setattr(SubscriptionService, 'create_remnawave_user', AsyncMock(return_value=None))
    monkeypatch.setattr(service, '_send_admin_notification', AsyncMock())
    async with postgres_session(postgres_database, TABLES) as db:
        user, tariff, sub, tx = await paid(db, monkeypatch)
        await db.delete(tx)
        await db.delete(sub)
        user.balance_kopeks = 10000
        guest = GuestPurchase(
            token='synthetic-gift-activation',
            contact_type='email',
            contact_value='buyer@example.com',
            user_id=user.id,
            tariff_id=tariff.id,
            period_days=30,
            amount_kopeks=2000,
            payment_method='platega',
            payment_id='synthetic-paid-gift',
            is_gift=True,
            paid_at=datetime.now(UTC),
            status='pending_activation',
            currency='RUB',
        )
        db.add(guest)
        await db.commit()
        result = await service.activate_purchase(db, guest.token, skip_notification=True)
        assert result.status == 'delivered'
        job = await db.scalar(select(BsTrafficEnrollment))
        assert job.state == 'pending' and job.guest_purchase_id == guest.id
        new_sub = await db.get(Subscription, job.subscription_id)
        paid_end = new_sub.end_date
        assert (await service.activate_purchase(db, guest.token, skip_notification=True)).id == guest.id
        await db.refresh(new_sub)
        assert new_sub.end_date == paid_end and user.balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('method', ['lava', 'platega', 'cashera', 'cispay'])
async def test_first_recurrent_charge_stages_initial_enrollment_once(postgres_database, monkeypatch, method):
    from unittest.mock import AsyncMock

    from app.database.models import BsTrafficEnrollment, Transaction
    from app.services import traffic_base_grant_service
    from app.services.subscription_service import SubscriptionService
    from tests.database.test_bs_recurrent_callbacks_postgres import adapter

    real_stage = traffic_base_grant_service.stage_paid_base_grant
    async with postgres_session(postgres_database, TABLES) as db:
        user, _, sub, tx = await paid(db, monkeypatch)
        await db.delete(tx)
        user.balance_kopeks = 10000
        await db.commit()
        call, _, _ = await adapter(db, monkeypatch, method, sub)
        monkeypatch.setattr(traffic_base_grant_service, 'stage_paid_base_grant', real_stage)
        monkeypatch.setattr(SubscriptionService, 'update_remnawave_user', AsyncMock(return_value=None))
        await call(1)
        job = await db.scalar(select(BsTrafficEnrollment))
        assert job is not None and job.state == 'pending' and job.source_kind == 'payment'
        await db.refresh(sub)
        end = sub.end_date
        await call(1)
        await db.refresh(sub)
        await db.refresh(user)
        assert sub.end_date == end and user.balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 1
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_enrollment_migration_refuses_to_discard_funded_work(postgres_database, monkeypatch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from tests.database.test_retired_gateway_migration_postgres import _load

    migration = _load('0161_bs_enrollment.py')
    async with postgres_session(postgres_database, TABLES) as db:

        def install(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()
                _load('0162_bs_ownership_cycles.py').upgrade()

        await (await db.connection()).run_sync(install)
        _, _, sub, tx = await paid(db, monkeypatch)
        await stage_paid_enrollment(db, sub, transaction=tx)
        await db.commit()

        def refuse(sync):
            with Operations.context(MigrationContext.configure(sync)), pytest.raises(RuntimeError, match='funded'):
                migration.downgrade()

        await (await db.connection()).run_sync(refuse)


async def test_second_epoch_cannot_replace_binding_before_enrollment(postgres_database, monkeypatch):
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.panel_binding import register_binding

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, tx = await paid(db, monkeypatch)
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        reader, _ = await bound_panel(db, sub, monkeypatch)
        with pytest.raises(LedgerConflict, match='already registered'):
            await register_binding(
                db, panel_scope='99999999-9999-4999-8999-999999999999', endpoint=reader._api.base_url
            )
        await db.rollback()
        await db.refresh(job)
        assert job.panel_scope is None and job.state == 'pending'
        reader._api.get_split_transfer_snapshot.assert_not_called()


@pytest.mark.parametrize('channel', ['telegram_auto', 'miniapp'])
async def test_paid_purchase_adapters_atomically_stage_initial_bs(postgres_database, monkeypatch, channel):
    from unittest.mock import AsyncMock

    from app.database.crud.user import get_user_by_id
    from app.database.models import BsTrafficEnrollment, Transaction
    from tests.database.test_tariff_purchase_devices_postgres import checkout_context

    async with checkout_context(postgres_database, monkeypatch) as checkout:
        async with checkout.maker() as db:
            tariff = await db.get(Tariff, 7)
            tariff.allowed_squads = [ORDINARY]
            tariff.bs_traffic_settings = {'enabled': True, 'squad_uuids': [BS], 'limit_gb': 5}
            await db.commit()
            user = await get_user_by_id(db, 1)
            if channel == 'telegram_auto':
                from app.services.subscription_auto_purchase_service import _auto_purchase_tariff

                assert await _auto_purchase_tariff(db, user, {'tariff_id': 7, 'period_days': 30})
            else:
                from app.webapi.routes import miniapp

                monkeypatch.setattr(miniapp, '_authorize_miniapp_user', AsyncMock(return_value=user))
                result = await miniapp.purchase_tariff_endpoint(
                    miniapp.MiniAppTariffPurchaseRequest(initData='synthetic', tariffId=7, periodDays=30),
                    db,
                )
                assert result.success
            await db.refresh(user)
            job = await db.scalar(select(BsTrafficEnrollment))
            assert job is not None and job.state == 'pending'
            assert user.balance_kopeks == 70000
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('entry', ['staging', 'shared_keep', 'shared_reset'])
async def test_new_real_payment_reenrolls_verified_ordinary_return_without_reset(postgres_database, monkeypatch, entry):
    from dataclasses import replace
    from unittest.mock import AsyncMock

    from app.database.models import (
        BsTrafficEnrollment,
        BsTrafficLedger,
        BsTrafficOwnershipCycle,
        TrafficAccountingState,
        TrafficAllowance,
    )
    from app.services.bs_traffic.cutover_activation import activate_cutover
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.reverse_delivery import advance_reverse
    from app.services.traffic_accounting_sync import stage_managed_sync
    from tests.database.test_bs_traffic_cutover_activation_postgres import ready as activated_ready

    monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: True)
    monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: True)
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, plan, op = await activated_ready(db, monkeypatch)
        await db.commit()
        assert await activate_cutover(reader, **keys, operation_id=op) == 'activated'
        ledger = await db.get(BsTrafficLedger, 1, populate_existing=True)
        ledger.access_policy = dict(
            ledger.access_policy,
            bs_purchase_enabled=False,
            bs_drain_until=(datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        await db.commit()
        current = reader._api.get_split_grace_snapshot.return_value
        counters = reader._api.get_split_transfer_snapshot.return_value
        reader._api.get_split_transfer_snapshot.return_value = replace(
            counters, used_bytes=current.used_traffic_bytes, traffic_limit_bytes=0
        )

        async def patch(target):
            reader._api.get_split_grace_snapshot.return_value = target
            reader._api.get_split_transfer_snapshot.return_value = replace(
                counters, used_bytes=target.used_traffic_bytes, traffic_limit_bytes=target.traffic_limit_bytes
            )

        import app.services.bs_traffic.reverse_delivery as reverse

        monkeypatch.setattr(reverse, 'collect_topology_evidence', AsyncMock(return_value=plan.evidence))
        reader._api.return_split_to_managed_once = AsyncMock(side_effect=patch)
        assert await advance_reverse(reader, **keys) == 'managed'
        await db.rollback()
        sub = await db.get(Subscription, 1)
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.allowed_squads = ledger.access_policy['topology']['ordinary_squads']
        tariff.bs_traffic_settings = dict(
            enabled=True, squad_uuids=ledger.access_policy['topology']['bs_squads'], limit_gb=5
        )
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 10000
        await db.flush()
        before = await db.scalar(
            select(func.sum(TrafficAllowance.remaining_bytes)).where(TrafficAllowance.retired_at.is_(None))
        )
        assert before == 800
        if entry == 'staging':
            assert await subtract_user_balance(db, user, 2000, 'Synthetic re-enable', commit=False)
            tx = await create_transaction(
                db,
                user_id=user.id,
                type=TransactionType.SUBSCRIPTION_PAYMENT,
                amount_kopeks=2000,
                description='Synthetic re-enable',
                payment_method=PaymentMethod.BALANCE,
                external_id='synthetic-cycle1',
                commit=False,
            )
            sub.end_date += timedelta(days=30)
            await stage_managed_sync(db, sub, only_fields={'expire_at', 'status'})
            job = await stage_paid_enrollment(db, sub, transaction=tx)
        else:
            from app.services.pricing_engine import pricing_engine
            from app.services.subscription_renewal_service import SubscriptionRenewalService
            from app.services.subscription_service import SubscriptionService

            tariff.period_prices = {'30': 2000}
            tariff.traffic_limit_gb = sub.traffic_limit_gb
            monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', entry == 'shared_reset')
            monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
            monkeypatch.setattr(SubscriptionService, 'update_remnawave_user', AsyncMock(return_value=None))
            monkeypatch.setattr(SubscriptionService, 'create_remnawave_user', AsyncMock(return_value=None))
            monkeypatch.setattr(
                'app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock()
            )
            monkeypatch.setattr('app.database.crud.transaction.emit_transaction_side_effects', AsyncMock())
            await db.flush()
            await db.refresh(sub, ['tariff'])
            price = await pricing_engine.calculate_renewal_price(db, sub, 30, user=user)
            result = await SubscriptionRenewalService().finalize(
                db, user, sub, price, operation_key='synthetic-reenable-key'
            )
            assert result.delivery_pending
            job = await db.scalar(select(BsTrafficEnrollment))
            tx = result.transaction
            before_replay = sub.end_date
            again = await SubscriptionRenewalService().finalize(
                db, user, sub, price, operation_key='synthetic-reenable-key'
            )
            assert again.transaction.id == tx.id and sub.end_date == before_replay
            assert user.balance_kopeks == 8000
        assert job.ownership_cycle == 1 and job.terms['ordinary_source_preexisting']
        assert (await db.get(BsTrafficOwnershipCycle, (sub.id, 0))).reverse_attempt['state'] == 'verified'
        await db.commit()
        assert (await stage_paid_enrollment(db, sub, transaction=tx)).id == job.id
        assert user.balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 1
        # The paid receipt only schedules enrollment. Existing ordinary rights
        # remain intact until their own sender confirms the paid projection.
        from app.database.models import TrafficBaseGrantIntent

        assert (
            await db.scalar(
                select(func.sum(TrafficAllowance.remaining_bytes)).where(TrafficAllowance.retired_at.is_(None))
            )
            == before
        )
        assert await db.scalar(select(func.count()).select_from(TrafficBaseGrantIntent)) == int(entry == 'shared_reset')
        assert (await db.get(TrafficAccountingState, sub.id)).pending_sync_fields


async def test_preexisting_managed_rights_are_not_reset_during_paid_enrollment(postgres_database, monkeypatch):
    from app.database.models import TrafficAllowance, TrafficBaseGrantIntent
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_source import prepare_ordinary_source
    from app.services.traffic_accounting_service import ensure_managed_account

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, tx = await paid(db, monkeypatch)
        reader, remote = await bound_panel(db, sub, monkeypatch, used=2 * 1024**3)
        await ensure_managed_account(reader._factory, reader._api, sub.id, timezone='UTC')
        await db.refresh(sub)
        assert sub.traffic_accounting_mode == 'managed'
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        assert job.terms['ordinary_source_preexisting']
        await db.commit()
        await prepare_ordinary_source(reader, job.id)
        await db.refresh(job)
        assert job.state == 'provisioned'
        assert remote['state'].used_traffic_bytes == 2 * 1024**3
        assert remote['state'].traffic_limit_bytes == 10 * 1024**3
        assert (
            await db.scalar(
                select(func.sum(TrafficAllowance.remaining_bytes)).where(TrafficAllowance.retired_at.is_(None))
            )
            == 8 * 1024**3
        )
        assert await db.scalar(select(func.count()).select_from(TrafficBaseGrantIntent)) == 0


@pytest.mark.parametrize('notification_fails', [False, True])
async def test_pending_purchase_never_falls_back_to_active_notification(
    postgres_database, monkeypatch, notification_fails
):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_customer import notify_pending_if_needed
    from app.services.notification_delivery_service import notification_delivery_service
    from app.services.notification_types import NotificationType

    send = AsyncMock(side_effect=RuntimeError('synthetic transport outage') if notification_fails else None)
    monkeypatch.setattr(notification_delivery_service, 'send_notification', send)
    async with postgres_session(postgres_database, TABLES) as db:
        user, _, sub, tx = await paid(db, monkeypatch)
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        await db.commit()
        assert await notify_pending_if_needed(db, user, sub.id)
        assert send.await_args.kwargs['notification_type'] == NotificationType.SUBSCRIPTION_PENDING
        assert send.await_args.kwargs['context']['delivery_pending'] is True
        job.state = 'verified'
        await db.commit()
        assert not await notify_pending_if_needed(db, user, sub.id)
        assert send.await_count == 1


async def test_historical_funding_replay_does_not_create_a_new_ownership_cycle(postgres_database, monkeypatch):
    from app.database.models import BsTrafficEnrollment
    from app.services.bs_traffic.enrollment import stage_paid_enrollment, terms_hash

    async with postgres_session(postgres_database, TABLES) as db:
        user, _, sub, tx = await paid(db, monkeypatch)
        old = await stage_paid_enrollment(db, sub, transaction=tx)
        old.state = 'verified'
        sub.traffic_accounting_mode = 'managed'
        second_tx = await create_transaction(
            db,
            user_id=user.id,
            type=TransactionType.SUBSCRIPTION_PAYMENT,
            amount_kopeks=2000,
            description='Synthetic later external payment',
            payment_method=PaymentMethod.LAVA,
            external_id='synthetic-cycle-payment',
            commit=False,
        )
        # A retained completed receipt and a later funded cycle coexist. The
        # historical callback must resolve its own receipt, never the latest one.
        terms = dict(
            old.terms,
            source=dict(
                amount_kopeks=second_tx.amount_kopeks,
                payment_method=second_tx.payment_method,
                external_id=second_tx.external_id,
            ),
        )
        newer = BsTrafficEnrollment(
            subscription_id=sub.id,
            user_id=user.id,
            tariff_id=sub.tariff_id,
            ownership_cycle=1,
            source_kind='payment',
            transaction_id=second_tx.id,
            terms=terms,
            terms_hash=terms_hash(terms),
            state='pending',
        )
        db.add(newer)
        await db.commit()
        assert (await stage_paid_enrollment(db, sub, transaction=tx)).id == old.id
        assert (await stage_paid_enrollment(db, sub, transaction=second_tx)).id == newer.id
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 2
        assert newer.state == 'pending' and old.state == 'verified'


@pytest.mark.parametrize('same_owner', [False, True])
async def test_single_tariff_enrollment_rejects_existing_panel_owner(postgres_database, monkeypatch, same_owner):
    from app.database.models import BsTrafficEnrollment
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        user, tariff, sub, tx = await paid(db, monkeypatch)
        monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: False)
        user.remnawave_id = 12
        if not same_owner:
            db.add(User(id=2, status='active'))
            await db.flush()
        db.add(
            Subscription(
                user_id=user.id if same_owner else 2,
                tariff_id=tariff.id,
                status='expired',
                is_trial=False,
                remnawave_id=12,
                remnawave_short_uuid='synthetic-other',
                remnawave_short_id='other',
                start_date=sub.start_date - timedelta(days=60),
                end_date=sub.start_date - timedelta(days=30),
            )
        )
        await db.flush()
        with pytest.raises(LedgerConflict, match='exclusive subscription ownership'):
            await stage_paid_enrollment(db, sub, transaction=tx)
        assert await db.scalar(select(func.count()).select_from(BsTrafficEnrollment)) == 0


async def test_one_payment_cannot_fund_two_subscription_enrollments(postgres_database, monkeypatch):
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        user, tariff, sub, tx = await paid(db, monkeypatch)
        await stage_paid_enrollment(db, sub, transaction=tx)
        second_tariff = Tariff(
            id=2,
            name='Other synthetic tariff',
            allowed_squads=list(tariff.allowed_squads),
            bs_traffic_settings=dict(tariff.bs_traffic_settings),
        )
        db.add(second_tariff)
        await db.flush()
        another = Subscription(
            user_id=user.id,
            tariff_id=second_tariff.id,
            remnawave_short_id='second',
            status='active',
            is_trial=False,
            start_date=sub.start_date,
            end_date=sub.end_date,
        )
        db.add(another)
        await db.flush()
        with pytest.raises(LedgerConflict, match='already funds another'):
            await stage_paid_enrollment(db, another, transaction=tx)
