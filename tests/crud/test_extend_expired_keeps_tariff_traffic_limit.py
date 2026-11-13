"""Продление истёкшей тарифной подписки обязано вернуть лимит трафика ТАРИФА.

Жалоба владельца (2026-09-13): «люди продлевают подписки, и через раз трафик
становится безлимитным, хотя у тарифов есть ограничение». Скриншот: подписка
истекла в 19:51, продлена в 19:53 — и в уведомлении «Трафик: ∞ Безлимит».

Корень: ``extend_subscription`` при включённом ``RESET_TRAFFIC_ON_PAYMENT`` для
истёкшей (или зарезанной по трафику) подписки пересобирал базовый лимит по
правилам КЛАССИЧЕСКОГО режима — из ``FIXED_TRAFFIC_LIMIT_GB`` в фиксированном
режиме выбора трафика, — хотя у подписки есть тариф со своим лимитом. С
``FIXED_TRAFFIC_LIMIT_GB=0`` это и есть «безлимит». Продление ещё живой
подписки в эту ветку не заходит — отсюда «через раз».

Проверка на настоящей БД: тариф, пользователь и подписка лежат в PostgreSQL,
продление идёт через настоящий ``extend_subscription``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.config import settings
from app.database.crud.subscription import extend_subscription
from app.database.models import Base, Subscription, SubscriptionStatus, Tariff, TrafficPurchase, User
from tests.fixtures.postgres_db import postgres_session


TABLES = list(Base.metadata.sorted_tables)
TARIFF_LIMIT_GB = 50
TOPUP_GB = 30


def _user() -> User:
    return User(id=1, telegram_id=1001, first_name='U', language='ru', status='active', balance_kopeks=100_000)


def _tariff(*, traffic_limit_gb: int = TARIFF_LIMIT_GB, custom_traffic_enabled: bool = False) -> Tariff:
    return Tariff(
        id=1,
        name='Тариф',
        description='',
        is_active=True,
        traffic_limit_gb=traffic_limit_gb,
        custom_traffic_enabled=custom_traffic_enabled,
        device_limit=3,
        allowed_squads=['squad-1'],
        period_prices={'30': 10_000},
        display_order=1,
    )


def _subscription(
    *,
    status: str = SubscriptionStatus.EXPIRED.value,
    traffic_limit_gb: int = TARIFF_LIMIT_GB,
    purchased_traffic_gb: int = 0,
    tariff_id: int | None = 1,
    days_from_now: int = -3,
) -> Subscription:
    now = datetime.now(UTC)
    return Subscription(
        id=10,
        remnawave_short_id='sub10',
        user_id=1,
        status=status,
        is_trial=False,
        start_date=now - timedelta(days=33),
        end_date=now + timedelta(days=days_from_now),
        traffic_limit_gb=traffic_limit_gb,
        traffic_used_gb=12.0,
        purchased_traffic_gb=purchased_traffic_gb,
        device_limit=3,
        tariff_id=tariff_id,
        connected_squads=['squad-1'],
    )


def _configure(monkeypatch, *, reset_on_payment: bool, traffic_mode: str, fixed_gb: int) -> None:
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', reset_on_payment)
    monkeypatch.setattr(settings, 'TRAFFIC_SELECTION_MODE', traffic_mode)
    monkeypatch.setattr(settings, 'FIXED_TRAFFIC_LIMIT_GB', fixed_gb)


async def _renew(monkeypatch, *rows, tariff: Tariff | None = None, postgres_database) -> Subscription:
    async with postgres_session(postgres_database, TABLES) as db:
        db.add(_user())
        db.add(tariff or _tariff())
        for row in rows:
            db.add(row)
        await db.commit()
        subscription = await db.get(Subscription, 10)
        assert subscription is not None
        await extend_subscription(db, subscription, 30)
        await db.refresh(subscription)
        return subscription


# ── ядро жалобы: фиксированный режим с нулём даёт безлимит ──


@pytest.mark.postgres
@pytest.mark.parametrize('status', [SubscriptionStatus.EXPIRED.value, SubscriptionStatus.LIMITED.value])
async def test_expired_tariff_renewal_keeps_tariff_limit_when_fixed_limit_is_zero(
    postgres_database, monkeypatch, status
):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)

    subscription = await _renew(monkeypatch, _subscription(status=status), postgres_database=postgres_database)

    assert subscription.status == SubscriptionStatus.ACTIVE.value
    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB
    assert subscription.traffic_used_gb == 0.0


@pytest.mark.postgres
async def test_expired_tariff_renewal_ignores_fixed_limit_value(postgres_database, monkeypatch):
    """И ненулевая глобальная настройка тарифу не указ: у тарифа свой лимит."""
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=100)

    subscription = await _renew(monkeypatch, _subscription(), postgres_database=postgres_database)

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB


@pytest.mark.postgres
async def test_expired_unlimited_tariff_stays_unlimited(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=100)

    subscription = await _renew(
        monkeypatch,
        _subscription(traffic_limit_gb=0),
        tariff=_tariff(traffic_limit_gb=0),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == 0


@pytest.mark.postgres
@pytest.mark.parametrize(
    ('status', 'days_from_now'),
    [(SubscriptionStatus.EXPIRED.value, -3), (SubscriptionStatus.ACTIVE.value, 3)],
)
@pytest.mark.parametrize('reset_on_payment', [True, False])
async def test_unlimited_tariff_renewal_drops_a_leftover_limit(
    postgres_database, monkeypatch, status, days_from_now, reset_on_payment
):
    """Жалоба 2026-09-15: грейс оставил в подписке лимит «расход + 1 ГБ» (103 ГБ)
    на безлимитном тарифе, человек продлил — безлимит не вернулся: для тарифа без
    лимита уборка продления только чистила просроченные докупки и лимит не трогала."""
    _configure(monkeypatch, reset_on_payment=reset_on_payment, traffic_mode='fixed', fixed_gb=100)

    subscription = await _renew(
        monkeypatch,
        _subscription(status=status, days_from_now=days_from_now, traffic_limit_gb=103),
        tariff=_tariff(traffic_limit_gb=0),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == 0
    assert subscription.purchased_traffic_gb == 0


# ── докупки поверх базы тарифа ──


@pytest.mark.postgres
async def test_expired_renewal_keeps_active_topup_on_top_of_tariff_base(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)
    now = datetime.now(UTC)

    subscription = await _renew(
        monkeypatch,
        _subscription(traffic_limit_gb=TARIFF_LIMIT_GB + TOPUP_GB, purchased_traffic_gb=TOPUP_GB),
        TrafficPurchase(subscription_id=10, traffic_gb=TOPUP_GB, expires_at=now + timedelta(days=20)),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB + TOPUP_GB
    assert subscription.purchased_traffic_gb == TOPUP_GB


@pytest.mark.postgres
async def test_expired_renewal_drops_expired_topup_and_returns_to_tariff_base(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)
    now = datetime.now(UTC)

    subscription = await _renew(
        monkeypatch,
        _subscription(traffic_limit_gb=TARIFF_LIMIT_GB + TOPUP_GB, purchased_traffic_gb=TOPUP_GB),
        TrafficPurchase(subscription_id=10, traffic_gb=TOPUP_GB, expires_at=now - timedelta(days=5)),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB
    assert subscription.purchased_traffic_gb == 0


# ── уже испорченная подписка выправляется при следующем продлении ──


@pytest.mark.postgres
async def test_expired_renewal_heals_zeroed_limit_in_selectable_mode(postgres_database, monkeypatch):
    """Подписка, которой прошлый баг уже выдал безлимит, при продлении возвращается к тарифу."""
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='selectable', fixed_gb=100)

    subscription = await _renew(monkeypatch, _subscription(traffic_limit_gb=0), postgres_database=postgres_database)

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB


# ── тариф с произвольным трафиком: база выбрана при покупке ──


@pytest.mark.postgres
async def test_custom_traffic_tariff_keeps_the_chosen_base(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)

    subscription = await _renew(
        monkeypatch,
        _subscription(traffic_limit_gb=75),
        tariff=_tariff(traffic_limit_gb=100, custom_traffic_enabled=True),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == 75


@pytest.mark.postgres
async def test_custom_traffic_tariff_falls_back_to_tariff_default_when_base_is_lost(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)

    subscription = await _renew(
        monkeypatch,
        _subscription(traffic_limit_gb=0),
        tariff=_tariff(traffic_limit_gb=100, custom_traffic_enabled=True),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == 100


# ── прежнее поведение, которое не должно сломаться ──


@pytest.mark.postgres
async def test_classic_expired_renewal_still_takes_the_fixed_limit(postgres_database, monkeypatch):
    """Без тарифа базовый лимит по-прежнему из настройки фиксированного режима."""
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=100)
    monkeypatch.setattr(settings, 'SALES_MODE', 'classic')

    subscription = await _renew(
        monkeypatch, _subscription(tariff_id=None, traffic_limit_gb=50), postgres_database=postgres_database
    )

    assert subscription.traffic_limit_gb == 100


@pytest.mark.postgres
async def test_active_tariff_renewal_keeps_tariff_base_plus_active_topup(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)
    now = datetime.now(UTC)

    subscription = await _renew(
        monkeypatch,
        _subscription(
            status=SubscriptionStatus.ACTIVE.value,
            days_from_now=10,
            traffic_limit_gb=TARIFF_LIMIT_GB + TOPUP_GB,
            purchased_traffic_gb=TOPUP_GB,
        ),
        TrafficPurchase(subscription_id=10, traffic_gb=TOPUP_GB, expires_at=now + timedelta(days=20)),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB + TOPUP_GB
    assert subscription.purchased_traffic_gb == TOPUP_GB


# ── самовосстановление: испорченная подписка выправляется на ЛЮБОМ продлении ──


@pytest.mark.postgres
@pytest.mark.parametrize('reset_on_payment', [True, False])
async def test_active_tariff_renewal_heals_zeroed_limit(postgres_database, monkeypatch, reset_on_payment):
    """Продление ещё живой подписки тоже возвращает лимит тарифа — иначе тот, кто
    продлевает заранее, оставался бы с безлимитом навсегда."""
    _configure(monkeypatch, reset_on_payment=reset_on_payment, traffic_mode='fixed', fixed_gb=0)

    subscription = await _renew(
        monkeypatch,
        _subscription(status=SubscriptionStatus.ACTIVE.value, days_from_now=10, traffic_limit_gb=0),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB


@pytest.mark.postgres
async def test_expired_renewal_with_reset_off_heals_zeroed_limit(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=False, traffic_mode='fixed', fixed_gb=0)

    subscription = await _renew(monkeypatch, _subscription(traffic_limit_gb=0), postgres_database=postgres_database)

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB


@pytest.mark.postgres
async def test_active_renewal_heals_zeroed_limit_but_keeps_active_topup(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=True, traffic_mode='fixed', fixed_gb=0)
    now = datetime.now(UTC)

    subscription = await _renew(
        monkeypatch,
        _subscription(
            status=SubscriptionStatus.ACTIVE.value, days_from_now=10, traffic_limit_gb=0, purchased_traffic_gb=TOPUP_GB
        ),
        TrafficPurchase(subscription_id=10, traffic_gb=TOPUP_GB, expires_at=now + timedelta(days=20)),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB + TOPUP_GB


@pytest.mark.postgres
async def test_active_renewal_of_unlimited_tariff_stays_unlimited(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=False, traffic_mode='fixed', fixed_gb=100)

    subscription = await _renew(
        monkeypatch,
        _subscription(status=SubscriptionStatus.ACTIVE.value, days_from_now=10, traffic_limit_gb=0),
        tariff=_tariff(traffic_limit_gb=0),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == 0


@pytest.mark.postgres
async def test_classic_active_renewal_keeps_its_limit(postgres_database, monkeypatch):
    """Классическая подписка тарифа не имеет — её лимит продление не пересобирает."""
    _configure(monkeypatch, reset_on_payment=False, traffic_mode='selectable', fixed_gb=100)
    monkeypatch.setattr(settings, 'SALES_MODE', 'classic')

    subscription = await _renew(
        monkeypatch,
        _subscription(status=SubscriptionStatus.ACTIVE.value, days_from_now=10, tariff_id=None, traffic_limit_gb=80),
        postgres_database=postgres_database,
    )

    assert subscription.traffic_limit_gb == 80


@pytest.mark.postgres
async def test_reset_off_expired_renewal_keeps_the_limit(postgres_database, monkeypatch):
    _configure(monkeypatch, reset_on_payment=False, traffic_mode='fixed', fixed_gb=0)

    subscription = await _renew(monkeypatch, _subscription(), postgres_database=postgres_database)

    assert subscription.traffic_limit_gb == TARIFF_LIMIT_GB
    assert subscription.traffic_used_gb == 12.0
