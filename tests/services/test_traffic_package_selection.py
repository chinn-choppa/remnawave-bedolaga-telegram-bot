"""Server-side package selection never mixes ordinary and BS tariff terms."""

from unittest.mock import AsyncMock

import pytest

from app.database.models import Tariff, User
from app.services.bs_traffic.tariff_settings import traffic_package_price


def tariff():
    return Tariff(
        traffic_limit_gb=100,
        traffic_topup_enabled=True,
        traffic_topup_packages={'10': 5000},
        max_topup_traffic_gb=150,
        bs_traffic_settings={
            'squad_uuids': ['00000000-0000-0000-0000-000000000002'],
            'limit_gb': 5,
            'topup_enabled': True,
            'topup_packages': {'10': 25000},
            'max_topup_gb': 20,
        },
    )


@pytest.mark.parametrize('group,current,price', [('ordinary', 100, 5000), ('bs', 5, 25000)])
def test_selected_group_owns_price_and_cap(group, current, price):
    assert traffic_package_price(tariff(), group=group, gb=10, current_limit_bytes=current * 1024**3) == price


@pytest.mark.parametrize('group', ['ordinary', 'bs'])
def test_cap_is_checked_without_rounding_bytes(group):
    cap = 150 if group == 'ordinary' else 20
    assert traffic_package_price(tariff(), group=group, gb=10, current_limit_bytes=(cap - 10) * 1024**3)
    with pytest.raises(ValueError, match='limit'):
        traffic_package_price(tariff(), group=group, gb=10, current_limit_bytes=(cap - 10) * 1024**3 + 1)


@pytest.mark.parametrize('group', ['ordinary', 'bs'])
@pytest.mark.parametrize('gb', [0, -1, 11, True, '10', 10.0])
def test_unconfigured_or_invalid_package_is_not_purchased(group, gb):
    with pytest.raises(ValueError):
        traffic_package_price(tariff(), group=group, gb=gb, current_limit_bytes=0)


@pytest.mark.parametrize('group', ['ordinary', 'bs'])
@pytest.mark.parametrize('change', ['disabled', 'unlimited', 'zero_price'])
def test_unavailable_group_cannot_fall_back_to_other_group(group, change):
    item = tariff()
    if group == 'bs':
        key, value = {
            'disabled': ('topup_enabled', False),
            'unlimited': ('limit_gb', 0),
            'zero_price': ('topup_packages', {'10': 0}),
        }[change]
        item.bs_traffic_settings[key] = value
    else:
        key, value = {
            'disabled': ('traffic_topup_enabled', False),
            'unlimited': ('traffic_limit_gb', 0),
            'zero_price': ('traffic_topup_packages', {'10': 0}),
        }[change]
        setattr(item, key, value)
    with pytest.raises(ValueError):
        traffic_package_price(item, group=group, gb=10, current_limit_bytes=0)


@pytest.mark.parametrize('group', [None, '', 'BS', 'other'])
def test_unknown_group_is_rejected(group):
    with pytest.raises(ValueError):
        traffic_package_price(tariff(), group=group, gb=10, current_limit_bytes=0)


@pytest.mark.asyncio
@pytest.mark.parametrize('group', ['bs', None, '', 'other'])
@pytest.mark.usefixtures('no_pending_bs_enrollment')
async def test_standard_cart_never_consumes_another_group(monkeypatch, group):
    from app.services.subscription_auto_purchase_service import _auto_add_traffic

    db = AsyncMock()
    lookup = AsyncMock(side_effect=AssertionError('Wrong group must not reach subscription lookup'))
    monkeypatch.setattr('app.database.crud.subscription.get_subscription_by_user_id', lookup)
    result = await _auto_add_traffic(
        db,
        User(id=1),
        {
            'traffic_gb': 10,
            'price_kopeks': 5000,
            'traffic_group': group,
        },
    )
    assert result is False
    lookup.assert_not_called()
    db.execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('saved_price', [4000, 6000])
@pytest.mark.usefixtures('no_pending_bs_enrollment')
async def test_changed_cart_price_requires_new_confirmation(monkeypatch, saved_price):
    from datetime import UTC, datetime, timedelta

    from app.config import settings
    from app.database.models import Subscription
    from app.services.subscription_auto_purchase_service import _auto_add_traffic
    from tests.cabinet.test_traffic_packages_discount import _make_user

    user = _make_user(traffic_discount_percent=0)
    sub = Subscription(
        id=1,
        user_id=user.id,
        tariff_id=1,
        traffic_limit_gb=100,
        status='active',
        is_trial=False,
        end_date=datetime.now(UTC) + timedelta(days=365),
    )
    monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: False)
    monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: True)
    monkeypatch.setattr('app.database.crud.subscription.get_subscription_by_user_id', AsyncMock(return_value=sub))
    monkeypatch.setattr('app.database.crud.tariff.get_tariff_by_id', AsyncMock(return_value=tariff()))
    monkeypatch.setattr('app.database.crud.user.lock_user_for_pricing', AsyncMock(return_value=user))
    monkeypatch.setattr(
        'app.services.traffic_purchase_service.prepare_traffic_purchase', AsyncMock(return_value=(sub, object()))
    )
    from unittest.mock import Mock

    from app.services.pricing_engine import PricingEngine

    discount = Mock(wraps=PricingEngine.calculate_traffic_discount)
    monkeypatch.setattr(PricingEngine, 'calculate_traffic_discount', discount)
    debit = AsyncMock(side_effect=AssertionError('Changed quote must not charge'))
    monkeypatch.setattr('app.database.crud.user.subtract_user_balance', debit)
    result = await _auto_add_traffic(
        AsyncMock(),
        user,
        {
            'traffic_gb': 10,
            'price_kopeks': saved_price,
            'traffic_group': 'ordinary',
        },
    )
    assert result is False
    debit.assert_not_called()
    assert discount.call_args.args[2] == 30


@pytest.mark.asyncio
@pytest.mark.parametrize('entrypoint', ['display', 'cart'])
@pytest.mark.parametrize('days', [1, 365, 10000])
@pytest.mark.usefixtures('no_pending_bs_enrollment')
async def test_telegram_tariff_package_discount_uses_30_days(monkeypatch, entrypoint, days):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace
    from unittest.mock import Mock

    from app.config import settings
    from app.database.models import Subscription
    from app.handlers.subscription import traffic
    from app.services.pricing_engine import PricingEngine
    from tests.cabinet.test_traffic_packages_discount import _make_user

    user = _make_user(traffic_discount_percent=20)
    user.language, user.balance_kopeks = 'ru', 0
    sub = Subscription(
        id=1,
        user_id=1,
        tariff_id=1,
        traffic_limit_gb=100,
        is_trial=False,
        end_date=datetime.now(UTC) + timedelta(days=days),
    )
    callback = SimpleNamespace(data='add_traffic_10', message=AsyncMock(), answer=AsyncMock())
    monkeypatch.setattr(type(settings), 'is_tariffs_mode', lambda self: True)
    monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: False)
    monkeypatch.setattr(traffic, '_resolve_subscription', AsyncMock(return_value=(sub, 1)))
    monkeypatch.setattr(
        'app.handlers.subscription.common.resolve_subscription_from_context', AsyncMock(return_value=(sub, 1))
    )
    monkeypatch.setattr('app.database.crud.tariff.get_tariff_by_id', AsyncMock(return_value=tariff()))
    monkeypatch.setattr('app.database.crud.user.lock_user_for_pricing', AsyncMock(return_value=user))
    monkeypatch.setattr(
        'app.services.traffic_purchase_service.prepare_traffic_purchase', AsyncMock(return_value=(sub, object()))
    )
    discount = Mock(return_value=20)
    monkeypatch.setattr(PricingEngine, 'get_addon_discount_percent', discount)
    saved = AsyncMock()
    monkeypatch.setattr(traffic.user_cart_service, 'save_user_cart', saved)
    if entrypoint == 'display':
        await traffic.handle_add_traffic(callback, user, AsyncMock())
    else:
        await traffic.add_traffic(callback, user, AsyncMock())
        assert saved.call_args.args[1]['price_kopeks'] == 4000
        assert saved.call_args.args[1]['traffic_group'] == 'ordinary'
    assert discount.call_args.args[2] == 30
    callback.message.edit_text.assert_awaited_once()
