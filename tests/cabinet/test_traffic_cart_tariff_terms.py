"""Saving a tariff traffic cart must quote the same 30-day package as purchase."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.cabinet.routes.subscription_modules import traffic
from app.cabinet.schemas.subscription import TrafficPurchaseRequest
from app.database.models import Subscription
from tests.cabinet.test_traffic_packages_discount import _make_user
from tests.services.test_traffic_package_selection import tariff


@pytest.mark.asyncio
@pytest.mark.parametrize('days', [1, 7, 30, 365, 10000])
async def test_saved_tariff_package_is_not_prorated(monkeypatch, days):
    sub = Subscription(
        id=1,
        user_id=1,
        tariff_id=1,
        status='active',
        is_trial=False,
        traffic_limit_gb=100,
        end_date=datetime.now(UTC) + timedelta(days=days),
    )
    monkeypatch.setattr(type(traffic.settings), 'is_tariffs_mode', lambda self: True)
    monkeypatch.setattr(traffic, 'resolve_subscription', AsyncMock(return_value=sub))
    monkeypatch.setattr(traffic, 'get_tariff_by_id', AsyncMock(return_value=tariff()))
    saved = AsyncMock()
    monkeypatch.setattr(traffic.user_cart_service, 'save_user_cart', saved)
    await traffic.save_traffic_cart(TrafficPurchaseRequest(gb=10), _make_user(traffic_discount_percent=20), object(), 1)
    cart = saved.call_args.args[1]
    assert cart['price_kopeks'] == 4000
    assert cart['base_price_kopeks'] == 5000
    assert cart['traffic_group'] == 'ordinary'


@pytest.mark.asyncio
@pytest.mark.parametrize('limit', [141, 150])
async def test_cart_does_not_offer_package_over_tariff_cap(monkeypatch, limit):
    sub = Subscription(
        id=1,
        user_id=1,
        tariff_id=1,
        status='active',
        is_trial=False,
        traffic_limit_gb=limit,
        end_date=datetime.now(UTC) + timedelta(days=30),
    )
    monkeypatch.setattr(type(traffic.settings), 'is_tariffs_mode', lambda self: True)
    monkeypatch.setattr(traffic, 'resolve_subscription', AsyncMock(return_value=sub))
    monkeypatch.setattr(traffic, 'get_tariff_by_id', AsyncMock(return_value=tariff()))
    saved = AsyncMock()
    monkeypatch.setattr(traffic.user_cart_service, 'save_user_cart', saved)
    with pytest.raises(HTTPException) as raised:
        await traffic.save_traffic_cart(
            TrafficPurchaseRequest(gb=10), _make_user(traffic_discount_percent=0), object(), 1
        )
    assert raised.value.status_code == 400
    saved.assert_not_called()


@pytest.mark.parametrize('group', ['bs', 'other', '', None])
def test_unimplemented_group_is_not_silently_ignored_by_http_schema(group):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        TrafficPurchaseRequest.model_validate({'gb': 10, 'traffic_group': group})


# These tests model legacy/managed accounts without an enrollment receipt.
pytestmark = pytest.mark.usefixtures('no_pending_bs_enrollment')
