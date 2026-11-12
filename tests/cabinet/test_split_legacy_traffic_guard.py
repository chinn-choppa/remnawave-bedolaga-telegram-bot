from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.cabinet.routes.subscription_modules import traffic
from app.cabinet.schemas.subscription import TrafficPurchaseRequest


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
@pytest.mark.parametrize(
    'handler',
    ['get_traffic_packages', 'purchase_traffic', 'save_traffic_cart', 'switch_traffic_package', 'refresh_traffic'],
)
async def test_old_traffic_endpoints_reject_before_work(monkeypatch, mode, handler):
    sub = SimpleNamespace(id=42, traffic_accounting_mode=mode)
    monkeypatch.setattr(traffic, 'resolve_subscription', AsyncMock(return_value=sub))
    db = SimpleNamespace()
    kwargs = dict(user=SimpleNamespace(id=7), db=db, subscription_id=42)
    if handler in ('purchase_traffic', 'save_traffic_cart', 'switch_traffic_package'):
        kwargs['request'] = TrafficPurchaseRequest(gb=10)
    with pytest.raises(HTTPException) as error:
        await getattr(traffic, handler)(**kwargs)
    assert error.value.status_code == 409


@pytest.mark.parametrize('mode', ['split', 'split_pending'])
async def test_old_telegram_resolver_stops_selected_split(monkeypatch, mode):
    from app.handlers.subscription import common, traffic as bot_traffic

    sub = SimpleNamespace(id=42, traffic_accounting_mode=mode)
    monkeypatch.setattr(common, 'resolve_subscription_from_context', AsyncMock(return_value=(sub, 42)))
    callback = SimpleNamespace(answer=AsyncMock())
    result = await bot_traffic._resolve_subscription(callback, SimpleNamespace(id=7, language='ru'), object())
    assert result == (None, 42)
    callback.answer.assert_awaited_once()
    assert callback.answer.await_args.kwargs['show_alert'] is True


@pytest.mark.usefixtures('no_pending_bs_enrollment')
async def test_refresh_preserves_conflict_after_rollback_and_mode_change(monkeypatch):
    monkeypatch.setattr(
        traffic,
        'resolve_subscription',
        AsyncMock(
            side_effect=[
                SimpleNamespace(id=42, traffic_accounting_mode='managed'),
                SimpleNamespace(id=42, traffic_accounting_mode='split'),
            ]
        ),
    )
    monkeypatch.setattr(traffic.RateLimitCache, 'is_rate_limited', AsyncMock(return_value=False))
    monkeypatch.setattr(
        traffic,
        'SubscriptionService',
        lambda: SimpleNamespace(
            _persisted_traffic_mode=AsyncMock(return_value='managed'),
            sync_subscription_usage=AsyncMock(return_value=False),
        ),
    )
    with pytest.raises(HTTPException) as error:
        await traffic.refresh_traffic(
            user=SimpleNamespace(id=7), db=SimpleNamespace(rollback=AsyncMock()), subscription_id=42
        )
    assert error.value.status_code == 409
