from unittest.mock import AsyncMock

import pytest

from app.services.panel_sync import push_subscription
from app.services.panel_sync.payload import build_panel_payload
from app.services.panel_sync.projection import PanelSnapshot, project_onto_subscription
from app.services.traffic_accounting import AccountingConflict, require_supported_accounting_mode
from tests.services.panel_sync.test_projection import _sub as projection_sub
from tests.services.panel_sync.test_writer import NOW, _api, _sub, _user


@pytest.mark.parametrize('mode', ['split', 'split_pending', 'unknown', '', True])
def test_unsupported_owner_cannot_build_legacy_payload(mode):
    with pytest.raises(AccountingConflict):
        build_panel_payload(_user(), _sub(traffic_accounting_mode=mode), multi_tariff=True, now=NOW)


@pytest.mark.parametrize('mode', ['split', 'split_pending', 'unknown', ''])
async def test_unsupported_owner_cannot_call_panel(mode):
    api = _api()
    with pytest.raises(AccountingConflict):
        await push_subscription(api, _user(), _sub(traffic_accounting_mode=mode), multi_tariff=True, now=NOW)
    assert api.mock_calls == []


async def test_persisted_owner_wins_over_stale_legacy_object():
    api = _api()
    db = AsyncMock()
    db.scalar.return_value = 'split'
    with pytest.raises(AccountingConflict):
        await push_subscription(api, _user(), _sub(traffic_accounting_mode='legacy'), db=db, multi_tariff=True, now=NOW)
    assert api.mock_calls == []


@pytest.mark.parametrize('mode', ['split', 'split_pending', 'unknown', ''])
def test_old_projection_does_not_import_technical_quota_or_status(mode):
    sub = projection_sub(traffic_accounting_mode=mode)
    before = vars(sub).copy()
    assert project_onto_subscription(sub, PanelSnapshot(status='DISABLED', traffic_limit_gb=0), now=NOW) == set()
    assert vars(sub) == before


@pytest.mark.parametrize('mode', ['legacy', 'managed', 'migrating', None])
def test_existing_accounting_owners_remain_supported(mode):
    assert require_supported_accounting_mode(mode) == ('legacy' if mode is None else mode)
