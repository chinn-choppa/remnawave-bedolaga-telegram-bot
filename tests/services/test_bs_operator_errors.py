"""Operator failures expose stable safe codes, never external exception text."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.bs_traffic import operator_binding
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.topology import TopologyConflict


@pytest.mark.parametrize(
    'failure,code',
    [
        (TopologyConflict('overlapping_nodes', ('private-identifier',)), 'bs_topology_invalid'),
        (TopologyConflict('accounting_configuration'), 'bs_usage_config_invalid'),
        (TimeoutError('private-endpoint'), 'bs_panel_unavailable'),
        (RuntimeError('private-token'), 'bs_panel_unavailable'),
        (LedgerConflict('private-binding'), 'bs_binding_mismatch'),
    ],
)
async def test_operator_preflight_sanitizes_codes(monkeypatch, failure, code):
    from app.config import settings
    from app.services.remnawave_service import RemnaWaveService

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)

    @asynccontextmanager
    async def client(self):
        yield SimpleNamespace(base_url='https://panel.example')

    monkeypatch.setattr(RemnaWaveService, 'get_api_client', client)
    monkeypatch.setattr(
        operator_binding,
        'collect_topology_evidence',
        AsyncMock(side_effect=None if isinstance(failure, LedgerConflict) else failure),
    )
    monkeypatch.setattr(
        operator_binding,
        'ensure_operator_binding',
        AsyncMock(side_effect=failure if isinstance(failure, LedgerConflict) else None),
    )
    with pytest.raises(operator_binding.OperatorSettingsError) as caught:
        await operator_binding.prepare_operator_settings(
            object(), settings={'enabled': True, 'squad_uuids': []}, ordinary_squads=[]
        )
    assert caught.value.code == code
    assert str(caught.value) == code
    assert 'private' not in str(caught.value)


async def test_uncertain_policy_requires_explicit_operator_setting_before_panel_io(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
    collector = AsyncMock()
    monkeypatch.setattr(operator_binding, 'collect_topology_evidence', collector)
    with pytest.raises(operator_binding.OperatorSettingsError, match='bs_uncertain_policy_required'):
        await operator_binding.prepare_operator_settings(
            object(), settings={'enabled': True, 'squad_uuids': []}, ordinary_squads=[]
        )
    collector.assert_not_awaited()
    assert settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is False
    await operator_binding.prepare_operator_settings(object(), settings={'enabled': False}, ordinary_squads=[])
