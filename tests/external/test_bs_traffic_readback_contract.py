from unittest.mock import AsyncMock

import pytest

from app.external.remnawave_api import RemnaWaveAPIError
from tests.external.test_remnawave_3_4_3_contract import _api


@pytest.mark.parametrize(
    'change', [{}, {'id': 12.5}, {'id': True}, {'createdAt': '2026-01-01'}, {'activeInternalSquads': None}]
)
async def test_incomplete_or_coerced_fields_cannot_confirm_empty_selection(change):
    payload = {'id': 12, 'createdAt': '2026-01-01T00:00:00Z', 'activeInternalSquads': []}
    if not change:
        payload.pop('activeInternalSquads')
    payload.update(change)
    api = _api()
    api._make_request = AsyncMock(return_value={'response': payload})
    with pytest.raises(RemnaWaveAPIError):
        await api.get_split_access_snapshot(12)


async def test_explicit_empty_is_preserved_without_enrichment():
    api = _api()
    api._make_request = AsyncMock(
        return_value={'response': {'id': 12, 'createdAt': '2026-01-01T00:00:00Z', 'activeInternalSquads': []}}
    )
    result = await api.get_split_access_snapshot(12)
    assert result.id == 12 and result.active_internal_squads == []
    api.enrich_user_with_happ_link.assert_not_awaited()
