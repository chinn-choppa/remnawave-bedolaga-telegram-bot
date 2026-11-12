from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.localization.texts import get_texts
from app.services.bs_traffic.customer_text import subscription_traffic_text
from app.services.bs_traffic.ledger import LedgerConflict


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'fa', 'zh'])
async def test_zero_not_unlimited_and_explicit_owner(monkeypatch, language):
    from app.services.bs_traffic import customer_text

    group = {
        'mode': 'finite',
        'remaining_bytes': '0',
        'pending_bytes': '0',
        'pending_operations': 0,
        'needs_review': False,
    }
    read = AsyncMock(return_value={'groups': {'ordinary': dict(group, mode='unlimited'), 'bs': group}})
    monkeypatch.setattr(customer_text, 'customer_summary', read)
    db = object()
    text = await subscription_traffic_text(
        db, user_id=7, subscription=SimpleNamespace(id=42, traffic_accounting_mode='split'), texts=get_texts(language)
    )
    assert '0' in text
    assert 'SPLIT_TRAFFIC_' not in text
    read.assert_awaited_once_with(db, user_id=7, subscription_id=42)


@pytest.mark.parametrize('mode', ['legacy', 'managed', 'split_pending'])
async def test_non_active_split_never_queries_summary(monkeypatch, mode):
    from app.services.bs_traffic import customer_text

    read = AsyncMock()
    monkeypatch.setattr(customer_text, 'customer_summary', read)
    result = await subscription_traffic_text(
        object(), user_id=7, subscription=SimpleNamespace(id=42, traffic_accounting_mode=mode), texts=get_texts('ru')
    )
    read.assert_not_awaited()
    assert (result is None) == (mode != 'split_pending')


async def test_conflict_is_sanitized(monkeypatch):
    from app.services.bs_traffic import customer_text

    monkeypatch.setattr(customer_text, 'customer_summary', AsyncMock(side_effect=LedgerConflict('private scope')))
    result = await subscription_traffic_text(
        object(), user_id=7, subscription=SimpleNamespace(id=42, traffic_accounting_mode='split'), texts=get_texts('ru')
    )
    assert 'private' not in result
    assert 'Не удалось' in result


@pytest.mark.parametrize('amount,expected', [('536870912', '512 МБ'), ('1', '&lt; 1 МБ'), ('0', '0 ГБ')])
async def test_small_positive_remainder_is_not_displayed_as_exhausted(monkeypatch, amount, expected):
    from app.services.bs_traffic import customer_text

    group = {
        'mode': 'finite',
        'remaining_bytes': amount,
        'pending_bytes': '0',
        'pending_operations': 0,
        'needs_review': False,
    }
    monkeypatch.setattr(
        customer_text, 'customer_summary', AsyncMock(return_value={'groups': {'ordinary': group, 'bs': group}})
    )
    result = await subscription_traffic_text(
        object(), user_id=7, subscription=SimpleNamespace(id=42, traffic_accounting_mode='split'), texts=get_texts('ru')
    )
    assert expected in result
