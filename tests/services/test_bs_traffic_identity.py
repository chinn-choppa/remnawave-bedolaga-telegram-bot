"""Rotating credentials must not create a new traffic accounting generation."""

from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.bs_traffic.identity import account_generation


SCOPE = '00000000-0000-4000-8000-000000000001'
CREATED = datetime(2026, 1, 1, tzinfo=UTC)


def account(**changes):
    return SimpleNamespace(**({'id': 12, 'created_at': CREATED, 'short_uuid': 'old-link'} | changes))


def test_link_and_password_rotation_preserve_account_generation():
    before = account_generation(panel_scope=SCOPE, account=account())
    after = account_generation(
        panel_scope=SCOPE, account=account(short_uuid='new-link', vless_uuid='new-password', updated_at=CREATED)
    )
    assert before == after
    assert len(before) == 64 and 'old-link' not in before and 'new-link' not in after


@pytest.mark.parametrize('changes', [{'id': 13}, {'created_at': CREATED + timedelta(microseconds=1)}])
def test_recreated_account_does_not_inherit_previous_generation(changes):
    assert account_generation(panel_scope=SCOPE, account=account()) != account_generation(
        panel_scope=SCOPE, account=account(**changes)
    )


def test_new_panel_epoch_does_not_inherit_previous_account_generation():
    assert account_generation(panel_scope=SCOPE, account=account()) != account_generation(
        panel_scope='00000000-0000-4000-8000-000000000002', account=account()
    )


def test_same_creation_in_another_timezone_has_same_identity():
    assert account_generation(panel_scope=SCOPE, account=account()) == account_generation(
        panel_scope=SCOPE, account=account(created_at=CREATED.astimezone(timezone(timedelta(hours=3))))
    )


@pytest.mark.parametrize('value', [None, '', '2026-01-01', datetime(2026, 1, 1)])
def test_unknown_or_naive_creation_time_is_not_invented(value):
    with pytest.raises(ValueError):
        account_generation(panel_scope=SCOPE, account=account(created_at=value))


@pytest.mark.parametrize('value', [True, None, 0, -1, '12', 2**53])
def test_invalid_numeric_identity_is_rejected(value):
    with pytest.raises(ValueError):
        account_generation(panel_scope=SCOPE, account=account(id=value))


@pytest.mark.parametrize('scope', [None, '', 'https://panel.example', 'not-a-uuid'])
def test_panel_scope_must_be_explicit_epoch_uuid(scope):
    with pytest.raises(ValueError):
        account_generation(panel_scope=scope, account=account())
