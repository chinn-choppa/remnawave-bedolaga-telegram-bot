from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from app.external.remnawave_api import SplitGraceSnapshot


NOW = datetime(2026, 11, 5, tzinfo=UTC)


def state():
    return SplitGraceSnapshot(12, NOW, [], 'ACTIVE', NOW + timedelta(days=30), 0, 'NO_RESET', 100, 2, None, None)


def confirm(current, target=None):
    from app.services.bs_traffic.grace_restore import full_restore_confirmed

    return full_restore_confirmed(current, target or state())


@pytest.mark.parametrize(
    'changes',
    [
        {'id': 13},
        {'created_at': NOW + timedelta(seconds=1)},
        {'status': 'DISABLED'},
        {'expire_at': NOW + timedelta(days=31)},
        {'traffic_limit_bytes': 1},
        {'traffic_limit_strategy': 'MONTH'},
        {'hwid_device_limit': 3},
        {'external_squad_uuid': '99999999-9999-4999-8999-999999999999'},
        {'active_internal_squads': [{'uuid': '99999999-9999-4999-8999-999999999999'}]},
        {'used_traffic_bytes': 99},
        {'last_traffic_reset_at': NOW},
    ],
)
def test_partial_or_divergent_restore_is_not_confirmation(changes):
    assert not confirm(replace(state(), **changes))


def test_usage_growth_is_not_a_restore_conflict():
    assert confirm(state())
    assert confirm(replace(state(), used_traffic_bytes=101))


def test_null_and_unlimited_devices_are_not_interchangeable():
    assert not confirm(replace(state(), hwid_device_limit=0), replace(state(), hwid_device_limit=None))


def test_squad_order_does_not_change_result():
    rows = [{'uuid': f'{n}9999999-9999-4999-8999-999999999999'} for n in (1, 2)]
    assert confirm(replace(state(), active_internal_squads=rows[::-1]), replace(state(), active_internal_squads=rows))


@pytest.mark.parametrize('value', [None, {}, object()])
def test_missing_full_evidence_cannot_confirm(value):
    assert not confirm(value)


@pytest.mark.parametrize('status', ['ACTIVE', 'EXPIRED', 'DISABLED', 'LIMITED'])
def test_full_confirmation_never_ignores_expiry_for_restricted_status(status):
    target = replace(state(), status=status)
    assert confirm(target, target)
    assert not confirm(replace(target, expire_at=target.expire_at + timedelta(seconds=1)), target)


def test_duplicate_squads_are_not_successful_readback():
    row = {'uuid': '99999999-9999-4999-8999-999999999999'}
    assert not confirm(
        replace(state(), active_internal_squads=[row, row]), replace(state(), active_internal_squads=[row])
    )


def test_same_expiry_in_another_timezone_is_the_same_instant():
    from datetime import timezone

    assert confirm(replace(state(), expire_at=state().expire_at.astimezone(timezone(timedelta(hours=3)))))


@pytest.mark.parametrize('status', ['ACTIVE', 'EXPIRED', 'DISABLED', 'LIMITED'])
def test_panel_millisecond_serialization_confirms_expiry_without_time_tolerance(status):
    target = replace(state(), status=status, expire_at=state().expire_at.replace(microsecond=330834))
    assert confirm(replace(target, expire_at=target.expire_at.replace(microsecond=330000)), target)
    assert confirm(target, target)
    for microsecond in (329000, 331000, 330001, 330833):
        assert not confirm(replace(target, expire_at=target.expire_at.replace(microsecond=microsecond)), target)


def test_expiry_precision_does_not_relax_identity_or_reset_evidence():
    target = replace(state(), expire_at=state().expire_at.replace(microsecond=330834))
    current = replace(target, expire_at=target.expire_at.replace(microsecond=330000))
    assert not confirm(replace(current, created_at=current.created_at + timedelta(microseconds=1)), target)
    assert not confirm(replace(current, last_traffic_reset_at=NOW), target)
