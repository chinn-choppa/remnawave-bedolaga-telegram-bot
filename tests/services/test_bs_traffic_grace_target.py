from dataclasses import replace
from datetime import timedelta

import pytest

from app.services.grace_access_service import GraceBillingState
from tests.services.test_bs_traffic_delivery_plan import TOPOLOGY, A, B, C
from tests.services.test_bs_traffic_grace_confirmation import NOW, state
from tests.services.test_bs_traffic_grace_restore import FINGERPRINT, projection


def billing():
    return GraceBillingState(1, 12, 'active', NOW + timedelta(days=60), 999, 999, 5, (A, B))


def target(**changes):
    from app.services.bs_traffic.grace_restore import build_paid_restore_target

    args = dict(
        billing=billing(),
        projection=projection(),
        topology=TOPOLOGY,
        current=replace(state(), active_internal_squads=[{'uuid': C}]),
        overlay_squads=(C,),
        paid_before_squads=(A, B),
        paid_before_fingerprint=FINGERPRINT,
        now=NOW,
    )
    args.update(changes)
    return build_paid_restore_target(**args)


def test_target_uses_latest_term_devices_and_independent_paid_groups():
    result = target()
    assert result.expire_at == billing().end_at
    assert result.hwid_device_limit == 5
    assert result.active_internal_squads == [{'uuid': A}]
    assert result.status == 'ACTIVE'
    assert result.traffic_limit_bytes == 0 and result.traffic_limit_strategy == 'NO_RESET'
    assert result.used_traffic_bytes == 100
    assert result.last_traffic_reset_at is None


@pytest.mark.parametrize(
    'change',
    [
        {'status': 'disabled'},
        {'status': 'expired'},
        {'end_at': NOW},
        {'end_at': None},
        {'user_status': 'blocked'},
        {'remnawave_id': 99},
    ],
)
def test_ineligible_or_other_account_cannot_build_paid_target(change):
    with pytest.raises(ValueError):
        target(billing=replace(billing(), **change))


def test_pending_topology_does_not_build_target():
    value = projection()
    value = replace(value, bs=replace(value.bs, state='topology_pending', squads=None))
    assert target(projection=value) is None


def test_manual_selection_is_not_overwritten():
    assert target(current=replace(state(), active_internal_squads=[])) is None


def test_exhausted_groups_produce_empty_squads_without_resetting_usage():
    result = target(projection=projection('exhausted', 'exhausted'))
    assert result.active_internal_squads == []
    assert result.used_traffic_bytes == 100


@pytest.mark.parametrize('devices', [None, 0, 3])
def test_latest_device_setting_is_preserved(devices):
    assert target(billing=replace(billing(), device_limit=devices)).hwid_device_limit == devices


def test_negative_paid_limit_is_rejected_even_if_panel_can_return_it():
    with pytest.raises(ValueError):
        target(billing=replace(billing(), device_limit=-1))


def test_latest_external_squad_and_current_reset_evidence_are_preserved():
    result = target(
        billing=replace(billing(), external_squad_uuid=B),
        current=replace(
            state(), active_internal_squads=[{'uuid': C}], last_traffic_reset_at=NOW, used_traffic_bytes=250
        ),
    )
    assert result.external_squad_uuid == B
    assert result.last_traffic_reset_at == NOW and result.used_traffic_bytes == 250


@pytest.mark.parametrize(
    'field,value',
    [
        ('device_limit', True),
        ('device_limit', '5'),
        ('external_squad_uuid', ''),
        ('external_squad_uuid', 'invalid'),
        ('remnawave_id', True),
    ],
)
def test_invalid_paid_fields_are_not_coerced(field, value):
    with pytest.raises(ValueError):
        target(billing=replace(billing(), **{field: value}))


def test_unchanged_squads_do_not_skip_changed_term_and_devices():
    result = target(current=replace(state(), active_internal_squads=[{'uuid': A}]))
    assert result.expire_at == billing().end_at and result.hwid_device_limit == 5


def test_result_can_be_confirmed_only_with_all_new_fields():
    from app.services.bs_traffic.grace_restore import full_restore_confirmed

    result = target()
    assert full_restore_confirmed(replace(result, used_traffic_bytes=result.used_traffic_bytes + 1), result)
    assert not full_restore_confirmed(replace(result, hwid_device_limit=2), result)
