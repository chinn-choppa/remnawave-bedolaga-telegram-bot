from dataclasses import replace

import pytest

from app.services.bs_traffic.policy import AccessProjection, GroupProjection
from tests.services.test_bs_traffic_delivery_plan import TOPOLOGY, A, B, C


FINGERPRINT = 'b' * 64


def group(mode, squads):
    if mode == 'unlimited':
        return GroupProjection(mode, None, 0, False, 'active', squads)
    if mode == 'active':
        return GroupProjection('finite', 100, 0, False, 'active', squads)
    if mode == 'pending':
        return GroupProjection('finite', 100, 50, False, 'pending', None)
    if mode == 'disabled':
        return GroupProjection(mode, None, 0, False, mode, ())
    return GroupProjection('finite', 0, 0, False, 'exhausted', ())


def projection(ordinary='unlimited', bs='exhausted'):
    return AccessProjection(1, 'a' * 64, FINGERPRINT, group(ordinary, (A,)), group(bs, (B,)))


def plan(value, **changes):
    from app.services.bs_traffic.grace_restore import plan_grace_restore

    args = dict(
        topology=TOPOLOGY,
        current_squads=(C,),
        overlay_squads=(C,),
        paid_before_squads=(A, B),
        paid_before_fingerprint=FINGERPRINT,
    )
    args.update(changes)
    return plan_grace_restore(value, **args)


@pytest.mark.parametrize('ordinary', ['active', 'unlimited', 'exhausted', 'disabled'])
@pytest.mark.parametrize('bs', ['active', 'unlimited', 'exhausted', 'disabled'])
def test_restore_only_current_entitled_groups(ordinary, bs):
    expected = tuple(
        ([A] if ordinary in ('active', 'unlimited') else []) + ([B] if bs in ('active', 'unlimited') else [])
    )
    result = plan(projection(ordinary, bs))
    assert result.action == 'patch'
    assert result.squads == expected
    assert result.removed_squads == (C,)


def test_pending_group_preserves_pre_grace_subset_not_overlay_or_all_configured():
    result = plan(projection(bs='pending'), paid_before_squads=(A,))
    assert result.action == 'patch' and result.squads == (A,)
    result = plan(projection(bs='pending'))
    assert result.squads == (A, B)


def test_pending_group_with_changed_topology_cannot_reuse_old_rights():
    assert plan(projection(bs='pending'), paid_before_fingerprint='c' * 64).action == 'wait'


def test_confirmed_groups_use_current_policy_after_tariff_change():
    assert plan(projection(), paid_before_fingerprint='c' * 64, paid_before_squads=()).squads == (A,)


def test_already_restored_selection_is_idempotent():
    result = plan(projection(), current_squads=(A,))
    assert result.action == 'unchanged' and result.removed_squads == ()


@pytest.mark.parametrize('current', [(), (B,), (A, C), (A, B)])
def test_manual_or_partial_selection_is_not_overwritten(current):
    result = plan(projection(), current_squads=current)
    assert result.action == 'wait' and result.squads is None


@pytest.mark.parametrize(
    'state', ['grace', 'expired', 'blocked', 'tariff_pending', 'subscription_pending', 'topology_pending']
)
def test_account_wide_transition_cannot_be_replaced_by_squad_only_restore(state):
    value = projection()
    value = replace(value, ordinary=replace(value.ordinary, state=state, squads=None))
    assert plan(value).action == 'wait'


def test_preserve_grace_projection_cannot_authorize_exit():
    assert plan(replace(projection(), preserve_grace=True)).action == 'wait'


@pytest.mark.parametrize('field', ['current_squads', 'overlay_squads', 'paid_before_squads'])
def test_invalid_source_is_not_interpreted_as_empty(field):
    with pytest.raises(ValueError):
        plan(projection(), **{field: ('invalid',)})


@pytest.mark.parametrize(
    'changes',
    [
        {'mode': 'finite', 'state': 'exhausted', 'remaining_bytes': 100},
        {'mode': 'unlimited', 'state': 'disabled', 'remaining_bytes': None},
        {'mode': 'disabled', 'state': 'active', 'remaining_bytes': None},
        {'mode': 'finite', 'state': 'pending', 'remaining_bytes': -1, 'squads': None},
    ],
)
def test_inconsistent_group_cannot_remove_or_grant_paid_access(changes):
    value = projection()
    with pytest.raises(ValueError):
        plan(replace(value, bs=replace(value.bs, **changes)))


def test_pending_snapshot_with_unknown_squad_cannot_claim_matching_topology():
    assert plan(projection(bs='pending'), paid_before_squads=(A, C)).action == 'wait'


def test_empty_overlay_is_not_a_confirmed_grace_source():
    with pytest.raises(ValueError):
        plan(projection(), overlay_squads=())
