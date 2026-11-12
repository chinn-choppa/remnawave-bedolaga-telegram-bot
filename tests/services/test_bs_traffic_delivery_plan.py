"""Conservative split access plans must not invent access or clear other groups."""

from dataclasses import replace

import pytest

from app.services.bs_traffic.delivery_plan import plan_squads
from app.services.bs_traffic.policy import AccessProjection, GroupProjection
from app.services.bs_traffic.topology import SplitTopology


A = '11111111-1111-4111-8111-111111111111'
B = '22222222-2222-4222-8222-222222222222'
C = '33333333-3333-4333-8333-333333333333'
N1 = '44444444-4444-4444-8444-444444444444'
N2 = '55555555-5555-4555-8555-555555555555'
TOPOLOGY = SplitTopology((A,), (B,), (N1,), (N2,))


def projection(ordinary=(A,), bs=()):
    return AccessProjection(
        3,
        'a' * 64,
        'b' * 64,
        GroupProjection('unlimited', None, 0, False, 'active', ordinary),
        GroupProjection('finite', 0, 0, False, 'exhausted', bs),
    )


@pytest.mark.parametrize('current', [(), (A,), (B,), (A, B)])
@pytest.mark.parametrize('ordinary', [None, (), (A,)])
@pytest.mark.parametrize('bs', [None, (), (B,)])
def test_each_group_is_projected_independently(current, ordinary, bs):
    value = projection(ordinary, bs)
    result = plan_squads(value, topology=TOPOLOGY, current_squads=current)
    expected = tuple(
        sorted(
            set(
                (tuple(s for s in current if s == A) if ordinary is None else ordinary)
                + (tuple(s for s in current if s == B) if bs is None else bs)
            )
        )
    )
    assert result.squads == expected
    assert result.action == ('unchanged' if expected == current else 'patch')
    assert result.removed_squads == tuple(s for s in current if s not in expected)


def test_unknown_squad_requires_reconciliation_instead_of_deleting_or_retaining_blindly():
    result = plan_squads(projection(), topology=TOPOLOGY, current_squads=(A, C))
    assert result.action == 'wait' and result.squads is None
    assert result.removed_squads == ()


def test_grace_overlay_never_gets_a_paid_squad_patch():
    value = replace(projection(), preserve_grace=True)
    result = plan_squads(value, topology=TOPOLOGY, current_squads=(C,))
    assert result.action == 'wait' and result.squads is None


@pytest.mark.parametrize('current', [None, 'bad', (A, A), ('bad',), (True,), (B, A)])
def test_malformed_panel_selection_cannot_be_interpreted_as_empty(current):
    with pytest.raises(ValueError):
        plan_squads(projection(), topology=TOPOLOGY, current_squads=current)


@pytest.mark.parametrize('ordinary,bs', [((B,), ()), ((C,), ()), ((A,), (A,))])
def test_projection_cannot_assign_squads_from_the_wrong_group(ordinary, bs):
    with pytest.raises(ValueError):
        plan_squads(projection(ordinary, bs), topology=TOPOLOGY, current_squads=(A, B))


def test_changed_topology_does_not_silently_keep_old_group():
    changed = replace(TOPOLOGY, ordinary_squads=(C,))
    with pytest.raises(ValueError):
        plan_squads(projection(), topology=changed, current_squads=(A, B))
