"""Independent access pools must never share physical accounting nodes."""

import pytest

from app.services.bs_traffic.topology import TopologyConflict, resolve_topology


def test_distinct_squads_on_same_node_are_rejected():
    with pytest.raises(TopologyConflict) as caught:
        resolve_topology(('ordinary',), ('bs',), {'ordinary': ('shared',), 'bs': ('shared',)})
    assert caught.value.code == 'overlapping_nodes'
    assert caught.value.identifiers == ('shared',)


def test_multiple_bs_squads_share_one_counter_without_double_counting():
    topology = resolve_topology(
        ('ordinary',),
        ('bs-a', 'bs-b'),
        {'ordinary': ('normal-node',), 'bs-a': ('bs-node',), 'bs-b': ('bs-node', 'bs-node-2')},
    )
    assert topology.bs_nodes == ('bs-node', 'bs-node-2')
    assert topology.ordinary_nodes == ('normal-node',)


def test_unknown_squad_is_not_treated_as_empty_access():
    with pytest.raises(TopologyConflict, match='missing_squads'):
        resolve_topology(('ordinary',), ('missing',), {'ordinary': ('n',)})


def test_empty_squad_cannot_be_activated_as_valid_paid_access():
    with pytest.raises(TopologyConflict, match='empty_squads'):
        resolve_topology(('ordinary',), ('bs',), {'ordinary': ('n',), 'bs': ()})


@pytest.mark.parametrize(('ordinary', 'bs'), [((), ('bs',)), (('ordinary',), ())])
def test_split_mode_requires_explicit_groups_not_legacy_empty_means_all(ordinary, bs):
    with pytest.raises(TopologyConflict, match='empty_group'):
        resolve_topology(ordinary, bs, {'ordinary': ('a',), 'bs': ('b',)})


def test_same_squad_in_both_groups_is_rejected_even_before_node_lookup():
    with pytest.raises(TopologyConflict, match='overlapping_squads'):
        resolve_topology(('same',), ('same',), {})


def test_order_duplicates_and_unselected_squads_do_not_change_snapshot():
    first = resolve_topology(('a', 'a'), ('b', 'c'), {'a': ('1',), 'b': ('3', '2'), 'c': ('2',)})
    second = resolve_topology(('a',), ('c', 'b'), {'a': ('1',), 'c': ('2',), 'b': ('2', '3'), 'unused': ()})
    assert first == second


def test_snapshot_does_not_retain_mutable_input():
    nodes = ['2']
    snapshot = resolve_topology(['a'], ['b'], {'a': ['1'], 'b': nodes})
    nodes.append('1')
    assert snapshot.bs_nodes == ('2',)


@pytest.mark.parametrize('bad_nodes', [None, 'node', ('',), (False,), ('  ',)])
def test_malformed_node_evidence_fails_closed(bad_nodes):
    with pytest.raises(TopologyConflict, match='invalid_nodes'):
        resolve_topology(('a',), ('b',), {'a': ('1',), 'b': bad_nodes})


@pytest.mark.parametrize('bad_group', ['squad', (None,), ('',), (' squad ',)])
def test_invalid_selection_is_not_silently_normalized(bad_group):
    with pytest.raises(TopologyConflict, match='invalid_squads'):
        resolve_topology(('a',), bad_group, {'a': ('1',)})
