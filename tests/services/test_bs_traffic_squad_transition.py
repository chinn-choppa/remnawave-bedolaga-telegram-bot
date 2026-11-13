"""Pure source/target squad migration must never invent partial access."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module, util

import pytest

from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.policy import AccessProjection, GroupProjection
from app.services.bs_traffic.topology import SplitTopology


A, B, C, D, N1, N2, X = (f'{i:08x}-0000-4000-8000-000000000000' for i in range(1, 8))
OLD = SplitTopology((A,), (B,), (N1,), (N2,))
NEW = SplitTopology((C,), (D,), (N1,), (N2,))
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def values():
    def projection(topology, fingerprint):
        return AccessProjection(
            1,
            'a' * 64,
            fingerprint,
            GroupProjection('unlimited', None, 0, False, 'active', topology.ordinary_squads),
            GroupProjection('finite', 10, 0, False, 'active', topology.bs_squads),
        )

    return dict(
        source_projection=projection(OLD, 'b' * 64),
        target_projection=projection(NEW, 'c' * 64),
        source_evidence=TopologyEvidence(
            'scope', OLD, 'b' * 64, NOW - timedelta(seconds=1), NOW + timedelta(seconds=10)
        ),
        target_evidence=TopologyEvidence(
            'scope', NEW, 'c' * 64, NOW - timedelta(seconds=1), NOW + timedelta(seconds=10)
        ),
        expected_source_squads=(A, B),
        current_squads=(A, B),
        now=NOW,
        panel_identity='scope',
    )


def plan(**kwargs):
    assert util.find_spec('app.services.bs_traffic.squad_transition') is not None
    return import_module('app.services.bs_traffic.squad_transition').plan_topology_transition(**kwargs)


def test_exact_source_moves_and_exact_target_is_unchanged():
    args = values()
    result = plan(**args)
    assert (result.action, result.squads, result.removed_squads) == ('patch', (C, D), (A, B))
    args['current_squads'] = (C, D)
    result = plan(**args)
    assert (result.action, result.squads, result.removed_squads) == ('unchanged', (C, D), ())


@pytest.mark.parametrize(
    'fault',
    [
        'pending',
        'review',
        'pending_bytes',
        'grace',
        'fingerprint',
        'stale',
        'panel',
        'unknown',
        'mixed',
        'extra_source',
    ],
)
@pytest.mark.parametrize('side', ['source', 'target'])
def test_uncertain_or_unpinned_inputs_wait_as_a_whole(fault, side):
    args = values()
    key = side + '_projection'
    p = args[key]
    if fault == 'pending':
        args[key] = replace(p, bs=replace(p.bs, state='pending', squads=None))
    elif fault == 'review':
        args[key] = replace(p, bs=replace(p.bs, needs_review=True))
    elif fault == 'pending_bytes':
        args[key] = replace(p, bs=replace(p.bs, pending_bytes=1))
    elif fault == 'grace':
        args[key] = replace(p, preserve_grace=True)
    elif fault == 'fingerprint':
        args[key] = replace(p, topology_fingerprint='d' * 64)
    elif fault == 'stale':
        args[side + '_evidence'] = replace(args[side + '_evidence'], valid_until=NOW)
    elif fault == 'panel':
        args[side + '_evidence'] = replace(args[side + '_evidence'], panel_identity='other')
    elif fault == 'unknown':
        args['current_squads'] = (A, X)
    elif fault == 'mixed':
        args['current_squads'] = (A, D)
    else:
        args['expected_source_squads'] = (A, B, X)
    result = plan(**args)
    assert result.action == 'wait' and result.squads is None and result.removed_squads == ()


@pytest.mark.parametrize(
    'fault',
    [
        'overlap',
        'cross_group_squad',
        'bad_uuid',
        'duplicate',
        'remaining',
        'squads',
        'negative_pending',
    ],
)
def test_invalid_transition_is_rejected(fault):
    args = values()
    p = args['target_projection']
    e = args['target_evidence']
    if fault == 'overlap':
        args['target_evidence'] = replace(e, topology=replace(NEW, bs_squads=(C,)))
    elif fault == 'cross_group_squad':
        args['target_evidence'] = replace(e, topology=replace(NEW, bs_squads=(A,)))
    elif fault == 'node_move':
        args['target_evidence'] = replace(e, topology=replace(NEW, ordinary_nodes=(N2,), bs_nodes=(N1,)))
    elif fault == 'node_add':
        args['target_evidence'] = replace(e, topology=replace(NEW, ordinary_nodes=(N1, X)))
    elif fault == 'bad_uuid':
        args['current_squads'] = ('bad',)
    elif fault == 'duplicate':
        args['current_squads'] = (A, A)
    elif fault == 'mode':
        args['target_projection'] = replace(p, ordinary=replace(p.ordinary, mode='finite', remaining_bytes=1))
    elif fault == 'remaining':
        args['target_projection'] = replace(p, bs=replace(p.bs, remaining_bytes=0))
    elif fault == 'squads':
        args['target_projection'] = replace(p, bs=replace(p.bs, squads=(C,)))
    else:
        args['target_projection'] = replace(p, bs=replace(p.bs, pending_bytes=-1))
    with pytest.raises(ValueError):
        plan(**args)


@pytest.mark.parametrize('current', [(), (A,), (B,), (A, B)])
def test_pinned_source_subset_is_explicit_authority(current):
    args = values()
    args.update(expected_source_squads=current, current_squads=current)
    assert plan(**args).squads == (C, D)


def test_exhausted_target_removes_old_group_without_granting_new_group():
    args = values()
    p = args['target_projection']
    args['target_projection'] = replace(p, bs=replace(p.bs, remaining_bytes=0, state='exhausted', squads=()))
    result = plan(**args)
    assert result.squads == (C,) and result.removed_squads == (A, B)


def test_same_group_shared_squad_is_not_removed():
    args = values()
    topology = replace(NEW, ordinary_squads=(A, C))
    args['target_evidence'] = replace(args['target_evidence'], topology=topology)
    target = args['target_projection']
    args['target_projection'] = replace(target, ordinary=replace(target.ordinary, squads=(A, C)))
    result = plan(**args)
    assert result.squads == (A, C, D)
    assert result.removed_squads == (B,)


def test_noncanonical_topology_cannot_produce_noncanonical_target():
    args = values()
    squad = 'ABCDEF01-0000-4000-8000-000000000000'
    args['target_evidence'] = replace(args['target_evidence'], topology=replace(NEW, ordinary_squads=(squad,)))
    target = args['target_projection']
    args['target_projection'] = replace(target, ordinary=replace(target.ordinary, squads=(squad,)))
    with pytest.raises(ValueError):
        plan(**args)


@pytest.mark.parametrize('side', ['source', 'target'])
def test_future_observation_and_grace_candidate_wait(side):
    args = values()
    args[side + '_evidence'] = replace(args[side + '_evidence'], observed_at=NOW + timedelta(seconds=1))
    assert plan(**args).action == 'wait'
    args = values()
    args[side + '_projection'] = replace(args[side + '_projection'], grace_candidate='expired')
    assert plan(**args).action == 'wait'


@pytest.mark.parametrize('source_mode', ['finite', 'unlimited'])
def test_inherited_bs_shutdown_can_only_remove_bs_access(source_mode):
    args = values()
    if source_mode == 'unlimited':
        source = args['source_projection']
        args['source_projection'] = replace(source, bs=replace(source.bs, mode='unlimited', remaining_bytes=None))
    target = args['target_projection']
    args['target_projection'] = replace(target, bs=GroupProjection('disabled', None, 0, False, 'disabled', ()))
    with pytest.raises(ValueError):
        plan(**args)
    result = plan(**args, allow_disabled_bs_target=True)
    assert (result.action, result.squads, result.removed_squads) == ('patch', (C,), (A, B))


@pytest.mark.parametrize('fault', ['pending', 'review', 'remaining', 'squads', 'ordinary', 'disabled_source'])
def test_restrictive_bs_shutdown_never_broadens_or_partially_grants(fault):
    args = values()
    target = args['target_projection']
    disabled = GroupProjection('disabled', None, 0, False, 'disabled', ())
    args['target_projection'] = replace(target, bs=disabled)
    if fault == 'pending':
        args['target_projection'] = replace(target, bs=replace(disabled, pending_bytes=1))
    elif fault == 'review':
        args['target_projection'] = replace(target, bs=replace(disabled, needs_review=True))
    elif fault == 'remaining':
        args['target_projection'] = replace(target, bs=replace(disabled, remaining_bytes=0))
    elif fault == 'squads':
        args['target_projection'] = replace(target, bs=replace(disabled, squads=(D,)))
    elif fault == 'ordinary':
        args['target_projection'] = replace(target, ordinary=disabled)
    else:
        source = args['source_projection']
        args['source_projection'] = replace(source, bs=GroupProjection('disabled', None, 0, False, 'disabled', ()))
    if fault in ('pending', 'review'):
        assert plan(**args, allow_disabled_bs_target=True).action == 'wait'
    else:
        with pytest.raises(ValueError):
            plan(**args, allow_disabled_bs_target=True)


@pytest.mark.parametrize('change', ['node_add', 'node_move', 'mode'])
async def test_fresh_pinned_transition_accepts_new_target_nodes_and_modes(change):
    args = values()
    projection, evidence = args['target_projection'], args['target_evidence']
    if change == 'node_add':
        args['target_evidence'] = replace(evidence, topology=replace(NEW, ordinary_nodes=(N1, X)))
    elif change == 'node_move':
        args['target_evidence'] = replace(evidence, topology=replace(NEW, ordinary_nodes=(N2,), bs_nodes=(N1,)))
    else:
        args['target_projection'] = replace(
            projection, ordinary=replace(projection.ordinary, mode='finite', remaining_bytes=1)
        )
    assert plan(**args).squads == (C, D)
