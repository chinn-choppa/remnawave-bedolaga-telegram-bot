"""A queued quota drop must not act on a newer entitlement or another account."""

from dataclasses import replace

import pytest

from app.services.bs_traffic.drop_guard import DropSnapshot, assess_drop


def snapshot(**changes):
    value = DropSnapshot(
        subscription_id=1,
        user_id=10,
        panel_user_id=100,
        account_generation='c' * 64,
        revision=4,
        topology_fingerprint='a' * 64,
        node_uuids=('node-a',),
    )
    return replace(value, **changes)


def assess(**changes):
    values = dict(
        planned=snapshot(),
        current=snapshot(),
        applied_revision=4,
        attempt_state='pending',
        enabled=True,
        eligible=True,
        evidence_fresh=True,
        ownership_verified=True,
    )
    values.update(changes)
    return assess_drop(**values)


def test_current_applied_entitlement_can_claim_one_attempt():
    assert assess() == 'claim'


@pytest.mark.parametrize(
    'changes',
    [
        {'subscription_id': 2},
        {'user_id': 20},
        {'panel_user_id': 101},
        {'account_generation': 'd' * 64},
        {'revision': 5},
        {'revision': 3},
        {'topology_fingerprint': 'b' * 64},
        {'node_uuids': ('node-b',)},
        {'node_uuids': ()},
    ],
)
def test_changed_entitlement_or_identity_cancels_old_drop(changes):
    assert assess(current=snapshot(**changes)) == 'cancel'


@pytest.mark.parametrize('attempt_state', ['attempted', 'accepted', 'unknown', 'cancelled'])
def test_restart_must_not_replay_a_claimed_or_terminal_attempt(attempt_state):
    assert assess(attempt_state=attempt_state) == 'skip'


def test_restriction_not_yet_applied_waits_without_spending_attempt():
    assert assess(applied_revision=3) == 'wait'


def test_future_applied_revision_is_inconsistent_and_cannot_drop():
    assert assess(applied_revision=5) == 'cancel'


@pytest.mark.parametrize('flag', ['enabled', 'eligible', 'ownership_verified'])
def test_disabled_or_ineligible_account_cancels_pending_work(flag):
    assert assess(**{flag: False}) == 'cancel'


def test_stale_evidence_waits_for_refresh():
    assert assess(evidence_fresh=False) == 'wait'


def test_empty_drop_plan_is_not_executable():
    assert assess(planned=snapshot(node_uuids=()), current=snapshot(node_uuids=())) == 'cancel'


@pytest.mark.parametrize('flag', ['enabled', 'eligible', 'evidence_fresh', 'ownership_verified'])
@pytest.mark.parametrize('value', [1, 'true', None])
def test_unknown_or_truthy_flags_cannot_authorize_disconnect(flag, value):
    with pytest.raises(ValueError):
        assess(**{flag: value})


@pytest.mark.parametrize('state', ['failed', 'retry', '', None])
def test_unknown_attempt_state_is_not_silently_requeued(state):
    with pytest.raises(ValueError):
        assess(attempt_state=state)


@pytest.mark.parametrize('revision', [True, -1, '4', None])
def test_invalid_entitlement_revision_is_rejected(revision):
    with pytest.raises(ValueError):
        snapshot(revision=revision)


def test_wrong_applied_revision_type_cannot_match_by_python_bool_equality():
    with pytest.raises(ValueError):
        assess(applied_revision=True)


@pytest.mark.parametrize(
    'changes',
    [
        {'subscription_id': True},
        {'subscription_id': 0},
        {'user_id': -1},
        {'panel_user_id': 2**53},
        {'panel_user_id': '100'},
        {'account_generation': ''},
        {'account_generation': None},
        {'topology_fingerprint': 'unknown'},
        {'topology_fingerprint': 'z' * 64},
        {'node_uuids': ['node-a']},
        {'node_uuids': ('node-a', 'node-a')},
        {'node_uuids': ('node-b', 'node-a')},
        {'node_uuids': (' node-a',)},
        {'node_uuids': (None,)},
        {'node_uuids': tuple(f'node-{index:03}' for index in range(101))},
    ],
)
def test_corrupted_persisted_snapshot_must_not_authorize_drop(changes):
    with pytest.raises(ValueError):
        snapshot(**changes)


def test_never_applied_projection_waits():
    assert assess(applied_revision=-1) == 'wait'


def test_opt_out_cancels_even_while_waiting_for_panel():
    assert assess(enabled=False, applied_revision=-1, evidence_fresh=False) == 'cancel'


def test_topup_and_second_exhaustion_do_not_revive_first_exhaustion_command():
    old = snapshot(revision=4)
    topped_up = snapshot(revision=5, node_uuids=())
    exhausted_again = snapshot(revision=6)
    assert assess(planned=old, current=topped_up, applied_revision=5) == 'cancel'
    assert assess(planned=old, current=exhausted_again, applied_revision=6) == 'cancel'
    assert assess(planned=exhausted_again, current=exhausted_again, applied_revision=6) == 'claim'
