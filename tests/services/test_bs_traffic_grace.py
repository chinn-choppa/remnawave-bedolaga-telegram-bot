import pytest

from app.services.bs_traffic.access import GroupAccess
from app.services.bs_traffic.grace import grace_candidate_reason
from app.services.grace_access_service import GraceReason


@pytest.mark.parametrize(
    'ordinary,bs',
    [
        (GroupAccess('unlimited'), GroupAccess('finite', 0)),
        (GroupAccess('finite', 0), GroupAccess('unlimited')),
        (GroupAccess('finite', 1), GroupAccess('finite', 0)),
        (GroupAccess('finite', 0), GroupAccess('finite', 1)),
    ],
)
def test_one_entitled_group_prevents_account_wide_grace(ordinary, bs):
    assert grace_candidate_reason(ordinary, bs, expired=False, blocked=False) is None


def test_both_exhausted_can_request_limited_grace():
    assert (
        grace_candidate_reason(GroupAccess('finite', 0), GroupAccess('finite', 0), expired=False, blocked=False)
        is GraceReason.LIMITED
    )


@pytest.mark.parametrize('remaining', [0, 100])
def test_unsettled_group_is_not_mistaken_for_confirmed_exhaustion(remaining):
    assert (
        grace_candidate_reason(
            GroupAccess('finite', 0), GroupAccess('finite', remaining, ready=False), expired=False, blocked=False
        )
        is None
    )


def test_expired_term_can_request_grace_without_spending_or_resetting_paid_balances():
    ordinary, bs = GroupAccess('finite', 150), GroupAccess('finite', 100)
    assert grace_candidate_reason(ordinary, bs, expired=True, blocked=False) is GraceReason.EXPIRED
    assert (ordinary.remaining_bytes, bs.remaining_bytes) == (150, 100)


@pytest.mark.parametrize('expired', [True, False])
def test_admin_block_prevents_any_grace(expired):
    assert (
        grace_candidate_reason(GroupAccess('finite', 0), GroupAccess('finite', 0), expired=expired, blocked=True)
        is None
    )


def test_disabled_configuration_is_not_a_traffic_incident():
    assert (
        grace_candidate_reason(GroupAccess('disabled'), GroupAccess('disabled'), expired=False, blocked=False) is None
    )


@pytest.mark.parametrize('field', ['expired', 'blocked'])
@pytest.mark.parametrize('value', [None, 0, 1, 'false'])
def test_unknown_global_state_cannot_be_interpreted_as_permission(field, value):
    flags = {'expired': False, 'blocked': False, field: value}
    with pytest.raises(ValueError):
        grace_candidate_reason(GroupAccess('finite', 0), GroupAccess('finite', 0), **flags)
