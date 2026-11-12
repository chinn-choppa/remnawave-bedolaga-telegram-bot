import pytest

from app.services.bs_traffic.access import GroupAccess, desired_squads
from app.services.bs_traffic.topology import resolve_topology


def topology():
    return resolve_topology(('ordinary',), ('bs',), {'ordinary': ('n1',), 'bs': ('n2',)})


@pytest.mark.parametrize('ordinary_limit', [None, 100])
@pytest.mark.parametrize('bs_limit', [None, 100])
def test_all_four_limit_combinations_keep_both_entitled_groups(ordinary_limit, bs_limit):
    ordinary = GroupAccess('unlimited' if ordinary_limit is None else 'finite', ordinary_limit)
    bs = GroupAccess('unlimited' if bs_limit is None else 'finite', bs_limit)
    assert desired_squads(topology(), ordinary, bs, subscription_allowed=True) == ('bs', 'ordinary')


@pytest.mark.parametrize(
    ('ordinary', 'bs', 'expected'),
    [
        (0, 100, ('bs',)),
        (100, 0, ('ordinary',)),
        (0, 0, ()),
        (1, 1, ('bs', 'ordinary')),
    ],
)
def test_only_exhausted_group_is_removed(ordinary, bs, expected):
    assert (
        desired_squads(
            topology(), GroupAccess('finite', ordinary), GroupAccess('finite', bs), subscription_allowed=True
        )
        == expected
    )


def test_paid_but_not_reconciled_group_is_not_restored_early():
    assert desired_squads(
        topology(), GroupAccess('unlimited'), GroupAccess('finite', 100, ready=False), subscription_allowed=True
    ) == ('ordinary',)


def test_expiry_or_admin_block_takes_precedence_over_remaining_allowances():
    assert (
        desired_squads(topology(), GroupAccess('unlimited'), GroupAccess('unlimited'), subscription_allowed=False) == ()
    )


@pytest.mark.parametrize(
    ('mode', 'remaining'),
    [
        ('finite', None),
        ('finite', -1),
        ('finite', True),
        ('finite', 1.5),
        ('unlimited', 0),
        ('disabled', 10),
        ('unknown', None),
    ],
)
def test_invalid_quota_is_not_treated_as_unlimited(mode, remaining):
    with pytest.raises(ValueError):
        GroupAccess(mode, remaining)
