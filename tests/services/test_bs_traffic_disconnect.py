"""Quota loss cannot widen a disconnect to the other paid traffic pool."""

import pytest

from app.services.bs_traffic import access
from app.services.bs_traffic.access import GroupAccess
from app.services.bs_traffic.topology import resolve_topology


TOPOLOGY = resolve_topology(['ordinary'], ['bs'], {'ordinary': ['node-a'], 'bs': ['node-b']})
PAID = GroupAccess('finite', 100)
EMPTY = GroupAccess('finite', 0)
UNLIMITED = GroupAccess('unlimited')
UNKNOWN = GroupAccess('finite', 0, ready=False)


@pytest.mark.parametrize(
    ('before', 'after', 'expected'),
    [
        ((PAID, PAID), (PAID, EMPTY), ('node-b',)),
        ((PAID, PAID), (EMPTY, PAID), ('node-a',)),
        ((PAID, PAID), (EMPTY, EMPTY), ('node-a', 'node-b')),
        ((UNLIMITED, PAID), (UNLIMITED, EMPTY), ('node-b',)),
        ((PAID, UNLIMITED), (EMPTY, UNLIMITED), ('node-a',)),
        ((UNLIMITED, UNLIMITED), (UNLIMITED, UNLIMITED), ()),
        ((EMPTY, PAID), (EMPTY, PAID), ()),
        ((PAID, PAID), (PAID, UNKNOWN), ()),
        ((PAID, EMPTY), (PAID, PAID), ()),
        ((PAID, PAID), (PAID, GroupAccess('disabled')), ()),
    ],
)
def test_only_new_confirmed_exhaustion_selects_nodes(before, after, expected):
    assert access.quota_disconnect_nodes(TOPOLOGY, before=before, after=after, enabled=True) == expected


def test_operator_opt_out_disables_drop_for_both_pools():
    assert access.quota_disconnect_nodes(TOPOLOGY, before=(PAID, PAID), after=(EMPTY, EMPTY), enabled=False) == ()


@pytest.mark.parametrize('enabled', ['true', 'false', 1, None])
def test_truthy_configuration_is_not_operator_consent(enabled):
    with pytest.raises(ValueError):
        access.quota_disconnect_nodes(TOPOLOGY, before=(PAID, PAID), after=(EMPTY, EMPTY), enabled=enabled)
