"""Request validation is pure; persistence is tested only on PostgreSQL."""

from datetime import date, datetime

import pytest

from app.services.bs_traffic.observations import MAX_IDENTITIES, prepare_poll


SCOPE = '11111111-1111-4111-8111-111111111111'
NODE = '22222222-2222-4222-8222-222222222222'


def request(**changes):
    args = dict(panel_scope=SCOPE, day=date(2026, 11, 4), node_uuids=(NODE,), identities={7: 'generation'})
    args.update(changes)
    return prepare_poll(**args)


@pytest.mark.parametrize(
    'changes',
    [
        {'panel_scope': 'invalid'},
        {'node_uuids': ()},
        {'node_uuids': (NODE,) * 101},
        {'node_uuids': (None,)},
        {'day': datetime(2026, 11, 4)},
        {'identities': {}},
        {'identities': {True: 'generation'}},
        {'identities': {'7': 'generation'}},
        {'identities': {7: ''}},
        {'identities': {7: 'a' * 129}},
        {'identities': {7: ' trailing '}},
    ],
)
def test_invalid_request_is_rejected(changes):
    with pytest.raises(ValueError):
        request(**changes)


def test_mapping_is_copied_and_has_a_hard_budget():
    original = {7: 'generation'}
    prepared = request(identities=original)
    original[7] = 'changed'
    assert prepared['identities'] == {'7': 'generation'}
    assert prepared['selected_accounts'] == 1
    with pytest.raises(ValueError):
        request(identities=dict.fromkeys(range(1, MAX_IDENTITIES + 2), 'g'))
