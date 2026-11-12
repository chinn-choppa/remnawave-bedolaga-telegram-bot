from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from app.services.grace_access_codec import GraceSnapshotError, _model_to_session, _session_to_model
from tests.database.test_bs_traffic_grace_transition_postgres import grace_session


SCOPE = '11111111-1111-4111-8111-111111111111'
SQUAD = '22222222-2222-4222-8222-222222222222'


def context(**changes):
    from app.services.grace_access_service import GraceSplitAccess

    values = dict(
        subscription_id=1,
        panel_user_id=12,
        panel_scope=SCOPE,
        account_generation='a' * 64,
        tariff_id=1,
        ledger_revision=3,
        topology_fingerprint='b' * 64,
        paid_squads=(SQUAD,),
        observed_at=datetime.now(UTC) - timedelta(seconds=1),
        valid_until=datetime.now(UTC) + timedelta(seconds=30),
    )
    return GraceSplitAccess(**(values | changes))


def test_split_snapshot_roundtrips_and_has_distinct_version():
    original = grace_session()
    session = replace(
        original, split_before=context(), panel_before=replace(original.panel_before, squad_uuids=(SQUAD,))
    )
    model = _session_to_model(session)
    assert model.snapshot_version == 4
    assert model.billing_before['split_access']['paid_squads'] == [SQUAD]
    assert _model_to_session(model).split_before == session.split_before


def test_ordinary_v3_snapshot_does_not_invent_split_context():
    model = _session_to_model(grace_session())
    assert model.snapshot_version == 3
    assert 'split_access' not in model.billing_before
    assert _model_to_session(model).split_before is None


@pytest.mark.parametrize('value', [None, {}, [], {'panel_scope': SCOPE}])
def test_version4_missing_or_partial_context_is_rejected(value):
    model = _session_to_model(grace_session())
    model.snapshot_version = 4
    model.billing_before['split_access'] = value
    with pytest.raises(GraceSnapshotError):
        _model_to_session(model)


@pytest.mark.parametrize(
    'field,value',
    [
        ('subscription_id', 2),
        ('panel_user_id', 13),
    ],
)
def test_context_cannot_be_attached_to_another_subscription_or_account(field, value):
    with pytest.raises(GraceSnapshotError):
        _session_to_model(replace(grace_session(), split_before=context(**{field: value})))


@pytest.mark.parametrize(
    'changes',
    [
        {'panel_scope': 'invalid'},
        {'account_generation': 'bad'},
        {'topology_fingerprint': 'bad'},
        {'ledger_revision': -1},
        {'ledger_revision': True},
        {'tariff_id': 0},
        {'paid_squads': (SQUAD, SQUAD)},
        {'paid_squads': ['invalid']},
        {'paid_squads': ('invalid',)},
    ],
)
def test_invalid_split_provenance_is_rejected(changes):
    with pytest.raises(ValueError):
        context(**changes)


def test_split_snapshot_must_match_actual_pre_grace_panel_selection():
    with pytest.raises(GraceSnapshotError):
        _session_to_model(replace(grace_session(), split_before=context()))
