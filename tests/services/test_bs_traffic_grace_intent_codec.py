import hashlib
import json
from dataclasses import replace

import pytest

from app.services.bs_traffic.grace_intent import _decode, _encode
from app.services.bs_traffic.ledger import LedgerConflict
from tests.services.test_bs_traffic_grace_confirmation import state


@pytest.mark.parametrize('devices', [None, 0, 2, -1])
def test_source_preserves_signed_panel_device_evidence(devices):
    source = replace(state(), hwid_device_limit=devices, traffic_limit_bytes=200, traffic_limit_strategy='MONTH')
    data = _encode(source, paid=False)
    digest = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert _decode(data, digest, paid=False) == source


def test_negative_source_device_does_not_authorize_negative_paid_target():
    with pytest.raises(LedgerConflict):
        _encode(replace(state(), hwid_device_limit=-1))


def test_changed_source_json_fails_its_integrity_check():
    source = _encode(state(), paid=False)
    digest = hashlib.sha256(json.dumps(source, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    with pytest.raises(LedgerConflict):
        _decode(source | {'traffic_limit_strategy': 'MONTH'}, digest, paid=False)
