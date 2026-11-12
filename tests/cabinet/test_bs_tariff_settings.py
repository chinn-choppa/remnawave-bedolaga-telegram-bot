import pytest
from pydantic import ValidationError

from app.cabinet.schemas.tariffs import BsTrafficSettings, TariffCreateRequest, TariffUpdateRequest


SQUAD = '11111111-1111-4111-8111-111111111111'


def test_bs_settings_roundtrip_and_omission():
    settings = BsTrafficSettings(squad_uuids=[SQUAD], limit_gb=10, topup_enabled=True, topup_packages={'5': 12300})
    request = TariffCreateRequest(name='Test', bs_traffic_settings=settings)
    assert request.bs_traffic_settings.topup_packages == {'5': 12300}
    assert TariffCreateRequest(name='Old').bs_traffic_settings is None
    assert 'bs_traffic_settings' not in TariffUpdateRequest(name='Rename').model_fields_set
    assert 'bs_traffic_settings' in TariffUpdateRequest(bs_traffic_settings=None).model_fields_set


@pytest.mark.parametrize(
    'change',
    [
        {'squad_uuids': []},
        {'squad_uuids': [SQUAD, SQUAD]},
        {'squad_uuids': ['not-uuid']},
        {'limit_gb': -1},
        {'limit_gb': True},
        {'topup_packages': {'0': 100}},
        {'topup_packages': {'01': 100}},
        {'topup_packages': {'5': -1}},
        {'topup_packages': {'5': True}},
        {'topup_packages': {'5': 1.5}},
        {'topup_enabled': True},
        {'unknown': 1},
    ],
)
def test_invalid_bs_terms_rejected(change):
    with pytest.raises(ValidationError):
        BsTrafficSettings(**({'squad_uuids': [SQUAD], 'limit_gb': 10} | change))


def test_unlimited_cannot_sell_finite_bs_packages():
    with pytest.raises(ValidationError):
        BsTrafficSettings(squad_uuids=[SQUAD], limit_gb=0, topup_enabled=True, topup_packages={'5': 100})
    assert BsTrafficSettings(squad_uuids=[SQUAD], limit_gb=0).limit_gb == 0
