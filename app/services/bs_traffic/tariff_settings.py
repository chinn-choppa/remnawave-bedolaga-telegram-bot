"""Validated tariff terms for the separately metered BS group."""

from typing import Annotated
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


class BsTrafficSettings(BaseModel):
    model_config = ConfigDict(extra='forbid')

    enabled: bool = Field(True, strict=True)
    # Internal stored cutoff; input serializers never forward a client value.
    disabled_at: AwareDatetime | None = Field(None, exclude=True)
    squad_uuids: list[str] = Field(min_length=1, max_length=64)
    limit_gb: int = Field(ge=0, le=1_000_000, strict=True, description='0 = unlimited')
    topup_enabled: bool = Field(False, strict=True)
    topup_packages: dict[str, Annotated[int, Field(ge=0, le=2_147_483_647, strict=True)]] = Field(
        default_factory=dict, max_length=100
    )
    max_topup_gb: int = Field(0, ge=0, le=1_000_000, strict=True)

    @field_validator('squad_uuids')
    @classmethod
    def canonical_squads(cls, values):
        normalized = [str(UUID(value)) for value in values]
        if len(set(normalized)) != len(normalized):
            raise ValueError('Duplicate BS squads')
        return sorted(normalized)

    @field_validator('topup_packages')
    @classmethod
    def package_sizes(cls, values):
        for key in values:
            if not key.isascii() or not key.isdecimal() or not 1 <= int(key) <= 100_000 or str(int(key)) != key:
                raise ValueError('Package size must be canonical positive integer GB')
        return values

    @model_validator(mode='after')
    def valid_topup(self):
        if self.topup_enabled and (self.limit_gb == 0 or not self.topup_packages):
            raise ValueError('BS top-up requires a finite allowance and packages')
        return self


def normalize_bs_settings(value, ordinary_squads):
    if value is None:
        return None
    settings = BsTrafficSettings.model_validate(value)
    if set(settings.squad_uuids) & {str(UUID(item)) for item in ordinary_squads or []}:
        raise ValueError('Ordinary and BS squads must not overlap')
    return settings.model_dump(mode='json')


def traffic_package_price(tariff, *, group: str, gb: int, current_limit_bytes: int) -> int:
    """Select a configured package, before discounts, from exactly one group.

    ``current_limit_bytes`` is the group's active allowance ceiling, not usage
    or the other group's balance. Callers revalidate under their purchase lock.
    A valid quote does not establish split activation or grant any traffic.
    """
    if group not in ('ordinary', 'bs'):
        raise ValueError('Unknown traffic group')
    if type(gb) is not int or not 1 <= gb <= 100_000:
        raise ValueError('Invalid traffic package size')
    if type(current_limit_bytes) is not int or current_limit_bytes < 0:
        raise ValueError('Invalid current traffic limit')
    if tariff is None:
        raise ValueError('Tariff not found')
    if group == 'bs':
        config = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
        if not config.enabled:
            raise ValueError('BS purchases are disabled for this tariff')
        enabled, limit = config.topup_enabled, config.limit_gb
        maximum, packages = config.max_topup_gb, config.topup_packages
        price = packages.get(str(gb))
    else:
        enabled, limit = tariff.traffic_topup_enabled, tariff.traffic_limit_gb
        maximum = tariff.max_topup_traffic_gb or 0
        price = tariff.get_traffic_topup_price(gb)
    if not enabled or not limit:
        raise ValueError('Traffic top-up is disabled or unlimited')
    if type(price) is not int or price <= 0:
        raise ValueError('Traffic package has no positive price configured')
    if maximum and current_limit_bytes + gb * 1024**3 > maximum * 1024**3:
        raise ValueError('Traffic top-up exceeds the tariff limit')
    return price


def require_matching_traffic_policy(tariff, policy):
    """Paid checkout and later delivery must agree on the same group access."""
    if tariff is None or tariff.id != policy['tariff_id']:
        raise ValueError('Traffic tariff changed; reconciliation required')
    bs = normalize_bs_settings(tariff.bs_traffic_settings, tariff.allowed_squads)
    if bs is None or bs['squad_uuids'] != policy['topology']['bs_squads']:
        raise ValueError('BS tariff squads changed; reconciliation required')
    if sorted(tariff.allowed_squads or []) != policy['topology']['ordinary_squads']:
        raise ValueError('Ordinary tariff squads changed; reconciliation required')
    modes = ('finite' if tariff.traffic_limit_gb else 'unlimited', 'finite' if bs['limit_gb'] else 'unlimited')
    if modes != (policy['ordinary_mode'], policy['bs_mode']):
        raise ValueError('Tariff traffic modes changed; reconciliation required')
