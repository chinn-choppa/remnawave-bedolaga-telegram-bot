"""Split entitlement input to grace eligibility, not a grace issuance bypass.

The existing grace policy must still check mode, kind, suppression and incident
identity. Runtime integration also needs a split-aware restore and usage boundary.
"""

from app.services.bs_traffic.access import GroupAccess
from app.services.grace_access_service import GraceReason


def grace_candidate_reason(
    ordinary: GroupAccess,
    bs: GroupAccess,
    *,
    expired: bool,
    blocked: bool,
) -> GraceReason | None:
    if type(expired) is not bool or type(blocked) is not bool:
        raise ValueError('Expiry and administrative block must be explicit')
    if blocked:
        return None
    if expired:
        return GraceReason.EXPIRED
    groups = (ordinary, bs)
    if any(group.allowed for group in groups):
        return None
    enabled = tuple(group for group in groups if group.mode != 'disabled')
    if enabled and all(group.ready and group.mode == 'finite' and group.remaining_bytes == 0 for group in enabled):
        return GraceReason.LIMITED
    return None
