"""Pre-claim checks for quota disconnects, not a durable queue or a panel CAS.

The future delivery worker must read current entitlements under its database
lock, call this guard, persist the claim and commit BEFORE network I/O. A claim
is never reset to pending after a crash or an ambiguous HTTP result. The panel
has no entitlement revision precondition: a change after the final check can
still race with the request. This module does not close that remote race.
"""

from dataclasses import dataclass
from typing import Literal


AttemptState = Literal['pending', 'attempted', 'accepted', 'unknown', 'cancelled']
DropDecision = Literal['claim', 'wait', 'cancel', 'skip']
ATTEMPT_STATES = frozenset({'pending', 'attempted', 'accepted', 'unknown', 'cancelled'})


@dataclass(frozen=True)
class DropSnapshot:
    """Internal snapshot of a specific quota transition and panel identity.

    Revision must advance on purchases, tariff/topology changes, grace and
    account recreation, not only exhaustion. Nodes come from the validated
    quota plan; UUID syntax is additionally enforced by the HTTP adapter.
    Numeric panel ID alone is insufficient when accounts are recreated. Use
    identity.account_generation from a verified panel account, never shortUuid
    or a password: credential rotation does not recreate an account.
    """

    subscription_id: int
    user_id: int
    panel_user_id: int
    account_generation: str
    revision: int
    topology_fingerprint: str
    node_uuids: tuple[str, ...]

    def __post_init__(self):
        for value in (self.subscription_id, self.user_id, self.panel_user_id):
            if type(value) is not int or not 0 < value < 2**53:
                raise ValueError('Drop identity requires a positive exact integer')
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError('Drop revision requires a nonnegative exact integer')
        if (
            not isinstance(self.account_generation, str)
            or len(self.account_generation) != 64
            or any(char not in '0123456789abcdef' for char in self.account_generation)
        ):
            raise ValueError('Drop requires a panel account generation identifier')
        if (
            not isinstance(self.topology_fingerprint, str)
            or len(self.topology_fingerprint) != 64
            or any(char not in '0123456789abcdef' for char in self.topology_fingerprint)
        ):
            raise ValueError('Drop requires a validated topology fingerprint')
        if (
            type(self.node_uuids) is not tuple
            or len(self.node_uuids) > 100
            or any(not isinstance(node, str) or not node or node.strip() != node for node in self.node_uuids)
            or self.node_uuids != tuple(sorted(set(self.node_uuids)))
        ):
            raise ValueError('Drop nodes must be bounded, immutable, unique and sorted')


def assess_drop(
    *,
    planned: DropSnapshot,
    current: DropSnapshot,
    applied_revision: int,
    attempt_state: AttemptState,
    enabled: bool,
    eligible: bool,
    evidence_fresh: bool,
    ownership_verified: bool,
) -> DropDecision:
    """Reject stale work; only a confirmed restriction can precede a drop.

    ``applied_revision`` means account access was read back and matched, not
    merely that the panel queued a write. ``eligible`` includes active paid
    access without a grace overlay or administrative block. Ownership must be
    verified independently for the exact subscription, including multi mode.
    A ``claim`` decision is permission to persist an attempt, NOT permission
    to issue HTTP before that transaction commits. No network/DB side effects.
    """
    if any(type(flag) is not bool for flag in (enabled, eligible, evidence_fresh, ownership_verified)):
        raise ValueError('Drop authorization flags must be explicit booleans')
    if attempt_state not in ATTEMPT_STATES:
        raise ValueError('Unknown drop attempt state')
    if type(applied_revision) is not int or applied_revision < -1:
        raise ValueError('Invalid applied entitlement revision')
    if attempt_state != 'pending':
        return 'skip'
    if not enabled or not eligible or not ownership_verified or not planned.node_uuids or planned != current:
        return 'cancel'
    if applied_revision > current.revision:
        return 'cancel'
    if not evidence_fresh or applied_revision < current.revision:
        return 'wait'
    return 'claim'
