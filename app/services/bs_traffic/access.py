"""Pure group access projection; no account-wide status or quota writes."""

from dataclasses import dataclass
from typing import Literal

from app.services.bs_traffic.topology import SplitTopology


@dataclass(frozen=True)
class GroupAccess:
    mode: Literal['disabled', 'finite', 'unlimited']
    remaining_bytes: int | None = None
    ready: bool = True

    def __post_init__(self):
        if type(self.ready) is not bool:
            raise ValueError('Readiness must be explicit')
        if self.mode == 'finite':
            if type(self.remaining_bytes) is not int or self.remaining_bytes < 0:
                raise ValueError('Finite access requires a nonnegative integer balance')
        elif self.mode not in ('disabled', 'unlimited') or self.remaining_bytes is not None:
            raise ValueError('Invalid group access mode or balance')

    @property
    def allowed(self) -> bool:
        return self.ready and (
            self.mode == 'unlimited'
            or (self.mode == 'finite' and self.remaining_bytes is not None and self.remaining_bytes > 0)
        )


def desired_squads(
    topology: SplitTopology,
    ordinary: GroupAccess,
    bs: GroupAccess,
    *,
    subscription_allowed: bool,
) -> tuple[str, ...]:
    """Project both groups; an empty result is intentional, not 'all servers'.

    Callers establish entitlement readiness, expiry/admin/grace eligibility and
    a validated topology. The result must be delivered through panel_sync with
    explicit empty-list semantics, never the legacy empty selection fallback.
    """
    if type(subscription_allowed) is not bool:
        raise ValueError('Subscription eligibility must be explicit')
    if not subscription_allowed:
        return ()
    return tuple(
        sorted(
            set(topology.ordinary_squads if ordinary.allowed else ()).union(topology.bs_squads if bs.allowed else ())
        )
    )


def quota_disconnect_nodes(
    topology: SplitTopology,
    *,
    before: tuple[GroupAccess, GroupAccess],
    after: tuple[GroupAccess, GroupAccess],
    enabled: bool,
) -> tuple[str, ...]:
    """Select nodes for newly confirmed quota exhaustion within one topology.

    Both tuples are (ordinary, BS). This is a pure plan, not delivery or an
    exactly-once guarantee. The worker must compare the current revision and
    persist its claim before requesting a drop. Tariff/topology changes,
    expiry, administrative blocks and grace transitions need their own plans;
    unknown usage must never be interpreted as a confirmed empty balance.
    """
    if type(enabled) is not bool:
        raise ValueError('Disconnect opt-in must be an explicit boolean')
    if not enabled:
        return ()
    nodes: set[str] = set()
    for group_nodes, previous, current in zip((topology.ordinary_nodes, topology.bs_nodes), before, after, strict=True):
        if (
            previous.mode == 'finite'
            and previous.allowed
            and current.ready
            and current.mode == 'finite'
            and current.remaining_bytes == 0
        ):
            nodes.update(group_nodes)
    return tuple(sorted(nodes))
