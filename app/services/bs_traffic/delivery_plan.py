"""Merge group masks into a complete internal-squad selection, without I/O.

The panel replaces the whole selection. A group with unknown usage must keep
exactly its current selection; it must not acquire its entire configured group.
This plan is not a delivery authorization: ownership, fresh topology, grace and
subscription state must be rechecked by the serialized delivery coordinator.
Unknown squads need reconciliation (including former tariff and grace squads).
"""

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from app.services.bs_traffic.policy import AccessProjection, _topology
from app.services.bs_traffic.topology import SplitTopology


@dataclass(frozen=True)
class SquadPlan:
    action: Literal['wait', 'unchanged', 'patch']
    squads: tuple[str, ...] | None
    removed_squads: tuple[str, ...] = ()


def _selection(value):
    if type(value) is not tuple or len(value) > 128:
        raise ValueError('Explicit bounded squad selection required')
    try:
        valid = all(isinstance(item, str) and str(UUID(item)) == item for item in value)
    except ValueError:
        valid = False
    if not valid or value != tuple(sorted(set(value))):
        raise ValueError('Squads must be canonical, unique and sorted')
    return set(value)


def plan_squads(projection: AccessProjection, *, topology: SplitTopology, current_squads: tuple[str, ...]) -> SquadPlan:
    """Preserve unknown groups; do not touch external squads or account status.

    Administrative disable/expiry also require their account-level coordinator:
    this helper does not revoke unknown overlay squads or change user status.
    """
    if not isinstance(projection, AccessProjection):
        raise ValueError('Explicit access projection required')
    _topology(topology)
    current = _selection(current_squads)
    desired = set()
    managed = set(topology.ordinary_squads) | set(topology.bs_squads)
    for group, squads in ((projection.ordinary, topology.ordinary_squads), (projection.bs, topology.bs_squads)):
        allowed = set(squads)
        if group.squads is None:
            desired.update(current & allowed)
        else:
            selected = _selection(group.squads)
            if not selected <= allowed:
                raise ValueError('Projection does not belong to this topology group')
            desired.update(selected)
    if projection.preserve_grace or not current <= managed:
        return SquadPlan('wait', None)
    result = tuple(sorted(desired))
    if desired == current:
        return SquadPlan('unchanged', result)
    return SquadPlan('patch', result, tuple(sorted(current - desired)))
