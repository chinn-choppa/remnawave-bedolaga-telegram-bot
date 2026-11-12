"""Validate explicit split access against already fetched squad/node evidence.

This pure check cannot prove freshness, panel capabilities, or successful
connection termination. Callers must establish those independently before
activation. Empty legacy selections (meaning all servers) are not accepted.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


class TopologyConflict(ValueError):
    """Stable machine-readable conflict; callers localize operator-facing text."""

    def __init__(self, code: str, identifiers: tuple[str, ...] = ()):
        self.code = code
        self.identifiers = identifiers
        super().__init__(code)


@dataclass(frozen=True)
class SplitTopology:
    ordinary_squads: tuple[str, ...]
    bs_squads: tuple[str, ...]
    ordinary_nodes: tuple[str, ...]
    bs_nodes: tuple[str, ...]


def _identifiers(values: Sequence[str], code: str) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)) or any(
        not isinstance(value, str) or not value or value.strip() != value for value in values
    ):
        raise TopologyConflict(code)
    return tuple(sorted(set(values)))


def resolve_topology(
    ordinary_squads: Sequence[str],
    bs_squads: Sequence[str],
    nodes_by_squad: Mapping[str, Sequence[str]],
) -> SplitTopology:
    """Return deterministic immutable groups, or reject unproven separation.

    Node identifiers must come from the same panel and identity namespace.
    UUID syntax and API payload validation belong to the transport boundary.
    No limits are accepted here: isolation is required for finite and unlimited
    groups alike, so changing a quota cannot introduce shared-node accounting.
    """
    ordinary = _identifiers(ordinary_squads, 'invalid_squads')
    bs = _identifiers(bs_squads, 'invalid_squads')
    if not ordinary or not bs:
        raise TopologyConflict('empty_group')
    overlap = tuple(sorted(set(ordinary).intersection(bs)))
    if overlap:
        raise TopologyConflict('overlapping_squads', overlap)
    selected = ordinary + bs
    missing = tuple(sorted(squad for squad in selected if squad not in nodes_by_squad))
    if missing:
        raise TopologyConflict('missing_squads', missing)
    resolved = {squad: _identifiers(nodes_by_squad[squad], 'invalid_nodes') for squad in selected}
    empty = tuple(sorted(squad for squad, nodes in resolved.items() if not nodes))
    if empty:
        raise TopologyConflict('empty_squads', empty)
    ordinary_nodes = {node for squad in ordinary for node in resolved[squad]}
    bs_nodes = {node for squad in bs for node in resolved[squad]}
    overlap = tuple(sorted(ordinary_nodes.intersection(bs_nodes)))
    if overlap:
        raise TopologyConflict('overlapping_nodes', overlap)
    return SplitTopology(ordinary, bs, tuple(sorted(ordinary_nodes)), tuple(sorted(bs_nodes)))
