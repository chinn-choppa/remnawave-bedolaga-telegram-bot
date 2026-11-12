"""Bounded read-only topology evidence, not a guarantee of live enforcement.

The panel does not offer an atomic topology snapshot. Activation must recheck
this evidence and subsequent reconciliation must detect panel-side changes.
These reads do not test socket destruction or prove that statistics are flushed.
"""

import asyncio
import hashlib
import json
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from app.services.bs_traffic.topology import SplitTopology, TopologyConflict, resolve_topology


class TopologyReader(Protocol):
    async def get_system_configuration(self) -> dict[str, Any]: ...

    async def get_internal_squad_topology(self, squad_uuid: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class TopologyEvidence:
    panel_identity: str
    topology: SplitTopology
    fingerprint: str
    observed_at: datetime
    valid_until: datetime

    def is_fresh(self, now: datetime, *, panel_identity: str) -> bool:
        return (
            now.tzinfo is not None
            and now.utcoffset() is not None
            and panel_identity == self.panel_identity
            and self.observed_at <= now < self.valid_until
        )


def _uuid(value: object) -> str:
    if not isinstance(value, str):
        raise TopologyConflict('invalid_uuid')
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise TopologyConflict('invalid_uuid') from exc


def _selection(values: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        raise TopologyConflict('invalid_squads')
    return tuple(sorted({_uuid(value) for value in values}))


def _configuration(document: object) -> None:
    if not isinstance(document, dict):
        raise TopologyConflict('accounting_configuration')
    service, misc = document.get('service'), document.get('misc')
    if not isinstance(service, dict) or not isinstance(misc, dict):
        raise TopologyConflict('accounting_configuration')
    if (
        service.get('disableUserUsageRecords') is not False
        or service.get('cleanUsageHistory') is not False
        or type(misc.get('userUsageIgnoreBelowBytes')) is not int
        or misc['userUsageIgnoreBelowBytes'] != 0
    ):
        raise TopologyConflict('accounting_configuration')


def _aware(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('An aware observation time is required')
    return now


async def collect_topology_evidence(
    api: TopologyReader,
    *,
    panel_identity: str,
    ordinary_squads: Sequence[str],
    bs_squads: Sequence[str],
    max_squads: int = 64,
    max_nodes_per_squad: int = 2048,
    timeout_seconds: float = 15,
    max_age_seconds: float = 30,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> TopologyEvidence:
    """Read each selected squad once with a total deadline and no fallback cache.

    Defaults bound this preflight, not the capacity or SLA of a deployment.
    Payload cardinality checks follow HTTP decoding. The production RemnaWave
    adapter bounds configuration to 1MiB and each topology response to 8MiB;
    injected readers must provide their own transport limits.
    """
    if not isinstance(panel_identity, str) or not panel_identity.strip():
        raise ValueError('Panel identity is required')
    if any(type(value) is not int or value <= 0 for value in (max_squads, max_nodes_per_squad)):
        raise ValueError('Positive count limits are required')
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0
        for value in (timeout_seconds, max_age_seconds)
    ):
        raise ValueError('Positive finite time limits are required')
    if not isinstance(ordinary_squads, (list, tuple)) or not isinstance(bs_squads, (list, tuple)):
        raise TopologyConflict('invalid_squads')
    if len(ordinary_squads) + len(bs_squads) > max_squads:
        raise TopologyConflict('too_many_squads')
    ordinary, bs = _selection(ordinary_squads), _selection(bs_squads)
    if not ordinary or not bs:
        raise TopologyConflict('empty_group')
    if set(ordinary).intersection(bs):
        raise TopologyConflict('overlapping_squads')
    started = _aware(clock())
    valid_until = started + timedelta(seconds=max_age_seconds)
    mapping: dict[str, tuple[str, ...]] = {}
    profiles: dict[str, list[tuple[str, str, tuple[str, ...]]]] = {}
    async with asyncio.timeout(timeout_seconds):
        _configuration(await api.get_system_configuration())
        for squad in ordinary + bs:
            document = await api.get_internal_squad_topology(squad)
            if not isinstance(document, dict):
                raise TopologyConflict('invalid_topology')
            if _uuid(document.get('squadUuid')) != squad:
                raise TopologyConflict('squad_identity')
            nodes = document.get('accessibleNodes')
            if not isinstance(nodes, list):
                raise TopologyConflict('invalid_nodes')
            if len(nodes) > max_nodes_per_squad:
                raise TopologyConflict('too_many_nodes')
            if any(not isinstance(node, dict) for node in nodes):
                raise TopologyConflict('invalid_nodes')
            details = []
            for node in nodes:
                node_id = _uuid(node.get('uuid'))
                profile = _uuid(node.get('configProfileUuid'))
                inbounds = node.get('activeInbounds')
                if (
                    not isinstance(inbounds, list)
                    or not inbounds
                    or any(not isinstance(tag, str) or not tag or tag.strip() != tag for tag in inbounds)
                ):
                    raise TopologyConflict('invalid_inbounds')
                details.append((node_id, profile, tuple(sorted(set(inbounds)))))
            if len({item[0] for item in details}) != len(details):
                raise TopologyConflict('duplicate_nodes')
            mapping[squad] = tuple(sorted(item[0] for item in details))
            profiles[squad] = sorted(details)
    topology = resolve_topology(ordinary, bs, mapping)
    completed = _aware(clock())
    if not started <= completed < valid_until:
        raise TopologyConflict('stale_evidence')
    fingerprint = hashlib.sha256(
        json.dumps(
            {'panel': panel_identity, 'ordinary': ordinary, 'bs': bs, 'nodes': mapping, 'profiles': profiles},
            sort_keys=True,
            separators=(',', ':'),
        ).encode()
    ).hexdigest()
    return TopologyEvidence(panel_identity, topology, fingerprint, started, valid_until)
