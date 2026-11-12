"""Explicitly owned, bounded topology cache for one immutable panel client/scope.

No user snapshots, tasks or runtime enrollment. A cancelled collection is simply
abandoned; the next waiter collects again. One collection at a time limits panel
load. Callers must bound admission/wait time (prepare_delivery does so) and replace
this instance on credential, endpoint or panel epoch changes. Mutating the bound
client's credentials in place is not supported.
"""

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Sequence
from contextlib import nullcontext
from datetime import UTC, datetime

from app.services.bs_traffic.evidence import (
    TopologyEvidence,
    TopologyReader,
    _aware,
    _selection,
    collect_topology_evidence,
)
from app.services.bs_traffic.topology import TopologyConflict


class TopologyEvidenceCache:
    def __init__(
        self,
        api: TopologyReader,
        *,
        panel_identity: str,
        capacity: int = 64,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        if type(capacity) is not int or capacity <= 0:
            raise ValueError('Positive cache capacity is required')
        if not isinstance(panel_identity, str) or not panel_identity.strip():
            raise ValueError('Panel identity is required')
        self._api = api
        self._scope = panel_identity
        self._capacity = capacity
        self._clock = clock
        self._lock = asyncio.Lock()
        self._entries: OrderedDict[tuple[tuple[str, ...], tuple[str, ...]], TopologyEvidence] = OrderedDict()

    async def get(
        self,
        api: TopologyReader,
        *,
        panel_identity: str,
        ordinary_squads: Sequence[str],
        bs_squads: Sequence[str],
        admission=None,
    ) -> TopologyEvidence:
        if api is not self._api or panel_identity != self._scope:
            raise ValueError('Topology cache belongs to another panel client/scope')
        if not isinstance(ordinary_squads, (list, tuple)) or not isinstance(bs_squads, (list, tuple)):
            raise TopologyConflict('invalid_squads')
        if len(ordinary_squads) + len(bs_squads) > 64:
            raise TopologyConflict('too_many_squads')
        key = (_selection(ordinary_squads), _selection(bs_squads))
        async with self._lock:
            now = _aware(self._clock())
            # Purge expired entries, including on a backwards wall-clock jump.
            for old_key, evidence in tuple(self._entries.items()):
                if not evidence.is_fresh(now, panel_identity=self._scope):
                    del self._entries[old_key]
            cached = self._entries.get(key)
            if cached is not None:
                self._entries.move_to_end(key)
                return cached
            # Admission is needed only for a cache miss; waiters never occupy
            # scarce provider slots while waiting for this cache lock.
            async with admission() if admission is not None else nullcontext():
                evidence = await collect_topology_evidence(
                    self._api,
                    panel_identity=self._scope,
                    ordinary_squads=key[0],
                    bs_squads=key[1],
                    clock=self._clock,
                )
            self._entries[key] = evidence
            while len(self._entries) > self._capacity:
                self._entries.popitem(last=False)
            return evidence
