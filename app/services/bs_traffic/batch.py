"""Bounded opt-in preparation, without account writes or automatic enrollment.

The source must be a lazy, non-blocking iterable of explicit durable operation
identities. This is a per-batch admission limit, not a distributed scheduler or
rate limiter. Use the async context manager even when stopping iteration early.
Already committed deliveries survive cancellation. Reusing their keys avoids
duplicates, subject to fresh account/rights checks; it is not unconditional replay.
A per-item deadline covers SQL waits too. Cancellation cleanup is awaited, so
a non-cooperative driver still requires database-side timeouts and supervision.
"""

import asyncio
import math
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal

from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.preparation import PreparedDelivery, prepare_delivery


@dataclass(frozen=True)
class PreparationRequest:
    user_id: int
    subscription_id: int
    account_generation: str
    operation_key: str


@dataclass(frozen=True)
class PreparationResult:
    request: PreparationRequest
    status: Literal['prepared', 'conflict', 'timeout']
    prepared: PreparedDelivery | None = None


async def _prepare(session_factory, api, request, cache, timeout_seconds):
    deadline = asyncio.timeout(timeout_seconds)
    try:
        async with deadline:
            result = await prepare_delivery(
                session_factory,
                api,
                user_id=request.user_id,
                subscription_id=request.subscription_id,
                account_generation=request.account_generation,
                operation_key=request.operation_key,
                topology_cache=cache,
            )
    except LedgerConflict:
        # Ownership/generation/operation conflicts are not provider outages.
        # Leave other accounts running; never expose raw exceptions to operators.
        return PreparationResult(request, 'conflict')
    except TimeoutError:
        if not deadline.expired():
            raise
        return PreparationResult(request, 'timeout')
    return PreparationResult(request, 'prepared', result)


async def _results(session_factory, api, requests, cache, concurrency, timeout_seconds):
    tasks = set()
    source = iter(requests)
    exhausted = False
    try:
        while True:
            while not exhausted and len(tasks) < concurrency:
                try:
                    request = next(source)
                except StopIteration:
                    exhausted = True
                    break
                if not isinstance(request, PreparationRequest):
                    raise TypeError('Expected PreparationRequest')
                tasks.add(asyncio.create_task(_prepare(session_factory, api, request, cache, timeout_seconds)))
            if not tasks:
                return
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                tasks.remove(task)
                yield task.result()
    finally:
        for task in tasks:
            task.cancel()
        # Retrieve exceptions from completed siblings too; no orphaned tasks.
        await asyncio.gather(*tasks, return_exceptions=True)


@asynccontextmanager
async def prepare_batch(
    session_factory,
    api,
    requests: Iterable[PreparationRequest],
    *,
    panel_scope: str,
    concurrency: int = 8,
    timeout_seconds: float = 30,
) -> AsyncIterator[AsyncIterator[PreparationResult]]:
    """Stream outcomes in completion order; stop consuming the source on backpressure.

    One immutable API client/scope per batch. Each account still receives its own
    live snapshot and SQL guards. Expected conflicts yield a result; unexpected
    failures propagate after sibling cleanup. No retries or PATCHes are performed.
    """
    if type(concurrency) is not int or not 1 <= concurrency <= 32:
        raise ValueError('Preparation concurrency must be between 1 and 32')
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 300
    ):
        raise ValueError('Preparation deadline must be positive and at most 300 seconds')
    cache = TopologyEvidenceCache(api, panel_identity=panel_scope)
    results = _results(session_factory, api, requests, cache, concurrency, timeout_seconds)
    try:
        yield results
    finally:
        await results.aclose()
