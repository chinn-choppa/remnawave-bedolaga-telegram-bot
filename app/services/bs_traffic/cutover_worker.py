"""Bounded recovery of explicitly staged cutovers, not automatic enrollment.

The owning runtime schedules passes; durable intent states govern every write
across restarts.
This worker neither creates operations nor retries a PATCH with unknown outcome.
"""

import asyncio
import math
from collections import Counter
from dataclasses import dataclass

import structlog

from app.services.bs_traffic.cutover_coordinator import advance_cutover
from app.services.bs_traffic.cutover_scan import cutover_page, start_cutover_scan
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import BoundPanelReader


logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CutoverResult:
    operation_id: int
    status: str


async def cutover_pass(reader, stop, *, page_size=100):
    """Yield committed results with backpressure; keep at most one page in memory.

    Cancellation leaves already committed stages intact. No scan session remains open
    across a yield or coordinator call. Panel admission uses its own transaction. New/late operations wait for the next pass.
    A stopped pass need not persist its cursor because exact intents are replayed.
    """
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound cutover reader required')
    if type(page_size) is not int or not 1 <= page_size <= 500:
        raise ValueError('Cutover page size must be between 1 and 500')
    if stop.is_set():
        return
    await reader.assert_current()
    async with reader._factory() as db:
        window = await start_cutover_scan(db, panel_scope=reader.panel_scope)
    after_id = 0
    while not stop.is_set():
        async with reader._factory() as db:
            page = await cutover_page(db, window, after_id=after_id, limit=page_size)
        for candidate in page.items:
            if stop.is_set():
                return
            result = await advance_cutover(
                reader,
                user_id=candidate.user_id,
                subscription_id=candidate.subscription_id,
                account_generation=candidate.account_generation,
                operation_id=candidate.operation_id,
            )
            yield CutoverResult(candidate.operation_id, result)
        if page.next_after is None:
            return
        after_id = page.next_after


async def run_cutover_worker(reader, stop, *, interval_seconds=30, page_size=100):
    """Run finite passes separated by an interruptible delay, never a busy retry.

    Unexpected SQL/programming errors propagate to the lifecycle supervisor.
    The existing cross-process panel admission/backoff also applies to each step.
    """
    if (
        isinstance(interval_seconds, bool)
        or not isinstance(interval_seconds, (int, float))
        or not math.isfinite(interval_seconds)
        or not 5 <= interval_seconds <= 300
    ):
        raise ValueError('Cutover interval must be between 5 and 300 seconds')
    while not stop.is_set():
        statuses = Counter()
        async for item in cutover_pass(reader, stop, page_size=page_size):
            statuses[item.status] += 1
        if statuses:
            logger.info('Split cutover pass completed', outcomes=dict(statuses))
        if not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            except TimeoutError:
                pass
