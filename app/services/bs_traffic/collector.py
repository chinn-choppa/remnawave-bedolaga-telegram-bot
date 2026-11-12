"""One bounded observation pass, not a scheduler or a paid traffic allocator.

The caller supplies a verified panel epoch and account-generation map. This
module deliberately does not discover identities by email/Telegram ID, enable
split accounting, modify panel access, or charge subscriptions. A persisted
response means evidence was stored, not that a day is final or every user was
present. Retention and scheduling of unfinished polls are separate work.
"""

import asyncio
import math
from collections.abc import Callable
from datetime import date
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.bs_traffic.observations import begin_poll, record_poll


class DailyUsageReader(Protocol):
    async def get_daily_nodes_usage(
        self, node_uuids: tuple[str, ...], day: date, *, max_response_bytes: int, timeout_seconds: float
    ) -> dict[str, Any]: ...


async def collect_daily_usage(
    session_factory: Callable[[], AsyncSession],
    api: DailyUsageReader,
    *,
    panel_scope: str,
    day: date,
    node_uuids: tuple[str, ...],
    identities: dict[int, str],
    max_response_bytes: int = 8 * 1024**2,
    timeout_seconds: float = 15,
) -> int:
    """Commit request identity, fetch without SQL locks, atomically store evidence.

    A timeout/cancellation/crash leaves an unfinished poll for diagnostics. A
    later fetch MUST allocate a fresh poll; reusing request order after restart
    would incorrectly treat new evidence as an older response. The deadline
    covers fetching, including API throttle waits, not PostgreSQL transactions.
    """
    if type(max_response_bytes) is not int or not 1 <= max_response_bytes <= 8 * 1024**2:
        raise ValueError('Invalid usage response budget')
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 60
    ):
        raise ValueError('Invalid usage deadline')
    async with session_factory() as db, db.begin():
        poll = await begin_poll(db, panel_scope=panel_scope, day=day, node_uuids=node_uuids, identities=identities)
        poll_id = poll.id
        requested_nodes = tuple(poll.node_uuids)
        requested_day = poll.day
    # No ORM instance fields are accessed after commit/close (expire_on_commit).
    async with asyncio.timeout(timeout_seconds):
        payload = await api.get_daily_nodes_usage(
            requested_nodes,
            requested_day,
            max_response_bytes=max_response_bytes,
            timeout_seconds=timeout_seconds,
        )
    async with session_factory() as db, db.begin():
        await record_poll(db, poll_id, payload)
    return poll_id
