"""Shared split-lane pause in the existing collector budget, not a new lease.

Collector slot ownership is never changed. Record under Budget row lock before
any Schedule lock; reads take no row locks. Callers own transactions/timeouts.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.database.models import BsTrafficCollectionBudget as Budget
from app.services.bs_traffic.recovery_backoff import valid_retry_delay
from app.services.bs_traffic.recovery_scan import _postgres, _scope


async def remaining_pause(db, *, panel_scope) -> float:
    _postgres(db)
    scope = _scope(panel_scope)
    seconds = await db.scalar(
        select(func.greatest(func.extract('epoch', Budget.cooldown_until - func.clock_timestamp()), 0)).where(
            Budget.panel_scope == scope
        )
    )
    return float(seconds or 0)


async def record_pause(db, *, panel_scope, seconds) -> None:
    _postgres(db)
    scope = _scope(panel_scope)
    if not valid_retry_delay(seconds):
        raise ValueError('Invalid shared panel pause')
    await db.execute(insert(Budget).values(panel_scope=scope).on_conflict_do_nothing())
    row = await db.scalar(
        select(Budget).where(Budget.panel_scope == scope).with_for_update().execution_options(populate_existing=True)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    try:
        until = now + timedelta(seconds=seconds)
    except OverflowError:
        until = datetime.max.replace(tzinfo=UTC)
    if row.cooldown_until is None or row.cooldown_until < until:
        row.cooldown_until = until
    await db.flush()


async def read_panel_pause(session_factory, scope) -> float:
    async with session_factory() as db:
        return await remaining_pause(db, panel_scope=scope)


async def persist_panel_pause(session_factory, scope, seconds) -> None:
    async with session_factory() as db, db.begin():
        await record_pause(db, panel_scope=scope, seconds=seconds)
