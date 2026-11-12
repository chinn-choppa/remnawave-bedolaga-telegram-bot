"""Bounded paid-BS horizon capture; explicit invocation only, no runtime hook.

Each subscription commits independently. The cursor is a scan position, not
proof of completion: deferred rows must be revisited on the next full pass.
This captures rights only; it never changes squads, grants or accounting mode.
All expiry writers must be fenced before enabling automatic orchestration.
"""

from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from app.database.models import BsTrafficLedger, Subscription
from app.services.bs_traffic.ledger import LedgerConflict, _lock
from app.services.bs_traffic.policy import capture_bs_drain_before_extension
from app.services.bs_traffic.recovery_scan import _postgres, _scope


@dataclass(frozen=True)
class DrainPageResult:
    scanned: int
    changed: int
    deferred: int
    upper_id: int
    next_after: int | None


async def process_drain_page(factory, *, panel_scope, after_id=0, upper_id=None, limit=100):
    """Capture at most one page; caller controls scheduling and pass restarts."""
    scope = _scope(panel_scope)
    if type(limit) is not int or not 1 <= limit <= 500 or type(after_id) is not int or after_id < 0:
        raise ValueError('Invalid BS drain scan bounds')
    if upper_id is not None and (type(upper_id) is not int or upper_id < after_id):
        raise ValueError('Invalid BS drain scan window')
    async with factory() as db:
        _postgres(db)
        await db.execute(select(func.set_config('statement_timeout', '5000', True)))
        if upper_id is None:
            upper_id = max(
                after_id,
                int(
                    await db.scalar(
                        select(func.coalesce(func.max(BsTrafficLedger.subscription_id), 0)).where(
                            BsTrafficLedger.panel_scope == scope
                        )
                    )
                ),
            )
        rows = (
            await db.execute(
                select(Subscription.id, Subscription.user_id, BsTrafficLedger.account_generation)
                .join(BsTrafficLedger, BsTrafficLedger.subscription_id == Subscription.id)
                .where(BsTrafficLedger.panel_scope == scope, Subscription.id > after_id, Subscription.id <= upper_id)
                .order_by(Subscription.id)
                .limit(limit)
            )
        ).all()
    changed = deferred = 0
    for row in rows:
        try:
            async with factory() as db, db.begin():
                # Transaction-local: a busy payment must not stall the page
                # or change timeout settings of the pooled connection.
                await db.execute(select(func.set_config('lock_timeout', '1000', True)))
                await db.execute(select(func.set_config('statement_timeout', '5000', True)))
                state, _ = await _lock(db, row.user_id, row.id, row.account_generation)
                if state.panel_scope != scope:
                    raise LedgerConflict('Panel scope changed during drain scan')
                sub = await db.get(Subscription, row.id)
                if sub.traffic_accounting_mode not in ('split', 'split_pending'):
                    continue
                revision = state.revision
                await capture_bs_drain_before_extension(db, sub)
                did_change = state.revision != revision
            # Count only committed transitions, not rolled back attempts.
            changed += int(did_change)
        except LedgerConflict:
            deferred += 1
        except DBAPIError as error:
            if getattr(error.orig, 'sqlstate', None) not in ('55P03', '57014'):
                raise
            deferred += 1
    next_after = rows[-1].id if rows and rows[-1].id < upper_id else None
    return DrainPageResult(len(rows), changed, deferred, upper_id, next_after)
