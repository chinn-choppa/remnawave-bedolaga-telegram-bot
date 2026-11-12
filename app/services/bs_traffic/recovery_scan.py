"""Read-only keyset candidates for a recovery pass, not a claim or authorization.

A window excludes new IDs and newly eligible attempts. It is not a repeatable
read snapshot: late commits behind the cursor are picked up on the next pass.
The caller owns cursor persistence, admission, SQL timeouts and trusted panel
client binding. Recovery must revalidate every candidate before network use.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, literal_column, select

from app.database.models import BsTrafficDelivery as Delivery, BsTrafficLedger as Ledger, Subscription
from app.services.bs_traffic.delivery_store import SEND_WINDOW_SECONDS


@dataclass(frozen=True)
class RecoveryWindow:
    panel_scope: str
    upper_id: int
    eligible_before: datetime


@dataclass(frozen=True)
class RecoveryCandidate:
    user_id: int
    subscription_id: int
    account_generation: str
    operation_id: int


@dataclass(frozen=True)
class RecoveryPage:
    items: tuple[RecoveryCandidate, ...]
    next_after: int | None


def _scope(value):
    if not isinstance(value, str):
        raise ValueError('Invalid recovery panel scope')
    return str(UUID(value))


def _postgres(db):
    if db.get_bind().dialect.name != 'postgresql':
        raise ValueError('Recovery scanning requires PostgreSQL')


async def start_recovery_scan(db, *, panel_scope: str) -> RecoveryWindow:
    _postgres(db)
    scope = _scope(panel_scope)
    now = await db.scalar(select(func.clock_timestamp()))
    upper = await db.scalar(
        select(func.coalesce(func.max(Delivery.id), 0))
        .join(Ledger, Ledger.subscription_id == Delivery.subscription_id)
        .where(Ledger.panel_scope == scope)
    )
    return RecoveryWindow(scope, upper, now - timedelta(seconds=SEND_WINDOW_SECONDS))


def _page_query(window, after_id, limit):
    return (
        select(Subscription.user_id, Delivery.subscription_id, Ledger.account_generation, Delivery.id)
        .select_from(Delivery)
        .join(Ledger, Ledger.subscription_id == Delivery.subscription_id)
        .join(Subscription, Subscription.id == Delivery.subscription_id)
        .where(
            Ledger.panel_scope == window.panel_scope,
            Delivery.id > after_id,
            Delivery.id <= window.upper_id,
            # Fixed states must remain SQL literals so generic prepared plans
            # can prove the partial-index predicate. No caller value is inlined.
            Delivery.state.in_((literal_column("'attempted'"), literal_column("'attention'"))),
            Delivery.attempted_at <= window.eligible_before,
        )
        .order_by(Delivery.id)
        .limit(limit + 1)
    )


async def recovery_page(db, window: RecoveryWindow, *, after_id: int = 0, limit: int = 100) -> RecoveryPage:
    """Return at most limit hints; next_after=None ends this pass, not all work.

    No locks, state changes, tokens or old guards. A repeated caller can receive
    the same candidates: this reader does not grant exclusive worker ownership.
    """
    _postgres(db)
    if not isinstance(window, RecoveryWindow) or _scope(window.panel_scope) != window.panel_scope:
        raise ValueError('Canonical recovery window required')
    if type(window.upper_id) is not int or window.upper_id < 0:
        raise ValueError('Invalid recovery highwater')
    if (
        not isinstance(window.eligible_before, datetime)
        or window.eligible_before.tzinfo is None
        or window.eligible_before.utcoffset() is None
    ):
        raise ValueError('Aware eligibility boundary required')
    if type(after_id) is not int or not 0 <= after_id <= window.upper_id:
        raise ValueError('Invalid recovery cursor')
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Recovery page limit must be between 1 and 500')
    rows = (await db.execute(_page_query(window, after_id, limit))).all()
    items = tuple(RecoveryCandidate(*row) for row in rows[:limit])
    next_after = items[-1].operation_id if len(rows) > limit else None
    return RecoveryPage(items, next_after)
