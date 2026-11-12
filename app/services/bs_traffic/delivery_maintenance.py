"""Bounded cleanup of expired, never-attempted deliveries; caller commits.

No retry, HTTP, account mutation or runtime registration. Potential remote
side effects (attempted/attention) remain unresolved irrespective of age.
"""

from uuid import UUID

from sqlalchemy import func, select

from app.database.models import BsTrafficDelivery as Delivery, BsTrafficLedger


async def expire_pending(db, *, panel_scope: str, limit: int = 100) -> tuple[int, ...]:
    """Cancel at most limit expired pending rows, skipping another worker's locks.

    Locks only Delivery, never acquires owner/subscription/ledger locks after it.
    Call with a short independent transaction and caller-owned SQL timeout.
    The deadline comes from the database, not worker wall clocks.
    """
    if db.get_bind().dialect.name != 'postgresql':
        raise ValueError('Delivery maintenance requires PostgreSQL')
    if not isinstance(panel_scope, str):
        raise ValueError('Invalid panel scope')
    scope = str(UUID(panel_scope))
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Maintenance limit must be between 1 and 500')
    now = await db.scalar(select(func.clock_timestamp()))
    rows = (
        await db.scalars(
            select(Delivery)
            .join(BsTrafficLedger, BsTrafficLedger.subscription_id == Delivery.subscription_id)
            .where(
                BsTrafficLedger.panel_scope == scope,
                Delivery.state == 'pending',
                Delivery.valid_until <= now,
            )
            .order_by(Delivery.valid_until, Delivery.id)
            .limit(limit)
            .with_for_update(of=Delivery, skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).all()
    for row in rows:
        row.state = 'cancelled'
    await db.flush()
    return tuple(row.id for row in rows)
