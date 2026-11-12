"""Bounded keyset scan of durable cutovers; candidates are not authorization.

A fixed highwater prevents new operations from prolonging a pass indefinitely.
A late commit behind the cursor is picked up by the next pass. Every candidate
must go through advance_cutover, which rechecks ownership and current state.
"""

from dataclasses import dataclass

from sqlalchemy import func, literal_column, select

from app.database.models import BsTrafficCutoverIntent as Intent, BsTrafficLedger as Ledger, Subscription
from app.services.bs_traffic.recovery_scan import RecoveryCandidate, RecoveryPage, _postgres, _scope


@dataclass(frozen=True)
class CutoverWindow:
    panel_scope: str
    upper_id: int


def _active():
    # Fixed SQL literals allow generic prepared plans to use the partial index.
    return Intent.state.in_((literal_column("'pending'"), literal_column("'attempted'"), literal_column("'verified'")))


async def start_cutover_scan(db, *, panel_scope):
    _postgres(db)
    scope = _scope(panel_scope)
    upper = await db.scalar(select(func.coalesce(func.max(Intent.id), 0)).where(Intent.panel_scope == scope, _active()))
    return CutoverWindow(scope, upper)


async def cutover_page(db, window, *, after_id=0, limit=100):
    """Read only scalar identities, never full payloads or every subscription.

    No row locks or network calls. A repeated scan can return the same operation;
    exclusive permission to PATCH is acquired by the existing durable claim.
    """
    _postgres(db)
    if not isinstance(window, CutoverWindow) or _scope(window.panel_scope) != window.panel_scope:
        raise ValueError('Canonical cutover window required')
    if type(window.upper_id) is not int or window.upper_id < 0:
        raise ValueError('Invalid cutover highwater')
    if type(after_id) is not int or not 0 <= after_id <= window.upper_id:
        raise ValueError('Invalid cutover cursor')
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Cutover page limit must be between 1 and 500')
    rows = (
        await db.execute(
            select(Subscription.user_id, Intent.subscription_id, Intent.account_generation, Intent.id)
            .select_from(Intent)
            .join(Ledger, Ledger.subscription_id == Intent.subscription_id)
            .join(Subscription, Subscription.id == Intent.subscription_id)
            .where(
                Intent.panel_scope == window.panel_scope,
                Ledger.panel_scope == window.panel_scope,
                Ledger.account_generation == Intent.account_generation,
                Intent.id > after_id,
                Intent.id <= window.upper_id,
                _active(),
            )
            .order_by(Intent.id)
            .limit(limit + 1)
        )
    ).all()
    items = tuple(RecoveryCandidate(*row) for row in rows[:limit])
    return RecoveryPage(items, items[-1].operation_id if len(rows) > limit else None)
