"""Bounded keyset passes over paid deliveries; explicit start, no main hook.

Only a fixed-size window of delivery rows enters receipt/successor lookups.
The high-water mark excludes successors created during this pass. Restarting
loses only the scan position: durable send claims and receipts remain intact.
"""

import asyncio
from dataclasses import dataclass

import structlog
from sqlalchemy import func, select, tuple_

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficDelivery,
    BsTrafficLedger,
    PaymentMethod,
    Subscription,
    Transaction,
    TransactionType,
)
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from app.services.bs_traffic.recovery_scan import RecoveryCandidate, _postgres, _scope


logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class PurchaseDeliveryPage:
    items: tuple[RecoveryCandidate, ...]
    upper_id: int
    next_after: int | None
    scanned: int


async def purchase_delivery_page(db, *, panel_scope, after_id=0, upper_id=None, limit=100):
    _postgres(db)
    scope = _scope(panel_scope)
    if type(limit) is not int or not 1 <= limit <= 500 or type(after_id) is not int or after_id < 0:
        raise ValueError('Invalid purchase delivery scan bounds')
    if upper_id is None:
        upper_id = int(await db.scalar(select(func.coalesce(func.max(BsTrafficDelivery.id), 0))))
    if type(upper_id) is not int or upper_id < after_id:
        raise ValueError('Invalid purchase delivery scan window')
    # Fetch a bounded page first; do not let a NOT EXISTS scan traverse an
    # unbounded history of cancelled ancestors searching for the next leaf.
    rows = (
        await db.execute(
            select(
                BsTrafficDelivery.id,
                BsTrafficDelivery.subscription_id,
                BsTrafficDelivery.operation_key,
                BsTrafficDelivery.state,
                Subscription.user_id,
                BsTrafficLedger.account_generation,
            )
            .join(BsTrafficLedger, BsTrafficLedger.subscription_id == BsTrafficDelivery.subscription_id)
            .join(Subscription, Subscription.id == BsTrafficDelivery.subscription_id)
            .where(
                BsTrafficLedger.panel_scope == scope, BsTrafficDelivery.id > after_id, BsTrafficDelivery.id <= upper_id
            )
            .order_by(BsTrafficDelivery.id)
            .limit(limit)
        )
    ).all()
    if not rows:
        return PurchaseDeliveryPage((), upper_id, None, 0)
    cursor = rows[-1].id if rows[-1].id < upper_id else None
    leaves = []
    for row in rows:
        key = row.operation_key.split(':retry:', 1)[0]
        digest = key.removeprefix('split-purchase:')
        if (
            row.state != 'verified'
            and key.startswith('split-purchase:')
            and len(digest) == 64
            and all(c in '0123456789abcdef' for c in digest)
        ):
            leaves.append((row, key))
    if not leaves:
        return PurchaseDeliveryPage((), upper_id, cursor, len(rows))
    # Separate exact-key batches prevent the planner from reordering a large
    # history self-join ahead of the page bound (measured with 50k ancestors).
    identity = tuple_(BsTrafficDelivery.subscription_id, BsTrafficDelivery.operation_key)
    roots = {
        (row.subscription_id, row.operation_key): row.id
        for row in (
            await db.execute(
                select(BsTrafficDelivery.id, BsTrafficDelivery.subscription_id, BsTrafficDelivery.operation_key).where(
                    identity.in_(list({(row.subscription_id, key) for row, key in leaves}))
                )
            )
        ).all()
    }
    successors = set(
        (
            await db.execute(
                select(BsTrafficDelivery.subscription_id, BsTrafficDelivery.operation_key).where(
                    identity.in_([(row.subscription_id, f'{key}:retry:{row.id}') for row, key in leaves])
                )
            )
        ).all()
    )
    eligible = [
        (row, key)
        for row, key in leaves
        if (row.subscription_id, key) in roots and (row.subscription_id, f'{key}:retry:{row.id}') not in successors
    ]
    if not eligible:
        return PurchaseDeliveryPage((), upper_id, cursor, len(rows))
    payments = set(
        (
            await db.execute(
                select(Transaction.user_id, Transaction.external_id).where(
                    Transaction.external_id.in_(list({key for _, key in eligible})),
                    Transaction.payment_method == PaymentMethod.BALANCE.value,
                    Transaction.type == TransactionType.SUBSCRIPTION_PAYMENT.value,
                    Transaction.is_completed.is_(True),
                    Transaction.amount_kopeks <= 0,
                )
            )
        ).all()
    )
    grants = set(
        (
            await db.execute(
                select(BsTrafficAllowance.subscription_id, BsTrafficAllowance.operation_key).where(
                    tuple_(BsTrafficAllowance.subscription_id, BsTrafficAllowance.operation_key).in_(
                        list({(row.subscription_id, key) for row, key in eligible})
                    ),
                )
            )
        ).all()
    )
    candidates = tuple(
        RecoveryCandidate(row.user_id, row.subscription_id, row.account_generation, roots[(row.subscription_id, key)])
        for row, key in eligible
        if (row.user_id, key) in payments and (row.subscription_id, key) in grants
    )
    return PurchaseDeliveryPage(candidates, upper_id, cursor, len(rows))


async def run_purchase_delivery_worker(reader, stop: asyncio.Event, *, page_size=100):
    """One sequential worker per bound panel; shared admission still bounds HTTP.

    No automatic scope registration/enrollment. Stop is checked between jobs;
    an in-flight job finishes its bounded step so its durable claim is retained.
    """
    if not isinstance(reader, BoundPanelReader) or type(page_size) is not int or not 1 <= page_size <= 500:
        raise ValueError('Bound panel reader and bounded page size required')
    after, upper = 0, None
    while not stop.is_set():
        try:
            await reader.assert_current()
            async with reader._factory() as db:
                page = await purchase_delivery_page(
                    db, panel_scope=reader.panel_scope, after_id=after, upper_id=upper, limit=page_size
                )
            for candidate in page.items:
                if stop.is_set():
                    return
                await advance_purchase_delivery(
                    reader,
                    user_id=candidate.user_id,
                    subscription_id=candidate.subscription_id,
                    account_generation=candidate.account_generation,
                    operation_id=candidate.operation_id,
                )
                # A fast attention response must not turn recovery into a hot
                # API polling loop. Empty history pages need no such delay.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except TimeoutError:
                    pass
            if page.next_after is not None:
                after, upper = page.next_after, page.upper_id
                await asyncio.sleep(0)
                continue
            after, upper = 0, None
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('Split purchase delivery pass failed')
            after, upper = 0, None
        try:
            await asyncio.wait_for(stop.wait(), timeout=30)
        except TimeoutError:
            pass
