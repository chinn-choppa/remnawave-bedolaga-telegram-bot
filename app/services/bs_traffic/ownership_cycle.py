"""Cycle-bound current pointer and immutable complete historical receipts.

Mutations require the shared owner/subscription/ledger locks. Never delete an old
receipt or infer a usage flush barrier from a successful panel GET.
"""

from copy import deepcopy
from datetime import date, datetime

from app.database.models import BsTrafficLedger, BsTrafficManagedTransfer, BsTrafficOwnershipCycle
from app.services.bs_traffic.ledger import LedgerConflict


def encode_row(row):
    return {
        column.name: (
            value.isoformat() if isinstance(value := getattr(row, column.name), (date, datetime)) else deepcopy(value)
        )
        for column in row.__table__.columns
    }


async def current_transfer(db, subscription_id):
    ledger = await db.get(BsTrafficLedger, subscription_id, populate_existing=True)
    receipt = await db.get(BsTrafficManagedTransfer, subscription_id, populate_existing=True)
    if ledger is None or receipt is None or ledger.ownership_cycle != receipt.ownership_cycle:
        return None
    return receipt


async def archive_forward(db, ledger):
    receipt = await current_transfer(db, ledger.subscription_id)
    if receipt is None:
        raise LedgerConflict('Current ownership receipt is missing')
    snapshot = encode_row(receipt)
    old = await db.get(BsTrafficOwnershipCycle, (ledger.subscription_id, ledger.ownership_cycle))
    if old is not None:
        if old.forward_receipt != snapshot:
            raise LedgerConflict('Ownership history cannot be rewritten')
        return old
    row = BsTrafficOwnershipCycle(
        subscription_id=ledger.subscription_id,
        ownership_cycle=ledger.ownership_cycle,
        forward_receipt=snapshot,
        state='split',
    )
    db.add(row)
    await db.flush()
    return row


async def begin_next_cycle(db, *, user_id, subscription_id, account_generation, allow_funded_pending_sync=False):
    """Caller must validate new genuine funding before invoking in its transaction.

    This helper grants nothing and never enables the panel. Old uncertain writes
    remain fenced; only a verified ordinary ownership return may start a cycle.
    """
    from sqlalchemy import func, select

    from app.database.models import BsTrafficPanelBinding, Subscription, TrafficAccountingState
    from app.services.bs_traffic.readback import _recovery_lock

    if type(allow_funded_pending_sync) is not bool:
        raise LedgerConflict('Explicit funding synchronization permission required')
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    sub = await db.get(Subscription, subscription_id, populate_existing=True)
    source = await db.get(TrafficAccountingState, subscription_id, populate_existing=True)
    cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, ledger.ownership_cycle))
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(BsTrafficPanelBinding.panel_scope == ledger.panel_scope, BsTrafficPanelBinding.retired_at.is_(None))
        .with_for_update(read=True)
    )
    if (
        cycle is None
        or cycle.state != 'managed'
        or cycle.managed_at is None
        or (cycle.reverse_attempt or {}).get('state') != 'verified'
        or sub.traffic_accounting_mode != 'managed'
        or source is None
        or source.phase not in ('ready', 'unlimited')
        or binding is None
        or (
            source.projected_revision != source.revision
            and not (allow_funded_pending_sync and source.pending_sync_fields)
        )
        or (source.pending_sync_fields and not allow_funded_pending_sync)
        or source.activation_requested
        or source.grace_excluded
    ):
        raise LedgerConflict('A verified ordinary ownership return is required for a new cycle')
    await archive_forward(db, ledger)
    ledger.ownership_cycle += 1
    ledger.ownership_started_at = await db.scalar(select(func.clock_timestamp()))
    ledger.revision += 1
    await db.flush()
    return ledger.ownership_cycle


async def renewal_floor(db, ledger):
    """Prior-cycle receipt IDs, frozen before the ordinary ownership return."""
    if ledger.ownership_cycle == 0:
        return 0
    previous = await db.get(BsTrafficOwnershipCycle, (ledger.subscription_id, ledger.ownership_cycle - 1))
    if previous is None or previous.state != 'managed' or previous.reverse_manifest is None:
        raise LedgerConflict('Previous-cycle renewal provenance is missing')
    floor = previous.reverse_manifest.get('max_renewal_id')
    if type(floor) is not int or floor < 0:
        raise LedgerConflict('Previous-cycle renewal boundary is missing')
    return floor
