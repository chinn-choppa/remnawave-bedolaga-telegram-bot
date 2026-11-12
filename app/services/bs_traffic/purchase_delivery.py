"""Bounded recovery of a paid delivery; no new debit or allowance.

Keep the original receipt and every attempt. Fresh successors are permitted
only for work known not to have been sent. Unknown writes receive GET recovery.
The owning runtime resumes the same receipt after restart.
"""

import asyncio

from sqlalchemy import or_, select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficDelivery,
    PaymentMethod,
    Subscription,
    Tariff,
    Transaction,
    TransactionType,
)
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.ledger import LedgerConflict, require_activated_owner
from app.services.bs_traffic.panel_admission import PanelBusy
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.preparation import prepare_delivery
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.bs_traffic.recovery_preflight import recover_from_panel
from app.services.bs_traffic.sender import send_delivery


async def _receipt_attempt(db, reader, target, operation_id):
    state, _ = await _recovery_lock(db, target['user_id'], target['subscription_id'], target['account_generation'])
    if state.panel_scope != reader.panel_scope:
        raise LedgerConflict('Wrong purchase panel scope')
    await require_activated_owner(db, target['subscription_id'])
    tariff = await db.scalar(
        select(Tariff)
        .join(Subscription, Subscription.tariff_id == Tariff.id)
        .where(Subscription.id == target['subscription_id'])
        .execution_options(populate_existing=True)
    )
    if tariff is None or tariff.id != _read_policy(state.access_policy)['tariff_id']:
        raise LedgerConflict('Subscription tariff changed; reconcile paid entitlements')
    root = await db.scalar(
        select(BsTrafficDelivery).where(
            BsTrafficDelivery.id == operation_id,
            BsTrafficDelivery.subscription_id == target['subscription_id'],
        )
    )
    if root is None:
        raise LedgerConflict('Purchase delivery not found')
    digest = root.operation_key.removeprefix('split-purchase:')
    if (
        not root.operation_key.startswith('split-purchase:')
        or len(digest) != 64
        or any(c not in '0123456789abcdef' for c in digest)
    ):
        raise LedgerConflict('Original purchase receipt required')
    payment = await db.scalar(
        select(Transaction.id).where(
            Transaction.external_id == root.operation_key,
            Transaction.payment_method == PaymentMethod.BALANCE.value,
            Transaction.user_id == target['user_id'],
            Transaction.type == TransactionType.SUBSCRIPTION_PAYMENT.value,
            Transaction.is_completed.is_(True),
            Transaction.amount_kopeks <= 0,
        )
    )
    grant = await db.scalar(
        select(BsTrafficAllowance.id).where(
            BsTrafficAllowance.subscription_id == target['subscription_id'],
            BsTrafficAllowance.operation_key == root.operation_key,
        )
    )
    if payment is None or grant is None:
        raise LedgerConflict('Incomplete paid receipt')
    # Safe LIKE prefix: the root was validated as a constant prefix + hex.
    latest = await db.scalar(
        select(BsTrafficDelivery)
        .where(
            BsTrafficDelivery.subscription_id == target['subscription_id'],
            or_(BsTrafficDelivery.id == root.id, BsTrafficDelivery.operation_key.like(root.operation_key + ':retry:%')),
        )
        .order_by(BsTrafficDelivery.id.desc())
        .limit(1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return root.operation_key, latest


async def advance_purchase_delivery(reader, *, user_id, subscription_id, account_generation, operation_id):
    """Resume by original receipt; commit each decision before any panel I/O.

    A successor key is deterministic from root and prior attempt, so competing
    workers converge on the same queued operation and its single-send claim.
    ``verified`` confirms panel access, not termination of existing sockets.
    """
    if not isinstance(reader, BoundPanelReader) or type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Bound panel reader and purchase receipt required')
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    try:
        async with asyncio.timeout(60):
            await reader.assert_current()
            async with reader._factory() as db, db.begin():
                root_key, row = await _receipt_attempt(db, reader, target, operation_id)
                status, latest_id = row.state, row.id
                if status == 'verified':
                    return 'already_verified'
                if status == 'pending':
                    row.state = 'cancelled'  # No send token exists; preserve history.
                elif status not in ('attempted', 'attention', 'cancelled', 'not_sent'):
                    return 'blocked'
            if status in ('attempted', 'attention'):
                return await recover_from_panel(reader._factory, reader, **target, operation_id=latest_id)
            prepared = await prepare_delivery(
                reader._factory, reader, **target, operation_key=f'{root_key}:retry:{latest_id}'
            )
            if prepared.status != 'queued':
                return prepared.status
            return await send_delivery(
                reader._factory,
                reader._api,
                **target,
                operation_id=prepared.operation_id,
                evidence=prepared.evidence,
                current_guard_hash=prepared.guard_hash,
                bound_reader=reader,
            )
    except PanelBusy:
        return 'deferred'
    except LedgerConflict:
        return 'obsolete'
    except RemnaWaveAPIError as error:
        pause = backoff_for(error)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unavailable'
    except TimeoutError:
        return 'unconfirmed'
