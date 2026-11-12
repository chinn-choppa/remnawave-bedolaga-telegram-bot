"""Shared balance checkout for cabinet and bot adapters.

Money commits before delivery/notifications. A post-commit failure is pending
fulfillment, never a failed charge. Existing persisted receipts survive retries.
Enrollment and runtime scheduling remain the responsibility of their coordinators.
"""

import asyncio
from dataclasses import dataclass

import structlog
from sqlalchemy import or_, select

from app.database.crud.transaction import emit_transaction_side_effects
from app.database.models import (
    BsTrafficDelivery,
    BsTrafficLedger,
    PaymentMethod,
    Subscription,
    Transaction,
    TransactionType,
)
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.purchase import PurchaseResult, find_purchase_receipt, purchase_package
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from app.services.bs_traffic.quote_preparation import _with_purchase_context


logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CheckoutResult:
    purchase: PurchaseResult
    delivery_status: str


async def _emit_effects(reader, purchase, user_id):
    async with reader._factory() as db:
        transaction = await db.get(Transaction, purchase.transaction_id)
        await emit_transaction_side_effects(
            db,
            transaction,
            amount_kopeks=purchase.price_kopeks,
            user_id=user_id,
            type=TransactionType.SUBSCRIPTION_PAYMENT,
            payment_method=PaymentMethod.BALANCE,
            external_id=transaction.external_id,
            description=transaction.description,
        )


async def _lookup_receipt(reader, *, user_id, subscription_id, group, gb, expected_price_kopeks, operation_key):
    async with reader._factory() as db, db.begin():
        generation = await db.scalar(
            select(BsTrafficLedger.account_generation)
            .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
            .where(
                Subscription.id == subscription_id,
                Subscription.user_id == user_id,
                BsTrafficLedger.panel_scope == reader.panel_scope,
            )
        )
        if generation is None:
            raise LedgerConflict('Owned split subscription required')
        purchase = await find_purchase_receipt(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=generation,
            group=group,
            gb=gb,
            expected_price_kopeks=expected_price_kopeks,
            operation_key=operation_key,
        )
        if purchase is not None:
            root = await db.get(BsTrafficDelivery, purchase.delivery_id)
            delivery_state = await db.scalar(
                select(BsTrafficDelivery.state)
                .where(
                    BsTrafficDelivery.subscription_id == subscription_id,
                    or_(
                        BsTrafficDelivery.id == root.id,
                        BsTrafficDelivery.operation_key.like(root.operation_key + ':retry:%'),
                    ),
                )
                .order_by(BsTrafficDelivery.id.desc())
                .limit(1)
            )
            # Recovery runs independently; replay must not depend on panel uptime.
            return generation, CheckoutResult(purchase, 'verified' if delivery_state == 'verified' else 'pending')

    return generation, None


async def checkout_package(reader, *, user_id, subscription_id, group, gb, expected_price_kopeks, operation_key):
    """Commit one purchase, then attempt bounded delivery; replay works offline.

    Postcommit hooks are best effort (as in the existing checkout). They are
    not replayed and are not claimed as an exactly-once durable outbox.
    """
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound panel reader required')
    if any(type(value) is not int or value <= 0 for value in (user_id, subscription_id)):
        raise LedgerConflict('Invalid purchase target')
    receipt_args = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        group=group,
        gb=gb,
        expected_price_kopeks=expected_price_kopeks,
        operation_key=operation_key,
    )
    generation, receipt = await _lookup_receipt(reader, **receipt_args)
    if receipt is not None:
        return receipt

    async def apply(db, **prepared):
        if prepared['account_generation'] != generation:
            raise LedgerConflict('Panel account changed; refresh the quote')
        return await purchase_package(
            db, **prepared, expected_price_kopeks=expected_price_kopeks, operation_key=operation_key
        )

    try:
        purchase = await _with_purchase_context(
            reader, user_id=user_id, subscription_id=subscription_id, group=group, gb=gb, apply=apply
        )
    except Exception:
        # A concurrent identical request may have committed while our snapshot aged.
        # Only a complete receipt with identical terms permits success here.
        _, receipt = await _lookup_receipt(reader, **receipt_args)
        if receipt is not None:
            return receipt
        raise
    delivery_status = 'pending'
    try:
        async with asyncio.timeout(10):
            outcome = await advance_purchase_delivery(
                reader,
                user_id=user_id,
                subscription_id=subscription_id,
                account_generation=generation,
                operation_id=purchase.delivery_id,
            )
        if outcome in ('verified', 'already_verified'):
            delivery_status = 'verified'
    except Exception:
        logger.exception('Paid split purchase delivery deferred', transaction_id=purchase.transaction_id)
    if not purchase.replayed:
        try:
            async with asyncio.timeout(10):
                await _emit_effects(reader, purchase, user_id)
        except Exception:
            logger.exception('Paid split purchase side effects failed', transaction_id=purchase.transaction_id)
    return CheckoutResult(purchase, delivery_status)
