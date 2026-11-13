"""Manual daily consent boundary: fresh context, durable payment, guarded delivery."""

import asyncio
from dataclasses import replace

import structlog
from sqlalchemy import func, select

from app.database.crud.transaction import emit_transaction_side_effects
from app.database.models import BsTrafficLedger, PaymentMethod, Transaction, TransactionType
from app.services.bs_traffic.daily_control import find_daily_resume, quote_daily_resume, resume_daily_from_balance
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.policy import _read_policy, _topology
from app.services.bs_traffic.quote_preparation import _with_split_context
from app.services.bs_traffic.renewal_grace import advance_paid_renewal


logger = structlog.get_logger(__name__)


async def _require_current_topology(db, reader, subscription_id, evidence):
    ledger = await db.get(BsTrafficLedger, subscription_id)
    policy = _read_policy(ledger.access_policy)
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        not isinstance(evidence, TopologyEvidence)
        or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
        or evidence.fingerprint != policy['fingerprint']
        or _topology(evidence.topology) != policy['topology']
    ):
        raise LedgerConflict('Daily resume topology changed; reconcile before payment')


async def prepare_daily_resume(reader, *, user_id, subscription_id):
    async def quote(db, *, evidence, current_squads, **target):
        await _require_current_topology(db, reader, subscription_id, evidence)
        return await quote_daily_resume(db, **target)

    return await _with_split_context(reader, user_id=user_id, subscription_id=subscription_id, apply=quote)


async def _emit_effects(reader, result, user_id):
    async with reader._factory() as db:
        transaction = await db.get(Transaction, result.transaction_id)
        await emit_transaction_side_effects(
            db,
            transaction,
            user_id=user_id,
            amount_kopeks=result.charged_kopeks,
            type=TransactionType.SUBSCRIPTION_PAYMENT,
            payment_method=PaymentMethod.BALANCE,
            external_id=transaction.external_id,
            description=transaction.description,
        )


async def checkout_daily_resume(
    reader, *, user_id, subscription_id, operation_key, expected_price_kopeks, expected_terms_hash
):
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound daily resume panel reader required')
    request = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        operation_key=operation_key,
        expected_price_kopeks=expected_price_kopeks,
        expected_terms_hash=expected_terms_hash,
    )

    async def lookup():
        async with reader._factory() as db:
            return await find_daily_resume(db, **request)

    historical = await lookup()
    if historical is not None:
        return historical

    async def purchase(db, *, evidence, current_squads, **target):
        await _require_current_topology(db, reader, subscription_id, evidence)
        result = await resume_daily_from_balance(
            db,
            **target,
            operation_key=operation_key,
            expected_price_kopeks=expected_price_kopeks,
            expected_terms_hash=expected_terms_hash,
        )
        return target['account_generation'], result

    try:
        generation, result = await _with_split_context(
            reader, user_id=user_id, subscription_id=subscription_id, apply=purchase
        )
    except Exception:
        historical = await lookup()
        if historical is not None:
            return historical
        raise
    if result.replayed:
        return result
    pending = True
    try:
        async with asyncio.timeout(10):
            outcome = await advance_paid_renewal(
                reader,
                user_id=user_id,
                subscription_id=subscription_id,
                account_generation=generation,
                operation_id=result.intent_id,
            )
        pending = outcome not in ('verified', 'already_verified', 'superseded')
    except Exception:
        logger.exception('Paid daily resume delivery deferred', transaction_id=result.transaction_id)
    try:
        async with asyncio.timeout(10):
            await _emit_effects(reader, result, user_id)
    except Exception:
        logger.exception('Paid daily resume effects failed', transaction_id=result.transaction_id)
    return replace(result, delivery_pending=pending)
