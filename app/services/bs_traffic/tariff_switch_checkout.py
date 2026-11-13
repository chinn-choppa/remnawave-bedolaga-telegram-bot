"""Shared switch boundary: fresh evidence, atomic payment, recoverable delivery."""

import asyncio
from dataclasses import replace

import structlog

from app.database.crud.transaction import emit_transaction_side_effects
from app.database.models import PaymentMethod, Transaction, TransactionType
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.mode_collection import collect_mode_baselines
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.quote_preparation import _with_split_context
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.bs_traffic.renewal_grace import advance_paid_renewal
from app.services.bs_traffic.tariff_switch import (
    SwitchQuoteChanged,
    find_tariff_switch,
    quote_tariff_switch,
    switch_tariff_from_balance,
)


logger = structlog.get_logger(__name__)


async def prepare_tariff_switch(reader, *, user_id, subscription_id, new_tariff_id):
    async def quote(db, *, current_squads, **context):
        return await quote_tariff_switch(db, **context, new_tariff_id=new_tariff_id, current_squads=current_squads)

    result = await _with_split_context(
        reader, user_id=user_id, subscription_id=subscription_id, apply=quote, target_tariff_id=new_tariff_id
    )
    if result.external_squad_uuid:
        try:
            async with asyncio.timeout(15), panel_slot(reader._factory, reader.panel_scope):
                if await reader.split_external_squad_exists(result.external_squad_uuid) is not True:
                    raise LedgerConflict('External squad is unavailable')
        except RemnaWaveAPIError as error:
            pause = backoff_for(error)
            if pause is not None:
                await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
            raise
    return result


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


async def checkout_tariff_switch(
    reader, *, user_id, subscription_id, new_tariff_id, expected_price_kopeks, expected_terms_hash, operation_key
):
    """Persist money and delivery atomically; replay never charges again.

    Transaction notifications/contest hooks follow the existing checkout's
    best-effort contract. They are not a durable outbox and cannot safely be
    retried blindly after cancellation or a partially completed hook sequence.
    """
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound panel reader required')
    request = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        new_tariff_id=new_tariff_id,
        expected_price_kopeks=expected_price_kopeks,
        expected_terms_hash=expected_terms_hash,
        operation_key=operation_key,
    )

    async def lookup():
        async with reader._factory() as db:
            return await find_tariff_switch(db, **request)

    historical = await lookup()
    if historical is not None:
        return historical
    collection_job_ids = ()

    async def purchase(db, *, current_squads, **context):
        result = await switch_tariff_from_balance(
            db,
            **context,
            new_tariff_id=new_tariff_id,
            expected_price_kopeks=expected_price_kopeks,
            expected_terms_hash=expected_terms_hash,
            operation_key=operation_key,
            collection_job_ids=collection_job_ids,
            current_squads=current_squads,
        )
        return context['account_generation'], result

    try:
        # The external target is checked before money moves. Its UUID is part of
        # the terms hash, which the locked debit rechecks after this network I/O.
        quote = await prepare_tariff_switch(
            reader, user_id=user_id, subscription_id=subscription_id, new_tariff_id=new_tariff_id
        )
        if quote.terms_hash != expected_terms_hash or quote.price_kopeks != expected_price_kopeks:
            raise SwitchQuoteChanged('Tariff switch terms changed')
        collection_job_ids = await collect_mode_baselines(
            reader,
            user_id=user_id,
            subscription_id=subscription_id,
            new_modes={'ordinary': quote.ordinary_mode, 'bs': quote.bs_mode},
            new_tariff_id=new_tariff_id,
        )
        generation, result = await _with_split_context(
            reader, user_id=user_id, subscription_id=subscription_id, apply=purchase, target_tariff_id=new_tariff_id
        )
    except Exception:
        # A concurrent identical purchase or a lost commit response is not a
        # failed payment. The durable, exactly matching receipt wins.
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
        logger.exception('Paid split tariff switch delivery deferred', transaction_id=result.transaction_id)
    try:
        async with asyncio.timeout(10):
            await _emit_effects(reader, result, user_id)
    except Exception:
        logger.exception('Paid split tariff switch side effects failed', transaction_id=result.transaction_id)
    return replace(result, delivery_pending=pending)
