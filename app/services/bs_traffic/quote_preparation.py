"""Trusted fresh split purchase quotes; no client-provided panel identity.

Public adapters supply an already bound panel reader.
Database locks end before network I/O; final locked pricing rechecks the snapshot.
"""

import asyncio
from copy import deepcopy

from sqlalchemy import func, select

from app.database.models import BsTrafficLedger, Subscription, Tariff
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.delivery_store import _local_guard
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, _group, require_activated_owner
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.purchase import quote_package
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for


async def prepare_package_quote(reader, *, user_id, subscription_id, group, gb):
    """Return display terms, not authority to debit or a persistent reservation."""
    return await _with_purchase_context(
        reader, user_id=user_id, subscription_id=subscription_id, group=group, gb=gb, apply=quote_package
    )


async def _with_purchase_context(reader, *, user_id, subscription_id, group, gb, apply):
    """Internal transaction boundary for quote or purchase; never a client callback."""
    _group(group)
    if type(gb) is not int or not 1 <= gb <= 100_000:
        raise LedgerConflict('Invalid traffic package')

    async def apply_package(db, **context):
        return await apply(db, **context, group=group, gb=gb)

    return await _with_split_context(reader, user_id=user_id, subscription_id=subscription_id, apply=apply_package)


async def _with_split_context(reader, *, user_id, subscription_id, apply, target_tariff_id=None):
    """Fresh account/topology evidence outside locks, followed by locked revalidation."""
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound panel reader required')
    if any(type(value) is not int or value <= 0 for value in (user_id, subscription_id)):
        raise LedgerConflict('Invalid purchase target')
    try:
        async with asyncio.timeout(15):
            await reader.assert_current()
            async with reader._factory() as db, db.begin():
                generation = await db.scalar(
                    select(BsTrafficLedger.account_generation)
                    .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
                    .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
                )
                if generation is None:
                    raise LedgerConflict('Owned split subscription required')
                target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=generation)
                state, panel_id = await _recovery_lock(
                    db, target['user_id'], target['subscription_id'], target['account_generation']
                )
                await require_activated_owner(db, subscription_id)
                if state.panel_scope != reader.panel_scope:
                    raise LedgerConflict('Wrong panel scope')
                policy, revision = deepcopy(_read_policy(state.access_policy)), state.revision
                now = await db.scalar(select(func.clock_timestamp()))
                guard = await _local_guard(db, user_id, subscription_id, now)
                target_squads = None
                if target_tariff_id is not None:
                    from app.services.bs_traffic.tariff_settings import BsTrafficSettings

                    tariff = await db.get(Tariff, target_tariff_id)
                    if tariff is None:
                        raise LedgerConflict('Target tariff missing')
                    bs = BsTrafficSettings.model_validate(tariff.bs_traffic_settings)
                    candidate = (tuple(sorted(tariff.allowed_squads or [])), tuple(bs.squad_uuids))
                    if candidate != (
                        tuple(policy['topology']['ordinary_squads']),
                        tuple(policy['topology']['bs_squads']),
                    ):
                        target_squads = candidate
            async with panel_slot(reader._factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
            target_evidence = None
            if target_squads is not None:
                async with panel_slot(reader._factory, reader.panel_scope):
                    target_evidence = await collect_topology_evidence(
                        reader,
                        panel_identity=reader.panel_scope,
                        ordinary_squads=target_squads[0],
                        bs_squads=target_squads[1],
                    )
            async with panel_slot(reader._factory, reader.panel_scope):
                account = await reader.get_split_access_snapshot(panel_id)
            if account is None or generation_of(panel_scope=reader.panel_scope, account=account) != generation:
                raise LedgerConflict('Panel account changed')
            squads = _observed_squads(account)
            await reader.assert_current()
            async with reader._factory() as db, db.begin():
                state, _ = await _recovery_lock(
                    db, target['user_id'], target['subscription_id'], target['account_generation']
                )
                now = await db.scalar(select(func.clock_timestamp()))
                if (
                    state.panel_scope != reader.panel_scope
                    or state.revision != revision
                    or state.access_policy != policy
                    or guard != await _local_guard(db, user_id, subscription_id, now)
                ):
                    raise LedgerConflict('Purchase snapshot changed; refresh the quote')
                extra = {'target_evidence': target_evidence} if target_tariff_id is not None else {}
                return await apply(db, **target, evidence=evidence, current_squads=squads, **extra)
    except RemnaWaveAPIError as error:
        pause = backoff_for(error)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        raise
