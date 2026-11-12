"""Explicit read-only recovery with independently collected topology evidence.

The trusted caller must bind this immutable API client to the persisted panel
scope. This does not enroll accounts, renew expired authorization or retry PATCH.
A matching account readback does not establish live socket/traffic boundaries.
"""

import asyncio
from copy import deepcopy
from datetime import timedelta

from sqlalchemy import func, select

from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.delivery_store import SEND_WINDOW_SECONDS, _operation
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_pause import persist_panel_pause, read_panel_pause
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.preparation import delivery_guard
from app.services.bs_traffic.readback import _recovery_lock, recover_delivery
from app.services.bs_traffic.recovery_backoff import RecoveryBackoff, backoff_for


RECOVERY_TIMEOUT = 30


async def _recover(session_factory, api, target, operation_id, propagate_backoff):
    async with session_factory() as db, db.begin():
        state, _ = await _recovery_lock(db, target['user_id'], target['subscription_id'], target['account_generation'])
        row = await _operation(db, target['subscription_id'], operation_id)
        if row.state == 'verified':
            return 'already_verified'
        if row.state == 'not_sent':
            return 'not_sent'
        if row.state not in ('attempted', 'attention'):
            return 'not_attempted'
        now = await db.scalar(select(func.clock_timestamp()))
        if row.state == 'attempted' and row.attempted_at + timedelta(seconds=SEND_WINDOW_SECONDS) > now:
            return 'in_progress'
        scope, revision = state.panel_scope, state.revision
        policy = deepcopy(_read_policy(state.access_policy))
    remaining = await read_panel_pause(session_factory, scope)
    if remaining > 0:
        if propagate_backoff:
            raise RecoveryBackoff(remaining)
        return 'unconfirmed'
    try:
        async with panel_slot(session_factory, scope):
            evidence = await collect_topology_evidence(
                api,
                panel_identity=scope,
                ordinary_squads=policy['topology']['ordinary_squads'],
                bs_squads=policy['topology']['bs_squads'],
            )
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(session_factory, scope, pause.retry_after_seconds)
            if propagate_backoff:
                raise pause from None
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
    async with session_factory() as db, db.begin():
        try:
            state, _ = await _recovery_lock(
                db, target['user_id'], target['subscription_id'], target['account_generation']
            )
        except LedgerConflict:
            return 'obsolete'
        if state.panel_scope != scope or state.revision != revision or state.access_policy != policy:
            return 'obsolete'
    return await recover_delivery(
        session_factory,
        api,
        **target,
        operation_id=operation_id,
        current_guard_hash=delivery_guard(**target, evidence=evidence),
        topology_evidence=evidence,
        propagate_backoff=propagate_backoff,
    )


async def recover_from_panel(
    session_factory, api, *, user_id, subscription_id, account_generation, operation_id, propagate_backoff=False
):
    """Collect current topology and read back an existing attempt, never resend.

    Thirty seconds bounds SQL and HTTP cooperatively; cleanup still waits for the
    driver. The original attempt deadline remains authoritative, never extended.
    Wrong initial ownership raises LedgerConflict before any panel request.
    """
    deadline = asyncio.timeout(RECOVERY_TIMEOUT)
    try:
        async with deadline:
            return await _recover(
                session_factory,
                api,
                dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation),
                operation_id,
                propagate_backoff,
            )
    except TimeoutError:
        if not deadline.expired():
            raise
        return 'unconfirmed'
