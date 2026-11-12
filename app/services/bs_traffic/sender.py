"""Explicit one-operation split sender used by delivery coordinators.

The attempt commits before HTTP. A second transaction holds the existing
User/grace/Subscription locks during bounded preflight and PATCH, preventing
local payments/grace from racing that write. This does not fence remote panel
administrators or a delayed upstream application after a timeout. Unresolved
attempts are read back, never resent. Callers must supply current bound
topology evidence; this primitive does not establish subscription ownership.
"""

import asyncio

from sqlalchemy import exists, func, select

from app.database.models import BsTrafficAllowance, BsTrafficPanelBinding, GraceAccessSessionModel, Subscription
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.base_extension import extension_boundary
from app.services.bs_traffic.delivery_plan import plan_squads
from app.services.bs_traffic.delivery_store import SEND_WINDOW_SECONDS, _local_guard, _operation, claim
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_pause import persist_panel_pause, remaining_pause
from app.services.bs_traffic.policy import _fingerprint, project_access
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock, recover_delivery
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.panel_sync.writer import patch_split_internal_squads


def _not_sent(row, reason):
    # Only used by the token owner under its row lock, BEFORE calling PATCH.
    # This must never be used for transport errors or post-write readback.
    row.state, row.not_sent_reason = 'not_sent', reason
    return 'not_sent'


async def send_delivery(
    session_factory,
    api,
    *,
    user_id,
    subscription_id,
    account_generation,
    operation_id,
    evidence,
    current_guard_hash,
    bound_reader=None,
):
    """Send at most once, then confirm using a separate strict GET.

    A timeout/cancellation after claim never makes the operation pending again.
    The caller supplies fresh verified topology evidence and the matching
    external guard; this API is internal, not a client-controlled endpoint.
    """
    if not isinstance(evidence, TopologyEvidence) or not _fingerprint(current_guard_hash):
        raise LedgerConflict('Verified external delivery evidence required')
    if bound_reader is not None:
        from app.services.bs_traffic.panel_binding import BoundPanelReader

        if not isinstance(bound_reader, BoundPanelReader) or bound_reader._api is not api:
            raise LedgerConflict('Delivery client does not match bound panel reader')
        await bound_reader.assert_current()
    read_api = bound_reader if bound_reader is not None else api
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    recovery = dict(**target, operation_id=operation_id, current_guard_hash=current_guard_hash)
    async with session_factory() as db, db.begin():
        state, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
        if state.restriction_attempt and state.restriction_attempt.get('state') == 'attempted':
            return 'deferred'
        scope = state.panel_scope
        if bound_reader is not None and bound_reader.panel_scope != scope:
            raise LedgerConflict('Delivery belongs to another panel')
        row = await _operation(db, subscription_id, operation_id)
        if row.state == 'pending' and await remaining_pause(db, panel_scope=scope) > 0:
            return 'deferred'
        attempt = await claim(db, **recovery)
        if attempt is None:
            row = await _operation(db, subscription_id, operation_id)
            if row.state == 'attempted':
                return 'in_progress'
    if attempt is None:
        return await recover_delivery(session_factory, read_api, **recovery)

    try:
        async with asyncio.timeout(SEND_WINDOW_SECONDS):
            async with session_factory() as db, db.begin():
                state, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
                if state.restriction_attempt and state.restriction_attempt.get('state') == 'attempted':
                    return 'deferred'
                row = await _operation(db, subscription_id, operation_id)
                if row.state != 'attempted' or row.token != attempt.token:
                    return 'already_verified' if row.state == 'verified' else 'attention'
                now = await db.scalar(select(func.clock_timestamp()))
                if (
                    attempt.send_deadline <= now
                    or state.revision != row.revision
                    or row.guard_hash != current_guard_hash
                    or row.valid_until <= now
                    or not evidence.is_fresh(now, panel_identity=state.panel_scope)
                    or row.local_guard_hash != await _local_guard(db, user_id, subscription_id, now)
                ):
                    return _not_sent(row, 'stale_evidence')
                if await remaining_pause(db, panel_scope=scope) > 0:
                    return _not_sent(row, 'provider_backoff')
                try:
                    async with panel_slot(session_factory, scope):
                        account = await read_api.get_split_access_snapshot(panel_id)
                        if (
                            account is None
                            or generation_of(panel_scope=state.panel_scope, account=account) != account_generation
                        ):
                            return _not_sent(row, 'account_changed')
                        current_squads = _observed_squads(account)
                        grace = await db.scalar(
                            select(
                                exists().where(
                                    GraceAccessSessionModel.subscription_id == subscription_id,
                                    GraceAccessSessionModel.state != 'completed',
                                )
                            )
                        )
                        grace = bool(
                            grace
                            or await db.scalar(
                                select(Subscription.grace_session_open).where(Subscription.id == subscription_id)
                            )
                        )
                        planned_at = await db.scalar(select(func.clock_timestamp()))
                        boundaries = []
                        for column in (
                            BsTrafficAllowance.starts_at,
                            BsTrafficAllowance.expires_at,
                            BsTrafficAllowance.retired_at,
                        ):
                            boundary = await db.scalar(
                                select(func.min(column)).where(
                                    BsTrafficAllowance.subscription_id == subscription_id, column > planned_at
                                )
                            )
                            if boundary is not None:
                                boundaries.append(boundary)
                        extended_boundary = await extension_boundary(db, subscription_id, planned_at)
                        if extended_boundary is not None:
                            boundaries.append(extended_boundary)
                        projection = await project_access(db, **target, evidence=evidence, grace_active=grace)
                        # Whole-account expiry/admin/grace has a different coordinator.
                        if any(
                            group.state not in ('active', 'exhausted', 'disabled', 'pending')
                            for group in (projection.ordinary, projection.bs)
                        ):
                            return _not_sent(row, 'policy_changed')
                        plan = plan_squads(projection, topology=evidence.topology, current_squads=current_squads)
                        now = await db.scalar(select(func.clock_timestamp()))
                        if (
                            attempt.send_deadline <= now
                            or (boundaries and min(boundaries) <= now)
                            or row.local_guard_hash != await _local_guard(db, user_id, subscription_id, now)
                            or plan.action == 'wait'
                            or plan.squads != tuple(row.squads)
                            or row.valid_until <= now
                            or not evidence.is_fresh(now, panel_identity=state.panel_scope)
                        ):
                            return _not_sent(row, 'selection_changed')
                        if await remaining_pause(db, panel_scope=scope) > 0:
                            return _not_sent(row, 'provider_backoff')
                        if bound_reader is not None:
                            binding = await db.scalar(
                                select(BsTrafficPanelBinding.panel_scope)
                                .where(
                                    BsTrafficPanelBinding.panel_scope == bound_reader.panel_scope,
                                    BsTrafficPanelBinding.endpoint_hash == bound_reader.endpoint_hash,
                                    BsTrafficPanelBinding.retired_at.is_(None),
                                )
                                .with_for_update(read=True)
                            )
                            if binding is None:
                                return _not_sent(row, 'account_changed')
                            await bound_reader.assert_current()
                        # Binding admission may have waited behind retirement.
                        final_now = await db.scalar(select(func.clock_timestamp()))
                        if (
                            attempt.send_deadline <= final_now
                            or (boundaries and min(boundaries) <= final_now)
                            or row.valid_until <= final_now
                            or row.local_guard_hash != await _local_guard(db, user_id, subscription_id, final_now)
                        ):
                            return _not_sent(row, 'stale_evidence')
                        if plan.action == 'patch':
                            await patch_split_internal_squads(api, user_id=panel_id, squads=plan.squads)
                except PanelBusy:
                    return _not_sent(row, 'provider_backoff')
                # PATCH response alone is never an acknowledgement of delivery.
    except LedgerConflict:
        return 'obsolete'
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(session_factory, scope, pause.retry_after_seconds)
    except (TimeoutError, ValueError, TypeError, AttributeError):
        # Claim was committed separately, so rollback cannot allow a replay.
        pass
    return await recover_delivery(
        session_factory, read_api, **recovery, completed_attempt_token=attempt.token, topology_evidence=evidence
    )
