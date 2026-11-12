"""Guarded paid grace completion, without settling ambiguous split usage.

Explicit internal operation, not runtime enrollment. Historical verification is
insufficient: read the full account again and revalidate locked paid policy.
The legacy managed accountant must be migrated separately; never invoke its
aggregate watermark/projection as if it were a split accounting boundary.
"""

import asyncio
from dataclasses import replace

from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, GraceAccessSessionModel, Subscription
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.base_extension import extension_boundary
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.grace_recovery import _load
from app.services.bs_traffic.grace_restore import build_exit_target, entitled_before_squads, full_restore_confirmed
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.policy import project_access
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.grace_access_codec import _model_to_session
from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway, SQLAlchemyGraceSessionStore
from app.services.grace_access_service import GraceCompletionReason, GraceSessionState


async def complete_restore(reader, *, user_id, subscription_id, account_generation, operation_id, evidence):
    if (
        not isinstance(reader, BoundPanelReader)
        or not isinstance(evidence, TopologyEvidence)
        or type(operation_id) is not int
        or operation_id <= 0
    ):
        raise LedgerConflict('Bound reader, exact restore operation and topology required')
    keys = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        operation_id=operation_id,
    )
    factory = reader._factory
    try:
        async with asyncio.timeout(30):
            await reader.assert_current()
            async with factory() as db, db.begin():
                status, row, target = await _load(
                    db, reader, **keys, revalidate_verified=True, allow_revision_change=True
                )
                grace = await db.get(GraceAccessSessionModel, row.grace_id)
                if (
                    row.state == 'verified'
                    and grace is not None
                    and grace.state == 'completed'
                    and grace.version == row.grace_version + 1
                    and grace.completion_reason == row.completion_reason
                ):
                    return 'completed'  # Historical receipt only; does not change current access.
                if status != 'verified' or target is None:
                    return status
                expected = (row.attempt_token, row.target_hash, row.guard_hash)
            # No business row locks during network I/O.
            async with panel_slot(factory, reader.panel_scope):
                if target.external_squad_uuid is not None:
                    if await reader.split_external_squad_exists(target.external_squad_uuid) is not True:
                        return 'external_missing'
                observed = await reader.get_split_grace_snapshot(target.id)
            if not full_restore_confirmed(observed, target):
                return 'unconfirmed'
            await reader.assert_current()
            async with factory() as db, db.begin():
                status, row, current_target = await _load(
                    db, reader, **keys, revalidate_verified=True, allow_revision_change=True
                )
                if status != 'verified' or current_target is None:
                    return status
                if expected != (row.attempt_token, row.target_hash, row.guard_hash) or current_target != target:
                    return 'obsolete'
                sub = await db.get(Subscription, subscription_id)
                if sub.traffic_accounting_mode == 'managed':
                    return 'accounting_pending'
                grace = await db.get(GraceAccessSessionModel, row.grace_id)
                session = _model_to_session(grace)
                if session.split_before is None or grace.snapshot_version != 4:
                    return 'obsolete'
                now = await db.scalar(select(func.clock_timestamp()))
                if not evidence.is_fresh(now, panel_identity=reader.panel_scope):
                    return 'obsolete'
                boundaries = []
                for column in (
                    BsTrafficAllowance.starts_at,
                    BsTrafficAllowance.expires_at,
                    BsTrafficAllowance.retired_at,
                ):
                    boundary = await db.scalar(
                        select(func.min(column)).where(
                            BsTrafficAllowance.subscription_id == subscription_id, column > now
                        )
                    )
                    if boundary is not None:
                        boundaries.append(boundary)
                extended_boundary = await extension_boundary(db, subscription_id, now)
                if extended_boundary is not None:
                    boundaries.append(extended_boundary)
                projection = await project_access(
                    db,
                    user_id=user_id,
                    subscription_id=subscription_id,
                    account_generation=account_generation,
                    evidence=evidence,
                    grace_active=False,
                )
                billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(subscription_id)
                desired = build_exit_target(
                    completion_reason=row.completion_reason,
                    safe_expire_at=target.expire_at,
                    billing=billing,
                    projection=projection,
                    topology=evidence.topology,
                    current=observed,
                    overlay_squads=session.overlay.squad_uuids,
                    paid_before_squads=entitled_before_squads(session, grace),
                    paid_before_fingerprint=session.split_before.topology_fingerprint,
                    now=now,
                )
                if desired is None or not full_restore_confirmed(desired, target):
                    return 'policy_changed'
                status, checked, _ = await _load(
                    db, reader, **keys, revalidate_verified=True, allow_revision_change=True
                )
                finished_at = await db.scalar(select(func.clock_timestamp()))
                if (
                    status != 'verified'
                    or checked.attempt_token != row.attempt_token
                    or not evidence.is_fresh(finished_at, panel_identity=reader.panel_scope)
                    or (target.status == 'ACTIVE' and target.expire_at <= finished_at)
                    or (boundaries and min(boundaries) <= finished_at)
                ):
                    return 'obsolete'
                if target.status == 'DISABLED':
                    sub.grace_tail_expire_at = target.expire_at
                    await db.flush((sub,))
                # Pending usage and every grant remain untouched. Closing the
                # overlay is not evidence that node history has been flushed.
                saved = await SQLAlchemyGraceSessionStore(db).save(
                    replace(
                        session,
                        state=GraceSessionState.COMPLETED,
                        completion_reason=GraceCompletionReason(row.completion_reason),
                        completed_at=finished_at,
                        updated_at=finished_at,
                        last_error=None,
                    )
                )
                return 'completed' if saved.state is GraceSessionState.COMPLETED else 'obsolete'
    except PanelBusy:
        return 'deferred'
    except LedgerConflict:
        return 'obsolete'
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
