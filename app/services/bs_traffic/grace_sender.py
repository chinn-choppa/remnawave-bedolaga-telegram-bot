"""Single full paid restore attempt; explicit internal call, no runtime loop.

Commit attempted before HTTP. Hold local grace/business locks during bounded
preflight and write; remote admin changes cannot be fenced by this API.
All uncertain writes go to read-only recovery and are never automatically resent.
Only explicit preflight exits from the live claim holder may cancel its intent.
Exceptions (including cancellation) retain attempted even before PATCH: a later
worker cannot establish that the original coroutine will never send.
External-squad assignment requires a bound catalog lookup; explicit null detaches.
"""

import asyncio

from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, GraceAccessSessionModel
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.base_extension import extension_boundary
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.grace_intent import claim_restore
from app.services.bs_traffic.grace_recovery import _load, recover_restore
from app.services.bs_traffic.grace_restore import build_exit_target, entitled_before_squads, full_restore_confirmed
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause, remaining_pause
from app.services.bs_traffic.policy import project_access
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.grace_access_codec import _model_to_session
from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway
from app.services.panel_sync.writer import patch_split_grace_target


async def send_restore(reader, *, user_id, subscription_id, account_generation, operation_id, evidence):
    if not isinstance(reader, BoundPanelReader) or not isinstance(evidence, TopologyEvidence):
        raise LedgerConflict('Bound panel and topology evidence required')
    factory = reader._factory
    keys = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        operation_id=operation_id,
    )
    attempt = None
    try:
        async with asyncio.timeout(30):
            await reader.assert_current()
            # Admission before claim: a busy provider must not consume an attempt.
            async with panel_slot(factory, reader.panel_scope):
                async with factory() as db, db.begin():
                    attempt = await claim_restore(db, **keys)
                if attempt is not None:
                    async with factory() as db, db.begin():
                        status, row, target = await _load(db, reader, **keys)

                        async def abort_unsent(reason):
                            # Only called on explicit exits below, before invoking
                            # the writer. Never use from recovery/exception handlers.
                            if row.state == 'attempted' and row.attempt_token == attempt.token:
                                row.state = 'cancelled'
                                await db.flush()
                            return reason

                        if status != 'attempted' or row.attempt_token != attempt.token:
                            return await abort_unsent(status)
                        now = await db.scalar(select(func.clock_timestamp()))
                        if attempt.send_deadline <= now or not evidence.is_fresh(
                            now, panel_identity=reader.panel_scope
                        ):
                            return await abort_unsent('obsolete')
                        if target.external_squad_uuid is not None:
                            if await reader.split_external_squad_exists(target.external_squad_uuid) is not True:
                                return await abort_unsent('external_missing')
                        current = await reader.get_split_grace_snapshot(target.id)
                        if not full_restore_confirmed(current, target):
                            if not full_restore_confirmed(current, attempt.expected_source):
                                return await abort_unsent('source_changed')
                            grace = await db.get(GraceAccessSessionModel, row.grace_id)
                            session = _model_to_session(grace)
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
                                current=current,
                                overlay_squads=session.overlay.squad_uuids,
                                paid_before_squads=entitled_before_squads(session, grace),
                                paid_before_fingerprint=session.split_before.topology_fingerprint,
                                now=planned_at,
                            )
                            if desired is None or not full_restore_confirmed(desired, target):
                                return await abort_unsent('policy_changed')
                            status, checked, _ = await _load(db, reader, **keys)
                            now = await db.scalar(select(func.clock_timestamp()))
                            if (
                                status != 'attempted'
                                or checked.attempt_token != attempt.token
                                or attempt.send_deadline <= now
                                or row.valid_until <= now
                                or (boundaries and min(boundaries) <= now)
                                or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
                            ):
                                return await abort_unsent('obsolete')
                            if await remaining_pause(db, panel_scope=reader.panel_scope) > 0:
                                return await abort_unsent('deferred')
                            await reader.assert_current()
                            await patch_split_grace_target(reader._api, target=target)
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
        if attempt is None:
            return 'unconfirmed'
        # The claim is already committed: rollback never permits a second PATCH.
    return await recover_restore(reader, **keys)
