"""Resume a paid renewal through an existing split grace restoration.

Payment is already committed. This coordinator never issues grace, charges,
resets counters or infers missing pre-grace provenance. A durable link in the
renewal receipt selects the full restore intent; ambiguous attempts are read
back by the existing sender, never replaced by a second PATCH.
"""

import asyncio
from dataclasses import replace

from sqlalchemy import func, select

from app.database.models import BsTrafficGraceIntent, GraceAccessSessionModel
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.grace_completion import complete_restore
from app.services.bs_traffic.grace_intent import _decode, cancel_pending_restore, stage_restore
from app.services.bs_traffic.grace_restore import (
    build_paid_restore_target,
    entitled_before_squads,
    full_restore_confirmed,
)
from app.services.bs_traffic.grace_sender import send_restore
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.policy import _read_policy, project_access
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.bs_traffic.renewal_delivery import _load as load_renewal, advance_renewal_delivery
from app.services.grace_access_codec import GraceSnapshotError, _model_to_session
from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway, SQLAlchemyGraceSessionStore
from app.services.grace_access_service import GraceSessionState


async def require_no_unresolved_grace_restore(db, subscription_id):
    """Call under the financial owner's locks, before accepting a new payment."""
    overlays = (
        await db.scalars(
            select(GraceAccessSessionModel.split_overlay_attempt).where(
                GraceAccessSessionModel.subscription_id == subscription_id,
                GraceAccessSessionModel.state != 'completed',
                GraceAccessSessionModel.split_overlay_attempt.is_not(None),
            )
        )
    ).all()
    if any(
        value is not None and (not isinstance(value, dict) or value.get('state') == 'attempted') for value in overlays
    ):
        raise LedgerConflict('Previous grace overlay attempt must be reconciled before another renewal')
    pending = await db.scalar(
        select(BsTrafficGraceIntent.id)
        .join(GraceAccessSessionModel, GraceAccessSessionModel.id == BsTrafficGraceIntent.grace_id)
        .where(
            BsTrafficGraceIntent.subscription_id == subscription_id,
            BsTrafficGraceIntent.state.in_(('attempted', 'attention', 'verified')),
            GraceAccessSessionModel.state != 'completed',
        )
        .limit(1)
    )
    if pending is not None:
        raise LedgerConflict('Previous grace restoration must be reconciled before another renewal')


async def _context(db, reader, keys):
    status, receipt, ledger, panel_id = await load_renewal(db, reader, **keys, validate_attempt=False)
    if status != 'blocked_grace' or receipt.state != 'pending':
        return status, receipt, ledger, None
    rows = (
        await db.scalars(
            select(GraceAccessSessionModel)
            .where(
                GraceAccessSessionModel.subscription_id == keys['subscription_id'],
                GraceAccessSessionModel.state != 'completed',
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    if len(rows) != 1:
        raise LedgerConflict('Exactly one persisted grace overlay is required')
    grace = _model_to_session(rows[0])
    before = grace.split_before
    link = receipt.parameters.get('grace_restore')
    if (
        before is None
        or rows[0].snapshot_version != 4
        or before.panel_scope != reader.panel_scope
        or before.account_generation != keys['account_generation']
        or before.panel_user_id != panel_id
        or grace.remnawave_id != panel_id
        or grace.started_at > receipt.created_at
        or (
            link is not None
            and (
                not isinstance(link, dict)
                or set(link) != {'grace_id', 'operation_id'}
                or link.get('grace_id') != grace.id
                or type(link.get('operation_id')) is not int
                or link['operation_id'] <= 0
            )
        )
    ):
        raise LedgerConflict('Paid receipt does not authorize this grace overlay')
    billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(keys['subscription_id'])
    if (
        billing is None
        or billing.user_status != 'active'
        or billing.status not in ('active', 'trial', 'limited')
        or billing.remnawave_id != panel_id
    ):
        raise LedgerConflict('Current paid subscription cannot restore this overlay')
    return status, receipt, ledger, grace


def _overlay_matches(current, grace, now):
    # Device/strategy/reset fields were not changed by the temporary overlay.
    # An unexplained administrator change is not permission to overwrite it.
    if current is None or grace.overlay.reset_used_traffic:
        return False
    if current.status not in ('ACTIVE', 'LIMITED') and not (
        current.status == 'EXPIRED' and grace.overlay.expire_at <= now
    ):
        return False
    expected = replace(
        current,
        status=current.status,
        expire_at=grace.overlay.expire_at,
        traffic_limit_bytes=grace.overlay.traffic_limit_bytes,
        traffic_limit_strategy='NO_RESET',
        hwid_device_limit=grace.billing_before.device_limit,
        external_squad_uuid=grace.overlay.external_squad_uuid,
        active_internal_squads=[{'uuid': value} for value in grace.overlay.squad_uuids],
        used_traffic_bytes=grace.panel_before.used_traffic_bytes,
        last_traffic_reset_at=grace.panel_before.last_traffic_reset_at,
    )
    return full_restore_confirmed(current, expected)


async def _prepare_restore(reader, keys, evidence, current, expected_grace_id):
    owner = {key: value for key, value in keys.items() if key != 'operation_id'}
    # GraceSessionStore.save(RESTORING) owns a durable checkpoint commit. Do not
    # wrap it in db.begin(): reacquire and revalidate all local rights afterward.
    async with reader._factory() as db:
        status, receipt, ledger, grace = await _context(db, reader, keys)
        if grace is None:
            return status, None
        if grace.id != expected_grace_id:
            return 'obsolete', None
        existing = await db.scalar(
            select(BsTrafficGraceIntent)
            .where(
                BsTrafficGraceIntent.grace_id == grace.id,
                BsTrafficGraceIntent.state != 'cancelled',
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if existing is not None and existing.state != 'pending':
            target = _decode(existing.target, existing.target_hash, paid=existing.completion_reason == 'paid')
            link = receipt.parameters.get('grace_restore')
            if link is not None and link['operation_id'] != existing.id:
                return 'obsolete', None
            if (
                existing.panel_scope != reader.panel_scope
                or existing.account_generation != keys['account_generation']
                or target.expire_at != receipt.intended_expires_at
            ):
                return 'unconfirmed', None
            receipt.parameters = dict(
                receipt.parameters, grace_restore={'grace_id': grace.id, 'operation_id': existing.id}
            )
            existing_id = existing.id
            await db.commit()
            return 'ready', existing_id
        now = await db.scalar(select(func.clock_timestamp()))
        if generation_of(panel_scope=reader.panel_scope, account=current) != keys[
            'account_generation'
        ] or not _overlay_matches(current, grace, now):
            return 'source_changed', None
        if existing is not None:
            await cancel_pending_restore(db, **owner, operation_id=existing.id)
        if grace.state is not GraceSessionState.RESTORING:
            grace = await SQLAlchemyGraceSessionStore(db).save(replace(grace, state=GraceSessionState.RESTORING))
            status, receipt, ledger, checked = await _context(db, reader, keys)
            if checked is None or checked.id != grace.id or checked.state is not GraceSessionState.RESTORING:
                return 'obsolete', None
            grace = checked
        now = await db.scalar(select(func.clock_timestamp()))
        if not evidence.is_fresh(now, panel_identity=reader.panel_scope):
            return 'obsolete', None
        projection = await project_access(db, **owner, evidence=evidence, grace_active=False)
        billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(keys['subscription_id'])
        target = build_paid_restore_target(
            billing=billing,
            projection=projection,
            topology=evidence.topology,
            current=current,
            overlay_squads=grace.overlay.squad_uuids,
            paid_before_squads=entitled_before_squads(grace, await db.get(GraceAccessSessionModel, grace.id)),
            paid_before_fingerprint=grace.split_before.topology_fingerprint,
            now=now,
        )
        if target is None:
            return 'blocked', None
        operation_id = await stage_restore(
            db,
            **owner,
            grace_id=grace.id,
            grace_version=grace.version,
            revision=ledger.revision,
            target=target,
            expected_source=current,
            valid_until=min(evidence.valid_until, receipt.intended_expires_at),
        )
        receipt.parameters = dict(
            receipt.parameters, grace_restore={'grace_id': grace.id, 'operation_id': operation_id}
        )
        await db.commit()
        return 'ready', operation_id


async def advance_paid_renewal(reader, *, user_id, subscription_id, account_generation, operation_id):
    """Use in place of advance_renewal_delivery after committed paid checkout."""
    if not isinstance(reader, BoundPanelReader) or type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Bound reader and exact paid renewal receipt required')
    keys = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        operation_id=operation_id,
    )
    try:
        async with asyncio.timeout(90):
            await reader.assert_current()
            async with reader._factory() as db, db.begin():
                status, _, ledger, grace = await _context(db, reader, keys)
                if grace is not None:
                    policy = _read_policy(ledger.access_policy)
                    grace_id, panel_id = grace.id, grace.remnawave_id
            if grace is None:
                if status == 'blocked_grace':
                    return status
                return await advance_renewal_delivery(reader, **keys)
            async with panel_slot(reader._factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
                current = await reader.get_split_grace_snapshot(panel_id)
            status, restore_id = await _prepare_restore(reader, keys, evidence, current, grace_id)
            if status != 'ready':
                return status
            restore_keys = dict(keys, operation_id=restore_id)
            status = await send_restore(reader, **restore_keys, evidence=evidence)
            if status != 'verified':
                return status
            status = await complete_restore(reader, **restore_keys, evidence=evidence)
            if status != 'completed':
                return status
            return await advance_renewal_delivery(reader, **keys)
    except PanelBusy:
        return 'deferred'
    except (LedgerConflict, GraceSnapshotError):
        return 'obsolete'
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
