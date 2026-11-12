"""Actual split grace issuance and recovery; no enrollment or counter resets."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select

from app.database.database import AsyncSessionLocal
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficGraceIntent,
    BsTrafficLedger,
    BsTrafficPanelBinding,
    BsTrafficRenewalIntent,
    GraceAccessSessionModel,
    Subscription,
)
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.grace_capture import capture_grace_context
from app.services.bs_traffic.grace_completion import complete_restore
from app.services.bs_traffic.grace_intent import _decode, _encode, _restore_guard, cancel_pending_restore, stage_restore
from app.services.bs_traffic.grace_restore import build_exit_target, entitled_before_squads, full_restore_confirmed
from app.services.bs_traffic.grace_sender import send_restore
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, require_activated_owner
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import bind_reader
from app.services.bs_traffic.policy import _read_policy, project_access
from app.services.bs_traffic.readback import _recovery_lock
from app.services.grace_access_codec import GraceSnapshotError, _model_to_session
from app.services.grace_access_service import (
    GraceAccessSession,
    GraceCompletionReason,
    GracePanelSnapshot,
    GraceReason,
    GraceSessionState,
    GraceStartDecision,
    GraceStartResult,
    billing_is_eligible,
    billing_is_revoked,
    build_incident_key,
    build_panel_overlay,
    panel_is_safe_pending_source,
    panel_status_matches_reason,
)
from app.services.panel_sync.writer import patch_split_grace_overlay
from app.services.remnawave_service import RemnaWaveService


def _snapshot(account):
    return GracePanelSnapshot(
        account.id,
        account.status,
        account.expire_at,
        account.traffic_limit_bytes,
        account.used_traffic_bytes,
        tuple(sorted(item['uuid'] for item in account.active_internal_squads)),
        account.external_squad_uuid,
        True,
        account.last_traffic_reset_at,
    )


def _packed(target):
    values = _encode(target, paid=False)
    digest = hashlib.sha256(json.dumps(values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return values, digest


@asynccontextmanager
async def runtime_reader(subscription_id, *, factory=None):
    """None selects legacy; a pending/invalid split owner fails closed."""
    factory = factory or AsyncSessionLocal
    async with factory() as db:
        sub = await db.get(Subscription, subscription_id)
        if sub is None or sub.traffic_accounting_mode not in ('split', 'split_pending'):
            keys = None
        else:
            await require_activated_owner(db, subscription_id)
            ledger = await db.get(BsTrafficLedger, subscription_id)
            keys = dict(
                user_id=sub.user_id, subscription_id=subscription_id, account_generation=ledger.account_generation
            )
            scope = ledger.panel_scope
    if keys is None:
        yield None, None
        return
    async with RemnaWaveService().get_api_client() as api:
        yield await bind_reader(factory, api, panel_scope=scope), keys


async def _locked(db, reader, keys):
    ledger, panel_id = await _recovery_lock(db, keys['user_id'], keys['subscription_id'], keys['account_generation'])
    await require_activated_owner(db, keys['subscription_id'])
    if ledger.panel_scope != reader.panel_scope:
        raise LedgerConflict('Grace belongs to another bound panel')
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == reader.panel_scope,
            BsTrafficPanelBinding.endpoint_hash == reader.endpoint_hash,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    if binding is None:
        raise LedgerConflict('Grace binding is obsolete')
    return ledger, panel_id


async def _evidence(reader, keys):
    async with reader._factory() as db, db.begin():
        ledger, panel_id = await _locked(db, reader, keys)
        policy = _read_policy(ledger.access_policy)
    async with panel_slot(reader._factory, reader.panel_scope):
        evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=policy['topology']['ordinary_squads'],
            bs_squads=policy['topology']['bs_squads'],
        )
        current = await reader.get_split_grace_snapshot(panel_id)
    if current is None or generation_of(panel_scope=reader.panel_scope, account=current) != keys['account_generation']:
        raise LedgerConflict('Grace panel account changed')
    return evidence, current


async def _start_split_grace(reader, *, user_id, subscription_id, account_generation, reason, policy):
    from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway, SQLAlchemyGraceSessionStore

    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    await reader.assert_current()
    async with reader._factory() as db:
        existing = await SQLAlchemyGraceSessionStore(db).get_open(subscription_id)
    if existing is not None:
        await advance_split_grace(reader, **keys)
        return GraceStartResult(GraceStartDecision.ALREADY_ACTIVE, existing)
    _, current = await _evidence(reader, keys)
    before = _snapshot(current)
    try:
        context = await capture_grace_context(reader, **keys, panel_before=before)
    except LedgerConflict:
        async with reader._factory() as db:
            winner = await SQLAlchemyGraceSessionStore(db).get_open(subscription_id)
        if (
            winner
            and winner.split_before
            and winner.split_before.account_generation == account_generation
            and winner.split_before.panel_scope == reader.panel_scope
        ):
            return GraceStartResult(GraceStartDecision.ALREADY_ACTIVE, winner)
        raise
    # The capture's separate sessions finish before acquiring the business owner lease.
    async with reader._factory() as db:
        await _locked(db, reader, keys)
        billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(subscription_id)
        candidate = (
            replace(billing, status='limited')
            if reason is GraceReason.LIMITED and billing and billing.status == 'active'
            else billing
        )
        if candidate is None or not billing_is_eligible(candidate, reason, policy):
            return GraceStartResult(GraceStartDecision.NOT_ELIGIBLE)
        if current.hwid_device_limit != candidate.device_limit or current.traffic_limit_strategy != 'NO_RESET':
            raise LedgerConflict('Pre-grace panel device limit or strategy differs from split source')
        if reason is not GraceReason.LIMITED and not panel_status_matches_reason(before.status, reason):
            return GraceStartResult(GraceStartDecision.SUPERSEDED)
        now = await db.scalar(select(func.clock_timestamp()))
        incident = await split_incident_key(
            db, billing=candidate, reason=reason, last_traffic_reset_at=before.last_traffic_reset_at, now=now
        )
        store = SQLAlchemyGraceSessionStore(db)
        previous = await store.get_by_incident(subscription_id, incident)
        if previous is not None:
            return GraceStartResult(GraceStartDecision.ALREADY_GRANTED, previous)
        overlay = build_panel_overlay(before, reason, policy, now=now, reset_used_traffic=False)
        session = await store.create(
            GraceAccessSession(
                id=str(uuid4()),
                subscription_id=subscription_id,
                remnawave_id=current.id,
                reason=reason,
                incident_key=incident,
                state=GraceSessionState.PENDING,
                billing_before=candidate,
                panel_before=before,
                split_before=context,
                overlay=overlay,
                started_at=now,
                grace_until=overlay.expire_at,
                updated_at=now,
            )
        )
        await db.commit()
    outcome = await advance_split_grace(reader, **keys)
    async with reader._factory() as db:
        saved = await SQLAlchemyGraceSessionStore(db).get_by_incident(subscription_id, incident)
    return GraceStartResult(
        GraceStartDecision.STARTED if outcome == 'activated' else GraceStartDecision.OBSERVED, saved or session
    )


def _overlay_confirmed(current, target, now):
    if current is None:
        return False
    status = current.status
    if (status == 'LIMITED' and current.used_traffic_bytes >= target.traffic_limit_bytes) or (
        status == 'EXPIRED' and target.expire_at <= now
    ):
        current = replace(current, status=target.status)
    return full_restore_confirmed(current, target)


async def _activate(reader, keys, evidence, current):
    from app.services.grace_access_runtime import SQLAlchemyGraceSessionStore

    claimed = False
    async with reader._factory() as db, db.begin():
        ledger, _ = await _locked(db, reader, keys)
        row = await db.scalar(
            select(GraceAccessSessionModel)
            .where(
                GraceAccessSessionModel.subscription_id == keys['subscription_id'],
                GraceAccessSessionModel.state != 'completed',
            )
            .with_for_update()
        )
        session = _model_to_session(row)
        if session.state is not GraceSessionState.PENDING:
            return 'unchanged'
        now = await db.scalar(select(func.clock_timestamp()))
        attempt = row.split_overlay_attempt
        history = []
        if attempt is not None and attempt.get('state') == 'cancelled':
            history = [*attempt.get('history', []), {key: value for key, value in attempt.items() if key != 'history'}]
            if len(history) > 32:
                return 'attention'
            attempt = None
        if attempt is None:
            from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway

            policy = _read_policy(ledger.access_policy)
            if (
                policy['fingerprint'] != session.split_before.topology_fingerprint
                or policy['tariff_id'] != session.split_before.tariff_id
            ):
                return 'obsolete'

            billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(keys['subscription_id'])
            if (
                billing is None
                or billing_is_revoked(billing)
                or billing.end_at != session.billing_before.end_at
                or billing.remnawave_id != session.remnawave_id
                or billing.is_trial != session.billing_before.is_trial
                or current.hwid_device_limit != billing.device_limit
                or current.traffic_limit_strategy != 'NO_RESET'
            ):
                return 'obsolete'
            if session.reason is GraceReason.LIMITED:
                from app.services.bs_traffic.access import GroupAccess
                from app.services.bs_traffic.grace import grace_candidate_reason

                access = await project_access(db, **keys, evidence=evidence, grace_active=False)
                groups = [
                    GroupAccess(
                        group.mode, group.remaining_bytes, ready=group.state != 'pending' and not group.needs_review
                    )
                    for group in (access.ordinary, access.bs)
                ]
                if grace_candidate_reason(*groups, expired=False, blocked=False) is not GraceReason.LIMITED:
                    return 'obsolete'
            if not evidence.is_fresh(now, panel_identity=reader.panel_scope) or now >= session.grace_until:
                return 'obsolete'
            if not panel_is_safe_pending_source(_snapshot(current), session.panel_before, session.overlay):
                return 'source_changed'
            target = replace(
                current,
                status='ACTIVE',
                expire_at=session.overlay.expire_at,
                traffic_limit_bytes=session.overlay.traffic_limit_bytes,
                traffic_limit_strategy='NO_RESET',
                active_internal_squads=[{'uuid': value} for value in session.overlay.squad_uuids],
                external_squad_uuid=session.overlay.external_squad_uuid,
            )
            encoded, digest = _packed(target)
            source, source_hash = _packed(current)
            attempt = dict(
                target=encoded,
                target_hash=digest,
                source=source,
                source_hash=source_hash,
                token=str(uuid4()),
                state='attempted',
                entitled_before_squads=[],
                history=history,
                guard=await _restore_guard(db, keys['user_id'], keys['subscription_id'], now),
                grace_version=row.version,
                valid_until=evidence.valid_until.isoformat(),
            )
            row.split_overlay_attempt = attempt
            claimed = True
        target = _decode(attempt['target'], attempt['target_hash'], paid=False)
        source = _decode(attempt['source'], attempt['source_hash'], paid=False)
    if claimed:
        async with panel_slot(reader._factory, reader.panel_scope):
            async with reader._factory() as db, db.begin():
                await _locked(db, reader, keys)
                row = await db.get(GraceAccessSessionModel, row.id)

                async def abort_unsent(reason):
                    # Only this live claim holder knows no PATCH was invoked.
                    # Exceptions/cancellation never call this release path.
                    if row.split_overlay_attempt == attempt:
                        row.split_overlay_attempt = dict(attempt, state='cancelled', cancelled_reason=reason)
                    return reason

                now = await db.scalar(select(func.clock_timestamp()))
                if (
                    row.state != 'pending'
                    or row.split_overlay_attempt != attempt
                    or now >= target.expire_at
                    or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
                    or attempt['guard'] != await _restore_guard(db, keys['user_id'], keys['subscription_id'], now)
                ):
                    return await abort_unsent('obsolete')
                observed = await reader.get_split_grace_snapshot(target.id)
                if not full_restore_confirmed(observed, target):
                    if not full_restore_confirmed(observed, source):
                        return await abort_unsent('source_changed')
                    if (
                        target.external_squad_uuid is not None
                        and await reader.split_external_squad_exists(target.external_squad_uuid) is not True
                    ):
                        return await abort_unsent('external_missing')
                    await reader.assert_current()
                    checked_at = await db.scalar(select(func.clock_timestamp()))
                    if target.expire_at <= checked_at or not evidence.is_fresh(
                        checked_at, panel_identity=reader.panel_scope
                    ):
                        return await abort_unsent('obsolete')
                    await patch_split_grace_overlay(reader._api, target=target)
    # An existing attempt is GET-only, including after exception/cancellation.
    async with panel_slot(reader._factory, reader.panel_scope):
        observed = await reader.get_split_grace_snapshot(target.id)
    if not _overlay_confirmed(observed, target, datetime.now(UTC)):
        return 'unconfirmed'
    async with reader._factory() as db:
        await _locked(db, reader, keys)
        row = await db.get(GraceAccessSessionModel, row.id)
        if row.state != 'pending' or row.split_overlay_attempt != attempt:
            return 'obsolete'
        row.split_overlay_attempt = dict(attempt, state='verified')
        saved = await SQLAlchemyGraceSessionStore(db).save(
            replace(_model_to_session(row), state=GraceSessionState.ACTIVE)
        )
        await db.commit()
        return 'activated' if saved.state is GraceSessionState.ACTIVE else 'obsolete'


async def _advance_split_grace(
    reader, *, user_id, subscription_id, account_generation, force_restore=False, drain=False
):
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway, SQLAlchemyGraceSessionStore

    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    await reader.assert_current()
    evidence, current = await _evidence(reader, keys)
    renewal_id = None
    async with reader._factory() as db:
        ledger, _ = await _locked(db, reader, keys)
        store = SQLAlchemyGraceSessionStore(db)
        session = await store.get_open(subscription_id)
        if session is None:
            return 'completed'
        before = session.split_before
        if (
            before is None
            or before.panel_scope != reader.panel_scope
            or before.account_generation != account_generation
            or before.panel_user_id != ledger.panel_user_id
            or session.remnawave_id != ledger.panel_user_id
        ):
            raise GraceSnapshotError('Split grace provenance belongs to another account or panel')
        billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(subscription_id)
        now = await db.scalar(select(func.clock_timestamp()))
        renewal_id = await db.scalar(
            select(BsTrafficRenewalIntent.id)
            .where(
                BsTrafficRenewalIntent.subscription_id == subscription_id,
                BsTrafficRenewalIntent.account_generation == account_generation,
                BsTrafficRenewalIntent.transaction_id.is_not(None),
                BsTrafficRenewalIntent.created_at >= session.started_at,
                BsTrafficRenewalIntent.intended_expires_at == billing.end_at,
            )
            .order_by(BsTrafficRenewalIntent.id.desc())
            .limit(1)
        )
        existing = await db.scalar(
            select(BsTrafficGraceIntent)
            .where(BsTrafficGraceIntent.grace_id == session.id, BsTrafficGraceIntent.state != 'cancelled')
            .with_for_update()
        )
        if (
            renewal_id is not None
            and existing is not None
            and existing.state == 'pending'
            and existing.completion_reason != 'paid'
        ):
            await cancel_pending_restore(db, **keys, operation_id=existing.id)
            await db.commit()
            existing = None
        if renewal_id is not None and (existing is None or existing.completion_reason == 'paid'):
            pending_row = await db.get(GraceAccessSessionModel, session.id)
            if session.state is GraceSessionState.PENDING and (
                pending_row.split_overlay_attempt is None
                or pending_row.split_overlay_attempt.get('state') == 'cancelled'
            ):
                from app.services.bs_traffic.renewal_delivery import _load as load_paid_renewal

                status, receipt, _, _ = await load_paid_renewal(
                    db, reader, **keys, operation_id=renewal_id, validate_attempt=False
                )
                if status != 'blocked_grace' or receipt.state != 'pending':
                    raise LedgerConflict('Exact committed renewal receipt required for unissued grace')
                await store.save(
                    replace(
                        session,
                        state=GraceSessionState.COMPLETED,
                        completion_reason=GraceCompletionReason.PAID,
                        completed_at=now,
                        updated_at=now,
                    )
                )
                await db.commit()
            else:
                await db.rollback()
        else:
            projection = await project_access(db, **keys, evidence=evidence, grace_active=False)
            reason = None
            if billing_is_revoked(billing):
                reason = 'revoked'
            elif force_restore or (drain and session.state is GraceSessionState.PENDING):
                reason = 'drained'
            elif now >= session.grace_until:
                reason = 'timeout'
            elif (session.reason is GraceReason.EXPIRED and billing.end_at and billing.end_at > now) or (
                session.reason is GraceReason.LIMITED
                and any(group.state == 'active' for group in (projection.ordinary, projection.bs))
            ):
                reason = 'conflict'
            if existing is not None:
                reason = existing.completion_reason
            if reason is None:
                pending = session.state is GraceSessionState.PENDING
                await db.rollback()
                return await _activate(reader, keys, evidence, current) if pending else 'unchanged'
            pending_row = await db.get(GraceAccessSessionModel, session.id)
            if session.state is GraceSessionState.PENDING and (
                pending_row.split_overlay_attempt is None
                or pending_row.split_overlay_attempt.get('state') == 'cancelled'
            ):
                await store.save(
                    replace(
                        session,
                        state=GraceSessionState.COMPLETED,
                        completion_reason=GraceCompletionReason(reason),
                        completed_at=now,
                        updated_at=now,
                    )
                )
                await db.commit()
                return reason
            if existing is not None and existing.state != 'pending':
                operation_id = existing.id
                await db.commit()
            else:
                if existing is not None:
                    await cancel_pending_restore(db, **keys, operation_id=existing.id)
                from app.services.bs_traffic.renewal_grace import _overlay_matches

                source = current
                if current.status == 'DISABLED' and (
                    billing_is_revoked(billing) or billing.end_at is None or billing.end_at <= now
                ):
                    source = replace(current, status='ACTIVE')
                if not _overlay_matches(source, session, now):
                    return 'source_changed'
                if session.state is not GraceSessionState.RESTORING:
                    session = await store.save(replace(session, state=GraceSessionState.RESTORING))
                    ledger, _ = await _locked(db, reader, keys)
                now = await db.scalar(select(func.clock_timestamp()))
                target = build_exit_target(
                    completion_reason=reason,
                    billing=billing,
                    projection=projection,
                    topology=evidence.topology,
                    current=current,
                    overlay_squads=session.overlay.squad_uuids,
                    paid_before_squads=entitled_before_squads(session, pending_row),
                    paid_before_fingerprint=session.split_before.topology_fingerprint,
                    now=now,
                )
                if target is None:
                    return 'blocked'
                operation_id = await stage_restore(
                    db,
                    **keys,
                    grace_id=session.id,
                    grace_version=session.version,
                    revision=ledger.revision,
                    target=target,
                    expected_source=current,
                    valid_until=min(evidence.valid_until, target.expire_at),
                    completion_reason=reason,
                )
                await db.commit()
            renewal_id = None
    if renewal_id is not None:
        result = await advance_paid_renewal(reader, **keys, operation_id=renewal_id)
        return 'paid' if result in ('verified', 'already_verified') else result
    result = await send_restore(reader, **keys, operation_id=operation_id, evidence=evidence)
    if result != 'verified':
        return result
    result = await complete_restore(reader, **keys, operation_id=operation_id, evidence=evidence)
    return reason if result == 'completed' else result


async def confirmed_active_overlay(db, *, subscription_id, account_generation, panel_scope, now):
    """Local verified-issuance predicate, not a claim of fresh remote evidence."""
    try:
        await require_activated_owner(db, subscription_id)
        ledger = await db.get(BsTrafficLedger, subscription_id)
        row = await db.scalar(
            select(GraceAccessSessionModel).where(
                GraceAccessSessionModel.subscription_id == subscription_id,
                GraceAccessSessionModel.state == 'active',
                GraceAccessSessionModel.snapshot_version == 4,
            )
        )
        if row is None or row.grace_until <= now:
            return False
        binding = await db.scalar(
            select(BsTrafficPanelBinding.panel_scope).where(
                BsTrafficPanelBinding.panel_scope == panel_scope, BsTrafficPanelBinding.retired_at.is_(None)
            )
        )
        if binding is None:
            return False
        session = _model_to_session(row)
        before, attempt = session.split_before, row.split_overlay_attempt
        if (
            before is None
            or attempt is None
            or attempt.get('state') != 'verified'
            or attempt.get('entitled_before_squads') != []
            or before.panel_scope != panel_scope
            or before.account_generation != account_generation
            or ledger.panel_scope != panel_scope
            or ledger.account_generation != account_generation
            or _read_policy(ledger.access_policy)['fingerprint'] != before.topology_fingerprint
        ):
            return False
        target = _decode(attempt['target'], attempt['target_hash'], paid=False)
        return bool(
            generation_of(panel_scope=panel_scope, account=target) == account_generation
            and target.id == session.remnawave_id == before.panel_user_id
            and target.status == 'ACTIVE'
            and target.traffic_limit_strategy == 'NO_RESET'
            and target.expire_at == session.overlay.expire_at
            and target.traffic_limit_bytes == session.overlay.traffic_limit_bytes
            and target.external_squad_uuid == session.overlay.external_squad_uuid
            and tuple(sorted(item['uuid'] for item in target.active_internal_squads))
            == tuple(sorted(session.overlay.squad_uuids))
            and not session.overlay.reset_used_traffic
        )
    except (LedgerConflict, GraceSnapshotError, ValueError, TypeError, KeyError, AttributeError):
        return False


async def start_split_grace(reader, **kwargs):
    async with asyncio.timeout(90):
        return await _start_split_grace(reader, **kwargs)


async def advance_split_grace(reader, **kwargs):
    async with asyncio.timeout(90):
        return await _advance_split_grace(reader, **kwargs)


async def split_incident_key(db, *, billing, reason, last_traffic_reset_at, now):
    key = build_incident_key(billing, reason, last_traffic_reset_at=last_traffic_reset_at)
    if reason is not GraceReason.LIMITED:
        return key
    # Started base periods substitute the native panel-reset epoch. Topups,
    # usage/revisions and retirement cannot manufacture another grace grant.
    boundary = await db.scalar(
        select(func.max(BsTrafficAllowance.starts_at)).where(
            BsTrafficAllowance.subscription_id == billing.subscription_id,
            BsTrafficAllowance.operation_key.startswith('base:renew:'),
            BsTrafficAllowance.starts_at <= now,
        )
    )
    return key + ':split-base:' + (boundary.isoformat() if boundary is not None else 'initial')
