"""Deliver a committed renewal once, recovering unknown writes by GET only.

The financial coordinator owns receipt creation. Preparation may refresh a
pending target; after claim its full target is immutable. An open grace overlay
is left intact for the existing guarded grace restore coordinator. Local owner,
subscription and grace leases serialize the final preflight and bounded PATCH.
"""

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficPanelBinding,
    BsTrafficRenewalIntent,
    PaymentMethod,
    Subscription,
    Transaction,
    TransactionType,
)
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.base_extension import extension_boundary
from app.services.bs_traffic.delivery_plan import plan_squads
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.grace_capture import _open_grace
from app.services.bs_traffic.grace_intent import _decode, _encode, _restore_guard
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, require_activated_owner
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause, remaining_pause
from app.services.bs_traffic.policy import _read_policy, project_access
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway
from app.services.panel_sync.writer import patch_split_renewal_target


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def _payment_receipt(db, row, user_id):
    payment = row.parameters.get('payment_receipt') if isinstance(row.parameters, dict) else None
    if payment is None:
        method, external_id, amount = PaymentMethod.BALANCE.value, row.operation_key, -row.charged_kopeks
    else:
        if (
            not isinstance(payment, dict)
            or set(payment) != {'method', 'external_id', 'amount_kopeks'}
            or not isinstance(payment['method'], str)
            or not payment['method']
            or not isinstance(payment['external_id'], str)
            or not payment['external_id']
            or type(payment['amount_kopeks']) is not int
        ):
            raise LedgerConflict('Invalid persisted payment receipt')
        method, external_id, amount = payment['method'], payment['external_id'], payment['amount_kopeks']
    receipt = await db.scalar(
        select(Transaction.id).where(
            Transaction.id == row.transaction_id,
            Transaction.user_id == user_id,
            Transaction.external_id == external_id,
            Transaction.payment_method == method,
            Transaction.type == TransactionType.SUBSCRIPTION_PAYMENT.value,
            Transaction.is_completed.is_(True),
            Transaction.amount_kopeks == amount,
        )
    )
    return receipt


async def _load(db, reader, *, user_id, subscription_id, account_generation, operation_id, validate_attempt=True):
    ledger, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
    await require_activated_owner(db, subscription_id)
    row = await db.scalar(
        select(BsTrafficRenewalIntent)
        .where(BsTrafficRenewalIntent.id == operation_id, BsTrafficRenewalIntent.subscription_id == subscription_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        row is None
        or row.panel_scope != reader.panel_scope
        or ledger.panel_scope != reader.panel_scope
        or row.account_generation != account_generation
    ):
        raise LedgerConflict('Renewal receipt belongs to another account or panel')
    if row.transaction_id is None:
        return 'pending_receipt', row, ledger, panel_id
    receipt = await _payment_receipt(db, row, user_id)
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == reader.panel_scope,
            BsTrafficPanelBinding.endpoint_hash == reader.endpoint_hash,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    if receipt is None or binding is None:
        raise LedgerConflict('Paid renewal receipt or current panel binding missing')
    if row.state == 'superseded':
        return 'superseded', row, ledger, panel_id
    now = await db.scalar(select(func.clock_timestamp()))
    sub = await db.get(Subscription, subscription_id)
    if sub.end_date != row.intended_expires_at or sub.end_date <= now:
        return 'obsolete', row, ledger, panel_id
    if await _open_grace(db, subscription_id):
        return 'blocked_grace', row, ledger, panel_id
    # A read-only confirmation is a historical receipt, not new write permission.
    # Usage settlement may advance the ledger after the original PATCH.
    if row.state == 'attempted' and validate_attempt:
        try:
            guard = await _restore_guard(db, user_id, subscription_id, now)
        except LedgerConflict:
            return 'obsolete', row, ledger, panel_id
        if ledger.revision != row.ledger_revision or guard != row.guard_hash:
            return 'obsolete', row, ledger, panel_id
    return row.state, row, ledger, panel_id


async def _desired(db, reader, keys, ledger, current, evidence):
    now = await db.scalar(select(func.clock_timestamp()))
    if not evidence.is_fresh(now, panel_identity=reader.panel_scope):
        raise LedgerConflict('Renewal topology expired')
    projection = await project_access(db, **keys, evidence=evidence, grace_active=False)
    if any(
        group.state not in ('active', 'disabled', 'exhausted', 'pending')
        for group in (projection.ordinary, projection.bs)
    ):
        raise LedgerConflict('Renewal topology or access policy requires reconciliation')
    selection = plan_squads(projection, topology=evidence.topology, current_squads=_observed_squads(current))
    if selection.action == 'wait':
        raise LedgerConflict('Paid access projection is not ready')
    billing = await SQLAlchemyGraceBillingGateway(db).get_subscription(keys['subscription_id'])
    if billing.user_status != 'active' or billing.status not in ('active', 'trial', 'limited') or billing.end_at <= now:
        raise LedgerConflict('Paid renewal is no longer active')
    target = replace(
        current,
        status='ACTIVE',
        expire_at=billing.end_at,
        traffic_limit_bytes=0,
        traffic_limit_strategy='NO_RESET',
        hwid_device_limit=billing.device_limit,
        external_squad_uuid=billing.external_squad_uuid,
        active_internal_squads=[{'uuid': value} for value in selection.squads],
    )
    _encode(target)
    valid_until = min(evidence.valid_until, target.expire_at)
    for column in (BsTrafficAllowance.starts_at, BsTrafficAllowance.expires_at, BsTrafficAllowance.retired_at):
        boundary = await db.scalar(
            select(func.min(column)).where(BsTrafficAllowance.subscription_id == keys['subscription_id'], column > now)
        )
        if boundary is not None:
            valid_until = min(valid_until, boundary)
    boundary = await extension_boundary(db, keys['subscription_id'], now)
    if boundary is not None:
        valid_until = min(valid_until, boundary)
    return target, valid_until


async def _release_unsent(db, row, token, reason):
    """Only the live claim holder, on an explicit exit before invoking PATCH.

    Retain the complete immutable attempt before allowing fresh preparation.
    Never call from cancellation, exception recovery, or after invoking writer.
    """
    if row.state == 'attempted' and row.attempt_token == token:
        fields = (
            'target',
            'expected_source',
            'target_hash',
            'source_hash',
            'ledger_revision',
            'guard_hash',
            'valid_until',
            'attempt_token',
            'attempted_at',
            'send_deadline',
        )
        history = {field: getattr(row, field) for field in fields}
        for field in ('valid_until', 'attempted_at', 'send_deadline'):
            history[field] = history[field].isoformat()
        row.attempt_history = [*(row.attempt_history or []), dict(history, reason=reason)]
        row.state = 'pending'
        for field in fields:
            setattr(row, field, None)
        await db.flush()
    return reason


async def _supersede_pending(db, verified, user_id):
    """Retire only known-unsent older receipts covered by a verified newer term.

    This is not historical panel verification. Preserve money/terms and the
    archived unsent attempts, recording the exact successor for audit/replay.
    Unknown or attempted writes are deliberately never modified here.
    """
    if verified.state != 'verified' or verified.verified_at is None:
        return
    older = (
        await db.scalars(
            select(BsTrafficRenewalIntent)
            .where(
                BsTrafficRenewalIntent.subscription_id == verified.subscription_id,
                BsTrafficRenewalIntent.panel_scope == verified.panel_scope,
                BsTrafficRenewalIntent.account_generation == verified.account_generation,
                BsTrafficRenewalIntent.id < verified.id,
                BsTrafficRenewalIntent.created_at <= verified.created_at,
                BsTrafficRenewalIntent.intended_expires_at < verified.intended_expires_at,
                BsTrafficRenewalIntent.state == 'pending',
                BsTrafficRenewalIntent.attempt_token.is_(None),
                BsTrafficRenewalIntent.attempted_at.is_(None),
                BsTrafficRenewalIntent.send_deadline.is_(None),
                BsTrafficRenewalIntent.transaction_id.isnot(None),
            )
            .with_for_update()
        )
    ).all()
    for row in older:
        try:
            receipt = await _payment_receipt(db, row, user_id)
        except LedgerConflict:
            continue
        if receipt is not None:
            row.state = 'superseded'
            row.parameters = dict(row.parameters, superseded_by_intent_id=verified.id)
    await db.flush()


async def _recover(reader, keys):
    async with reader._factory() as db, db.begin():
        status, row, _, panel_id = await _load(db, reader, **keys, validate_attempt=False)
        if status != 'attempted':
            return 'already_verified' if status == 'verified' else status
        target = _decode(row.target, row.target_hash)
        if (
            target.id != panel_id
            or generation_of(panel_scope=reader.panel_scope, account=target) != keys['account_generation']
        ):
            raise LedgerConflict('Renewal target identity changed')
        expected = (row.attempt_token, row.target_hash, row.guard_hash)
    async with panel_slot(reader._factory, reader.panel_scope):
        observed = await reader.get_split_grace_snapshot(panel_id)
    if not full_restore_confirmed(observed, target):
        return 'unconfirmed'
    async with reader._factory() as db, db.begin():
        status, row, _, _ = await _load(db, reader, **keys, validate_attempt=False)
        if status != 'attempted':
            return 'already_verified' if status == 'verified' else status
        if expected != (row.attempt_token, row.target_hash, row.guard_hash):
            return 'obsolete'
        verified_at = await db.scalar(select(func.clock_timestamp()))
        row.state = 'verified'
        row.verified_at = verified_at
        await db.flush()
        await _supersede_pending(db, row, keys['user_id'])
    return 'verified'


async def advance_renewal_delivery(reader, *, user_id, subscription_id, account_generation, operation_id):
    """Resume a receipt without charging, resetting counters or repeating PATCH."""
    if not isinstance(reader, BoundPanelReader) or type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Bound reader and exact renewal receipt required')
    keys = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        operation_id=operation_id,
    )
    owner = {key: value for key, value in keys.items() if key != 'operation_id'}
    try:
        async with asyncio.timeout(60):
            await reader.assert_current()
            async with reader._factory() as db, db.begin():
                status, row, ledger, panel_id = await _load(db, reader, **keys, validate_attempt=False)
                if status == 'verified':
                    await _supersede_pending(db, row, user_id)
                    return 'already_verified'
                if status not in ('pending', 'attempted'):
                    return status
                if status == 'pending':
                    policy = _read_policy(ledger.access_policy)
                    revision = ledger.revision
                    now = await db.scalar(select(func.clock_timestamp()))
                    guard = await _restore_guard(db, user_id, subscription_id, now)
            if status == 'attempted':
                return await _recover(reader, keys)
            async with panel_slot(reader._factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
                current = await reader.get_split_grace_snapshot(panel_id)
            if (
                current is None
                or current.id != panel_id
                or generation_of(panel_scope=reader.panel_scope, account=current) != account_generation
            ):
                return 'obsolete'
            # Admission before claim; a busy panel does not consume the attempt.
            async with panel_slot(reader._factory, reader.panel_scope):
                async with reader._factory() as db, db.begin():
                    status, row, ledger, _ = await _load(db, reader, **keys)
                    if status != 'pending':
                        return status
                    now = await db.scalar(select(func.clock_timestamp()))
                    if ledger.revision != revision or guard != await _restore_guard(db, user_id, subscription_id, now):
                        return 'obsolete'
                    target, valid_until = await _desired(db, reader, owner, ledger, current, evidence)
                    if (
                        target.external_squad_uuid is not None
                        and await reader.split_external_squad_exists(target.external_squad_uuid) is not True
                    ):
                        return 'external_missing'
                    row.target, row.expected_source = _encode(target), _encode(current, paid=False)
                    row.target_hash, row.source_hash = _hash(row.target), _hash(row.expected_source)
                    row.guard_hash, row.ledger_revision, row.valid_until = guard, revision, valid_until
                    row.state, row.attempt_token = 'attempted', str(uuid4())
                    row.attempted_at = now
                    row.send_deadline = min(valid_until, now + timedelta(seconds=15))
                    token = row.attempt_token
                    await db.flush()
                # The claim committed before HTTP. Every uncertain exit retains it.
                async with reader._factory() as db, db.begin():
                    status, row, ledger, _ = await _load(db, reader, **keys)
                    if status != 'attempted' or row.attempt_token != token:
                        return await _release_unsent(db, row, token, status)
                    fresh = await reader.get_split_grace_snapshot(panel_id)
                    if not full_restore_confirmed(fresh, target):
                        if not full_restore_confirmed(fresh, current):
                            return await _release_unsent(db, row, token, 'source_changed')
                        try:
                            desired, boundary = await _desired(db, reader, owner, ledger, fresh, evidence)
                        except LedgerConflict:
                            return await _release_unsent(db, row, token, 'obsolete')
                        if not full_restore_confirmed(desired, target):
                            return await _release_unsent(db, row, token, 'obsolete')
                        now = await db.scalar(select(func.clock_timestamp()))
                        if (
                            row.send_deadline <= now
                            or boundary <= now
                            or await remaining_pause(db, panel_scope=reader.panel_scope) > 0
                        ):
                            return await _release_unsent(db, row, token, 'deferred')
                        await reader.assert_current()
                        await patch_split_renewal_target(reader._api, target=target)
            return await _recover(reader, keys)
    except PanelBusy:
        return 'deferred'
    except LedgerConflict:
        return 'obsolete'
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError, AttributeError):
        return 'unconfirmed'
