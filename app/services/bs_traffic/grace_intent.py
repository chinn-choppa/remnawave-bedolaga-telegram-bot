"""Durable target staging and single-attempt claims; no panel I/O.

Caller owns commit. Commit the claim before any HTTP. The sender still must validate the full
overlay, topology, local rights, binding and deadline immediately before PATCH.
This module never completes grace or retries an unknown external operation.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficGraceIntent,
    BsTrafficPanelBinding,
    GraceAccessSessionModel,
    Subscription,
    Tariff,
)
from app.external.remnawave_api import SplitGraceSnapshot
from app.services.bs_traffic.delivery_store import _local_guard
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, _lock
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock
from app.services.grace_access_codec import _model_to_session


async def _restore_guard(db, user_id, subscription_id, now):
    local = await _local_guard(db, user_id, subscription_id, now)
    extra = (
        await db.execute(
            select(Subscription.device_limit, Tariff.external_squad_uuid)
            .select_from(Subscription)
            .outerjoin(Tariff, Tariff.id == Subscription.tariff_id)
            .where(Subscription.id == subscription_id, Subscription.user_id == user_id)
        )
    ).one()
    return hashlib.sha256(json.dumps([local, *extra], separators=(',', ':')).encode()).hexdigest()


def _encode(target, *, paid=True):
    if not isinstance(target, SplitGraceSnapshot):
        raise LedgerConflict('Full typed restore target required')
    for value in (target.created_at, target.expire_at, target.last_traffic_reset_at):
        if value is not None and (not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None):
            raise LedgerConflict('Aware target dates required')
    if target.created_at is None or target.expire_at is None:
        raise LedgerConflict('Target identity and expiry required')
    if paid and (
        target.status != 'ACTIVE' or target.traffic_limit_bytes != 0 or target.traffic_limit_strategy != 'NO_RESET'
    ):
        raise LedgerConflict('Only paid split restoration targets supported')
    if target.status not in ('ACTIVE', 'LIMITED', 'EXPIRED', 'DISABLED') or target.traffic_limit_strategy not in (
        'NO_RESET',
        'DAY',
        'WEEK',
        'MONTH',
        'MONTH_ROLLING',
    ):
        raise LedgerConflict('Invalid full source state')
    for value in (target.traffic_limit_bytes, target.used_traffic_bytes):
        if type(value) is not int or not 0 <= value < 2**53:
            raise LedgerConflict('Exact target counter required')
    devices = target.hwid_device_limit
    if devices is not None and (type(devices) is not int or not (0 if paid else -(2**53) + 1) <= devices < 2**53):
        raise LedgerConflict('Invalid target device limit')
    external = target.external_squad_uuid
    if external is not None:
        try:
            if not isinstance(external, str) or str(UUID(external)) != external:
                raise ValueError
        except ValueError:
            raise LedgerConflict('Invalid target external squad') from None
    values = asdict(target)
    values['active_internal_squads'] = [{'uuid': uuid} for uuid in _observed_squads(target)]
    for field in ('created_at', 'expire_at', 'last_traffic_reset_at'):
        value = values[field]
        values[field] = value.astimezone(UTC).isoformat() if value is not None else None
    return values


async def stage_restore(
    db,
    *,
    user_id,
    subscription_id,
    account_generation,
    grace_id,
    grace_version,
    revision,
    target,
    expected_source,
    valid_until,
    completion_reason='paid',
):
    if type(grace_version) is not int or grace_version <= 0 or type(revision) is not int or revision < 0:
        raise LedgerConflict('Exact restore versions required')
    if not isinstance(valid_until, datetime) or valid_until.tzinfo is None or valid_until.utcoffset() is None:
        raise LedgerConflict('Aware restore deadline required')
    if completion_reason not in ('paid', 'timeout', 'revoked', 'drained', 'conflict'):
        raise LedgerConflict('Explicit grace completion reason required')
    values = _encode(target, paid=completion_reason == 'paid')
    if (
        target.status not in ('ACTIVE', 'DISABLED')
        or target.traffic_limit_bytes != 0
        or target.traffic_limit_strategy != 'NO_RESET'
    ):
        raise LedgerConflict('Invalid split exit target')
    if target.status == 'DISABLED' and (target.active_internal_squads or target.external_squad_uuid is not None):
        raise LedgerConflict('Revoked split exit cannot retain access squads')
    source_values = _encode(expected_source, paid=False)
    source_hash = hashlib.sha256(json.dumps(source_values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    digest = hashlib.sha256(json.dumps(values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    ledger, panel_id = await _lock(db, user_id, subscription_id, account_generation)
    if generation_of(panel_scope=ledger.panel_scope, account=target) != account_generation or target.id != panel_id:
        raise LedgerConflict('Restore target belongs to another panel account')
    session_row = await db.scalar(
        select(GraceAccessSessionModel)
        .where(
            GraceAccessSessionModel.id == grace_id,
            GraceAccessSessionModel.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if session_row is None:
        raise LedgerConflict('Grace session does not belong to this subscription')
    existing = await db.scalar(
        select(BsTrafficGraceIntent).where(
            BsTrafficGraceIntent.grace_id == grace_id, BsTrafficGraceIntent.state != 'cancelled'
        )
    )
    if existing is not None:
        if (
            existing.target_hash,
            existing.source_hash,
            existing.grace_version,
            existing.ledger_revision,
            existing.account_generation,
            existing.panel_scope,
        ) != (
            digest,
            source_hash,
            grace_version,
            revision,
            account_generation,
            ledger.panel_scope,
        ):
            raise LedgerConflict('Durable restore target cannot be replaced')
        if existing.completion_reason != completion_reason:
            raise LedgerConflict('Durable restore reason cannot be replaced')
        return existing.id
    session = _model_to_session(session_row)
    if (
        session_row.state != 'restoring'
        or session_row.version != grace_version
        or ledger.revision != revision
        or session.split_before is None
        or session.split_before.panel_scope != ledger.panel_scope
        or session.split_before.account_generation != account_generation
    ):
        raise LedgerConflict('Grace restore source changed')
    if (
        generation_of(panel_scope=ledger.panel_scope, account=expected_source) != account_generation
        or expected_source.id != target.id
        or expected_source.expire_at != session.overlay.expire_at
        or expected_source.traffic_limit_bytes != session.overlay.traffic_limit_bytes
        or _observed_squads(expected_source) != tuple(sorted(session.overlay.squad_uuids))
        or expected_source.external_squad_uuid != session.overlay.external_squad_uuid
        or expected_source.used_traffic_bytes != target.used_traffic_bytes
        or expected_source.last_traffic_reset_at != target.last_traffic_reset_at
    ):
        raise LedgerConflict('Expected full source does not match the saved grace overlay')
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == ledger.panel_scope,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        completion_reason != 'revoked'
        and expected_source.status not in ('ACTIVE', 'LIMITED')
        and now < session.overlay.expire_at
    ):
        raise LedgerConflict('Unexpected disabled grace source before expiry')
    if binding is None or valid_until <= now or target.expire_at <= now:
        raise LedgerConflict('Restore evidence or paid term expired')
    row = BsTrafficGraceIntent(
        subscription_id=subscription_id,
        grace_id=grace_id,
        grace_version=grace_version,
        ledger_revision=revision,
        panel_scope=ledger.panel_scope,
        account_generation=account_generation,
        completion_reason=completion_reason,
        target=values,
        expected_source=source_values,
        source_hash=source_hash,
        target_hash=digest,
        guard_hash=await _restore_guard(db, user_id, subscription_id, now),
        state='pending',
        valid_until=valid_until,
    )
    db.add(row)
    await db.flush()
    return row.id


@dataclass(frozen=True)
class RestoreClaim:
    operation_id: int
    token: str
    target: SplitGraceSnapshot
    expected_source: SplitGraceSnapshot
    send_deadline: datetime


def _decode(raw, digest, *, paid=True):
    try:
        values = dict(raw)
        for field in ('created_at', 'expire_at', 'last_traffic_reset_at'):
            value = values[field]
            values[field] = datetime.fromisoformat(value) if value is not None else None
        target = SplitGraceSnapshot(**values)
        canonical = _encode(target, paid=paid)
        actual = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if canonical != raw or actual != digest:
            raise ValueError
        return target
    except (ValueError, TypeError, KeyError, AttributeError):
        raise LedgerConflict('Stored restore target is invalid') from None


async def claim_restore(db, *, user_id, subscription_id, account_generation, operation_id):
    """Reserve one attempt; caller MUST commit before HTTP and never replay it.

    Returning a receipt does not prove the current remote overlay. A sender must
    hold/reacquire the grace lease and revalidate after commit before its one PATCH.
    An expired attempted request stays attempted for read-only reconciliation.
    """
    if type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Exact restore operation ID required')
    ledger, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
    row = await db.scalar(
        select(BsTrafficGraceIntent)
        .where(
            BsTrafficGraceIntent.id == operation_id,
            BsTrafficGraceIntent.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise LedgerConflict('Restore operation does not belong to this subscription')
    if row.state != 'pending':
        return None
    target = _decode(row.target, row.target_hash, paid=row.completion_reason == 'paid')
    source = _decode(row.expected_source, row.source_hash, paid=False)
    if (
        generation_of(panel_scope=ledger.panel_scope, account=source) != account_generation
        or source.used_traffic_bytes != target.used_traffic_bytes
        or source.last_traffic_reset_at != target.last_traffic_reset_at
        or row.account_generation != account_generation
        or row.panel_scope != ledger.panel_scope
        or target.id != panel_id
        or generation_of(panel_scope=ledger.panel_scope, account=target) != account_generation
    ):
        raise LedgerConflict('Stored restore account binding changed')
    grace = await db.scalar(
        select(GraceAccessSessionModel)
        .where(
            GraceAccessSessionModel.id == row.grace_id,
            GraceAccessSessionModel.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == row.panel_scope,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        grace is None
        or grace.state != 'restoring'
        or grace.version != row.grace_version
        or ledger.revision != row.ledger_revision
        or binding is None
        or row.guard_hash != await _restore_guard(db, user_id, subscription_id, now)
        or row.valid_until <= now
        or target.expire_at <= now
    ):
        raise LedgerConflict('Restore source or evidence is no longer current')
    row.state = 'attempted'
    row.attempt_token = str(uuid4())
    row.attempted_at = now
    row.send_deadline = min(row.valid_until, target.expire_at, now + timedelta(seconds=15))
    await db.flush()
    return RestoreClaim(row.id, row.attempt_token, target, source, row.send_deadline)


async def cancel_pending_restore(db, *, user_id, subscription_id, account_generation, operation_id):
    """Retire a never-claimed target. Caller owns commit; attempts cannot be cancelled here."""
    if type(operation_id) is not int or operation_id <= 0:
        raise LedgerConflict('Exact restore operation ID required')
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    row = await db.scalar(
        select(BsTrafficGraceIntent)
        .where(
            BsTrafficGraceIntent.id == operation_id,
            BsTrafficGraceIntent.subscription_id == subscription_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None or row.account_generation != account_generation or row.panel_scope != ledger.panel_scope:
        raise LedgerConflict('Restore operation belongs to another account or panel')
    if row.state != 'pending':
        return False
    row.state = 'cancelled'
    await db.flush()
    return True
