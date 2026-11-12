"""Read-only preparation of a managed-to-split panel target.

Not permission to PATCH or activate accounting. A durable single-attempt sender
must revalidate this local guard, full source and topology, then verify readback.
No quiet period or matching counter is treated as a usage-flush boundary.
"""

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficManagedTransfer,
    BsTrafficPanelBinding,
    Subscription,
    Tariff,
    TrafficAccountingState,
    User,
)
from app.external.remnawave_api import RemnaWaveAPIError, SplitGraceSnapshot
from app.services.bs_traffic.access import GroupAccess, desired_squads
from app.services.bs_traffic.base_extension import extension_boundary
from app.services.bs_traffic.evidence import TopologyEvidence, collect_topology_evidence
from app.services.bs_traffic.grace_capture import _open_grace
from app.services.bs_traffic.grace_intent import _encode
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, summary
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.policy import _read_policy, _topology
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for


@dataclass(frozen=True)
class ManagedCutoverPlan:
    revision: int
    guard_hash: str
    expected_source: SplitGraceSnapshot
    target: SplitGraceSnapshot
    evidence: TopologyEvidence
    valid_until: datetime


async def _pending_snapshot(db, reader, *, user_id, subscription_id, account_generation):
    ledger, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
    sub = await db.get(Subscription, subscription_id, populate_existing=True)
    receipt = await db.get(BsTrafficManagedTransfer, subscription_id, populate_existing=True)
    source = await db.get(TrafficAccountingState, subscription_id, populate_existing=True)
    now = await db.scalar(select(func.clock_timestamp()))
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == reader.panel_scope,
            BsTrafficPanelBinding.endpoint_hash == reader.endpoint_hash,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    if (
        binding is None
        or ledger.panel_scope != reader.panel_scope
        or sub.traffic_accounting_mode != 'split_pending'
        or receipt is None
        or receipt.ownership_cycle != ledger.ownership_cycle
        or source is None
        or receipt.panel_scope != reader.panel_scope
        or receipt.account_generation != account_generation
        or receipt.source_phase not in ('ready', 'unlimited')
        or source.phase != 'transferred'
        or source.revision != receipt.source_revision + 1
        or source.panel_identity != receipt.source_identity
        or source.used_bytes != receipt.source_used_bytes
        or source.lifetime_bytes != receipt.source_lifetime_bytes
        or source.pending_sync_fields
        or source.activation_requested
        or source.grace_excluded
        or settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True
        or await _open_grace(db, subscription_id)
        or sub.is_trial
        or sub.status != 'active'
        or sub.end_date is None
        or sub.end_date <= now
        or await db.scalar(select(User.status).where(User.id == user_id)) != 'active'
    ):
        raise LedgerConflict('Managed cutover is not ready')
    from app.database.models import BsTrafficOwnershipCycle

    cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, ledger.ownership_cycle))
    if cycle is not None and cycle.state != 'split':
        raise LedgerConflict('Reverse ownership transfer cannot use the forward writer')
    policy = deepcopy(_read_policy(ledger.access_policy))
    tariff = await db.get(Tariff, sub.tariff_id, populate_existing=True)
    if tariff is None or policy['tariff_id'] != sub.tariff_id:
        raise LedgerConflict('Cutover tariff changed')
    balances = await summary(
        db, user_id=user_id, subscription_id=subscription_id, account_generation=account_generation, now=now
    )
    if any(item['pending_operations'] or item['needs_review'] for item in balances.values()):
        raise LedgerConflict('Unresolved usage must not cross the cutover')
    boundaries = [sub.end_date, now + timedelta(seconds=15)]
    if source.next_refresh_at is not None:
        if source.next_refresh_at <= now:
            raise LedgerConflict('Managed calendar refresh needs reconciliation')
        boundaries.append(source.next_refresh_at)
    for column in (BsTrafficAllowance.starts_at, BsTrafficAllowance.expires_at, BsTrafficAllowance.retired_at):
        boundary = await db.scalar(
            select(func.min(column)).where(
                BsTrafficAllowance.subscription_id == subscription_id,
                column > now,
            )
        )
        if boundary is not None:
            boundaries.append(boundary)
    extended_boundary = await extension_boundary(db, subscription_id, now)
    if extended_boundary is not None:
        boundaries.append(extended_boundary)
    # Fingerprint current rights, not ORM object identity or the observation time.
    values = [
        user_id,
        subscription_id,
        account_generation,
        reader.panel_scope,
        reader.endpoint_hash,
        panel_id,
        ledger.revision,
        policy,
        balances,
        sub.tariff_id,
        sub.end_date,
        sub.device_limit,
        sub.connected_squads,
        tariff.updated_at,
        tariff.external_squad_uuid,
        receipt.manifest_hash,
        receipt.operation_key,
        receipt.source_phase,
        receipt.source_used_bytes,
        receipt.imported_bytes,
        receipt.grants,
        source.next_refresh_at,
    ]
    guard = hashlib.sha256(json.dumps(values, sort_keys=True, default=lambda v: v.isoformat()).encode()).hexdigest()
    quota = 0 if receipt.source_phase == 'unlimited' else max(1, receipt.source_used_bytes + receipt.imported_bytes)
    return dict(
        panel_id=panel_id,
        ownership_cycle=ledger.ownership_cycle,
        revision=ledger.revision,
        guard_hash=guard,
        policy=policy,
        balances=balances,
        expiry=sub.end_date,
        source_quota=quota,
        source_used=receipt.source_used_bytes,
        valid_until=min(boundaries),
    )


def _build_target(before, current, evidence, *, panel_scope, account_generation):
    policy = before['policy']
    if not isinstance(current, SplitGraceSnapshot):
        raise LedgerConflict('Full cutover source required')
    _encode(current, paid=False)  # Strict full snapshot validation; no write.
    if (
        generation_of(panel_scope=panel_scope, account=current) != account_generation
        or current.status not in ('ACTIVE', 'LIMITED')
        or current.expire_at != before['expiry']
        or current.traffic_limit_strategy != 'NO_RESET'
        or current.traffic_limit_bytes != before['source_quota']
        or current.used_traffic_bytes < before['source_used']
        or evidence.fingerprint != policy['fingerprint']
        or _topology(evidence.topology) != policy['topology']
    ):
        raise LedgerConflict('Panel source or topology changed before cutover')
    current_squads = set(_observed_squads(current))
    managed = set(evidence.topology.ordinary_squads) | set(evidence.topology.bs_squads)
    if not current_squads <= managed:
        raise LedgerConflict('Cutover source contains unmanaged squads')
    groups = [
        GroupAccess(
            policy[f'{name}_mode'],
            before['balances'][name]['remaining_bytes'] if policy[f'{name}_mode'] == 'finite' else None,
        )
        for name in ('ordinary', 'bs')
    ]
    squads = desired_squads(evidence.topology, *groups, subscription_allowed=True)
    if not squads:
        raise LedgerConflict('Cutover has no confirmed paid access')
    target = replace(
        current, status='ACTIVE', traffic_limit_bytes=0, active_internal_squads=[{'uuid': item} for item in squads]
    )
    _encode(target)
    return target


async def prepare_managed_cutover(reader, *, user_id, subscription_id, account_generation):
    """Read without holding business locks, then reject any changed local rights."""
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound panel reader required')
    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    factory = reader._factory
    async with asyncio.timeout(30):
        await reader.assert_current()
        async with factory() as db, db.begin():
            before = await _pending_snapshot(db, reader, **keys)
        policy = before['policy']
        try:
            async with panel_slot(factory, reader.panel_scope):
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
            async with panel_slot(factory, reader.panel_scope):
                current = await reader.get_split_grace_snapshot(before['panel_id'])
        except RemnaWaveAPIError as exc:
            pause = backoff_for(exc)
            if pause is not None:
                await persist_panel_pause(factory, reader.panel_scope, pause.retry_after_seconds)
            raise
        target = _build_target(
            before, current, evidence, panel_scope=reader.panel_scope, account_generation=account_generation
        )
        await reader.assert_current()
        async with factory() as db, db.begin():
            after = await _pending_snapshot(db, reader, **keys)
            now = await db.scalar(select(func.clock_timestamp()))
            deadline = min(before['valid_until'], after['valid_until'], evidence.valid_until)
            if (
                after['guard_hash'] != before['guard_hash']
                or now >= deadline
                or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
            ):
                raise LedgerConflict('Local rights changed during cutover preparation')
        return ManagedCutoverPlan(before['revision'], before['guard_hash'], current, target, evidence, deadline)
