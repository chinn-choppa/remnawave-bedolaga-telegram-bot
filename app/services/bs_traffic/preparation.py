"""Prepare one split delivery from live topology/account evidence, without writes.

Only works for an already initialized ledger and configured policy. Does not
create a panel epoch, enroll accounts, migrate existing subscriptions or start a
scheduler. Panel credentials must belong to the persisted panel scope supplied
by the trusted runtime; matching numeric IDs alone cannot establish that scope.
"""

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC
from typing import Literal

from sqlalchemy import exists, func, select

from app.database.models import GraceAccessSessionModel, Subscription
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.day_rollover import stage_day_rollover
from app.services.bs_traffic.delivery_plan import plan_squads
from app.services.bs_traffic.delivery_store import _local_guard, enqueue
from app.services.bs_traffic.evidence import TopologyEvidence, collect_topology_evidence
from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.intake import _cursor, settle_reported_observation
from app.services.bs_traffic.ledger import LedgerConflict, _key, require_activated_owner
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause, read_panel_pause
from app.services.bs_traffic.policy import _read_policy, _topology, project_access
from app.services.bs_traffic.readback import _observed_squads, _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for


PREPARE_TIMEOUT = 15


@dataclass(frozen=True)
class PreparedDelivery:
    status: Literal['queued', 'stale', 'blocked', 'unavailable']
    operation_id: int | None = None
    evidence: TopologyEvidence | None = None
    guard_hash: str | None = None


def delivery_guard(*, user_id, subscription_id, account_generation, evidence):
    """Stable evidence identity; observation time and mutable current squads excluded."""
    value = [
        'bs-delivery-v1',
        user_id,
        subscription_id,
        account_generation,
        evidence.panel_identity,
        evidence.fingerprint,
        asdict(evidence.topology),
    ]
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def prepare_delivery(
    session_factory,
    api,
    *,
    user_id,
    subscription_id,
    account_generation,
    operation_key,
    topology_cache: TopologyEvidenceCache | None = None,
    settle_usage: bool = False,
):
    """Read outside SQL, then atomically check the unchanged local snapshot and queue.

    The returned evidence can be used by send_delivery only while fresh; that
    sender independently checks the account and recomputes the access plan.
    Optional topology_cache shares only fresh topology for the bound immutable
    panel client/scope. Account snapshots and local authorization are never cached.

    Explicit settle_usage also processes current-day finite nodes in the final
    transaction before projecting access. Missing daily baselines use guarded
    rollover; missing evidence aborts the transaction. Queue failure rolls back
    accounting; blocked projections preserve accounted evidence without a write.
    No automatic callers enable this mode until remaining runtime policy is ready.
    """
    _key(operation_key)
    if type(settle_usage) is not bool:
        raise LedgerConflict('Explicit usage settlement mode required')
    if settle_usage and (not isinstance(api, BoundPanelReader) or api._factory is not session_factory):
        raise LedgerConflict('Usage settlement requires matching bound panel reader')
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with session_factory() as db, db.begin():
        state, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
        if state.restriction_attempt and state.restriction_attempt.get('state') == 'attempted':
            return PreparedDelivery('blocked')
        policy = deepcopy(_read_policy(state.access_policy))
        scope, revision = state.panel_scope, state.revision
        if isinstance(api, BoundPanelReader) and api.panel_scope != scope:
            raise LedgerConflict('Bound panel scope does not match subscription')
        now = await db.scalar(select(func.clock_timestamp()))
        local_guard = await _local_guard(db, user_id, subscription_id, now)
    try:
        async with asyncio.timeout(PREPARE_TIMEOUT):
            if await read_panel_pause(session_factory, scope) > 0:
                return PreparedDelivery('unavailable')
            collect = topology_cache.get if topology_cache is not None else collect_topology_evidence
            async with panel_slot(session_factory, scope):
                evidence = await collect(
                    api,
                    panel_identity=scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
            if await read_panel_pause(session_factory, scope) > 0:
                return PreparedDelivery('unavailable')
            async with panel_slot(session_factory, scope):
                account = await api.get_split_access_snapshot(panel_id)
        if account is None or generation_of(panel_scope=scope, account=account) != account_generation:
            return PreparedDelivery('blocked')
        observed = _observed_squads(account)
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(session_factory, scope, pause.retry_after_seconds)
        return PreparedDelivery('unavailable')
    except TimeoutError:
        return PreparedDelivery('unavailable')
    except (ValueError, TypeError, AttributeError):
        return PreparedDelivery('blocked')
    async with session_factory() as db, db.begin():
        try:
            state, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
        except LedgerConflict:
            return PreparedDelivery('stale')
        now = await db.scalar(select(func.clock_timestamp()))
        if (
            state.revision != revision
            or state.panel_scope != scope
            or state.access_policy != policy
            or local_guard != await _local_guard(db, user_id, subscription_id, now)
            or not evidence.is_fresh(now, panel_identity=scope)
        ):
            return PreparedDelivery('stale')
        if settle_usage:
            if evidence.fingerprint != policy['fingerprint'] or _topology(evidence.topology) != policy['topology']:
                return PreparedDelivery('blocked')
            await require_activated_owner(db, subscription_id)
            day = now.astimezone(UTC).date()
            nodes = [
                (node, group)
                for group in ('ordinary', 'bs')
                if policy[f'{group}_mode'] == 'finite'
                for node in policy['topology'][f'{group}_nodes']
            ]
            if len(nodes) > 200:
                raise LedgerConflict('Usage settlement exceeds node budget')
            for node, group in nodes:
                cursor = await _cursor(db, subscription_id, node, day)
                if cursor is None:
                    await stage_day_rollover(db, **target, node_uuid=node, day=day, evidence=evidence)
                else:
                    if cursor.account_generation != account_generation or cursor.traffic_group != group:
                        raise LedgerConflict('Usage cursor no longer matches topology')
                    await settle_reported_observation(db, **target, node_uuid=node, day=day)
        grace = await db.scalar(
            select(
                exists().where(
                    GraceAccessSessionModel.subscription_id == subscription_id,
                    GraceAccessSessionModel.state != 'completed',
                )
            )
        )
        grace = bool(
            grace or await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == subscription_id))
        )
        projection = await project_access(db, **target, evidence=evidence, grace_active=grace)
        if any(
            group.state not in ('active', 'disabled', 'exhausted', 'pending')
            for group in (projection.ordinary, projection.bs)
        ):
            return PreparedDelivery('blocked')
        plan = plan_squads(projection, topology=evidence.topology, current_squads=observed)
        if plan.action == 'wait':
            return PreparedDelivery('blocked')
        guard = delivery_guard(**target, evidence=evidence)
        operation_id = await enqueue(
            db,
            **target,
            operation_key=operation_key,
            revision=projection.revision,
            guard_hash=guard,
            squads=plan.squads,
            valid_until=evidence.valid_until,
        )
        from app.services.bs_traffic.connection_drop import stage_drop

        await stage_drop(db, operation_id=operation_id, projection=projection, observed=observed, evidence=evidence)
        return PreparedDelivery('queued', operation_id, evidence, guard)
