"""Bounded read-only PostgreSQL snapshot, independent of HTTP worker placement.

No panel IO, lazy recovery, row locks, or accounting writes. Every query has a
five-second database timeout; the whole snapshot has a ten-second deadline.
The durable snapshot is authoritative; process-local observations are nullable.
"""

import asyncio
from datetime import timedelta

from sqlalchemy import func, or_, select, text

from app.cabinet.schemas.split_diagnostics import (
    SplitDiagnosticQueues,
    SplitDiagnosticScope,
    SplitDiagnosticsResponse,
)
from app.config import settings
from app.database.models import (
    BsTrafficCollectionBudget as Budget,
    BsTrafficCollectionJob as Job,
    BsTrafficDelivery as Delivery,
    BsTrafficEnrollment as Enrollment,
    BsTrafficFleetRound as Round,
    BsTrafficFleetWork as Work,
    BsTrafficLedger as Ledger,
    BsTrafficNodeMembership as Membership,
    BsTrafficNodeTail as Tail,
    BsTrafficOwnershipCycle as Cycle,
    BsTrafficPanelBinding as Binding,
    BsTrafficRenewalIntent as Renewal,
)


MAX_SCOPES = 16


async def _aggregate(db, scopes, scope_column, model, conditions, age_column, *, join=None, where=None):
    """Return a fixed number of aggregate rows, never materialize job/account rows."""
    statement = select(
        scope_column,
        *(func.count().filter(condition).label(key) for key, condition in conditions.items()),
        func.min(age_column).filter(or_(*conditions.values())).label('oldest'),
    ).select_from(model)
    if join is not None:
        statement = statement.join(*join)
    if where is not None:
        statement = statement.where(where)
    rows = await db.execute(statement.where(scope_column.in_(scopes), or_(*conditions.values())).group_by(scope_column))
    return {row[0]: row._mapping for row in rows}


async def read_diagnostics(session_factory, *, runtime):
    async with asyncio.timeout(10), session_factory() as db, db.begin():
        await db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        await db.execute(text("SET LOCAL statement_timeout = '5s'"))
        await db.execute(text("SET LOCAL lock_timeout = '1s'"))
        now = await db.scalar(select(func.clock_timestamp()))
        fresh_seconds = int(runtime.interval * 3)
        cutoff = now - timedelta(seconds=fresh_seconds)
        total = await db.scalar(select(func.count()).select_from(Binding).where(Binding.retired_at.is_(None)))
        scopes = list(
            await db.scalars(
                select(Binding.panel_scope)
                .where(Binding.retired_at.is_(None))
                .order_by(Binding.created_at, Binding.panel_scope)
                .limit(MAX_SCOPES)
            )
        )
        unbound = await db.scalar(
            select(func.count())
            .select_from(Enrollment)
            .where(Enrollment.panel_scope.is_(None), Enrollment.state != 'verified')
        )
        task = runtime.task
        local_running = None if task is None else not task.done()
        result = SplitDiagnosticsResponse(
            checked_at=now,
            freshness_seconds=fresh_seconds,
            local_runtime_running=local_running,
            force_disconnect_enabled=settings.BS_TRAFFIC_FORCE_DISCONNECT_ENABLED is True,
            active_bindings=total,
            bindings_truncated=total > MAX_SCOPES,
            unbound_enrollments=unbound,
            scopes=[],
        )
        if not scopes:
            return result
        rounds = {row.panel_scope: row for row in await db.scalars(select(Round).where(Round.panel_scope.in_(scopes)))}
        pauses = dict(
            (
                await db.execute(
                    select(Budget.panel_scope, Budget.cooldown_until).where(Budget.panel_scope.in_(scopes))
                )
            ).all()
        )
        # Every aggregate is scoped before grouping; output is at most MAX_SCOPES.
        returned = (
            select(Cycle.subscription_id)
            .join(
                Ledger,
                (Ledger.subscription_id == Cycle.subscription_id) & (Ledger.ownership_cycle == Cycle.ownership_cycle),
            )
            .where(
                Cycle.subscription_id == Work.subscription_id,
                Cycle.state == 'managed',
                Cycle.reverse_attempt['state'].as_string() == 'verified',
            )
            .exists()
        )
        groups = [
            await _aggregate(
                db,
                scopes,
                Job.panel_scope,
                Job,
                {
                    'collection_pending': Job.state.in_(('pending', 'leased')),
                    'collection_attention': Job.state == 'attention',
                },
                Job.created_at,
            ),
            await _aggregate(
                db,
                scopes,
                Work.panel_scope,
                Work,
                {
                    'settlement_pending': Work.access_dirty
                    | Work.settled_at.is_(None)
                    | (Work.settled_at < cutoff)
                    | Work.completed_round.is_distinct_from(Work.target_round),
                    'settlement_errors': Work.error.is_not(None),
                },
                Work.due_at,
                where=~returned,
            ),
            await _aggregate(
                db,
                scopes,
                Ledger.panel_scope,
                Delivery,
                {
                    'delivery_pending': Delivery.state.in_(('pending', 'attempted')),
                    'delivery_attention': Delivery.state == 'attention',
                    'connection_drop_pending': (Delivery.state == 'verified')
                    & (Delivery.connection_drop['state'].as_string() == 'pending'),
                    'connection_drop_unknown': (Delivery.state == 'verified')
                    & (Delivery.connection_drop['state'].as_string() == 'attempted'),
                },
                Delivery.created_at,
                join=(Ledger, Ledger.subscription_id == Delivery.subscription_id),
            ),
            await _aggregate(
                db,
                scopes,
                Renewal.panel_scope,
                Renewal,
                {
                    'renewal_pending': Renewal.state.in_(('pending', 'attempted')),
                },
                Renewal.created_at,
            ),
            await _aggregate(
                db,
                scopes,
                Enrollment.panel_scope,
                Enrollment,
                {
                    'enrollment_pending': Enrollment.state != 'verified',
                },
                Enrollment.created_at,
            ),
            await _aggregate(
                db,
                scopes,
                Ledger.panel_scope,
                Cycle,
                {
                    'reverse_pending': Cycle.state == 'reverse_pending',
                },
                Cycle.created_at,
                join=(
                    Ledger,
                    (Ledger.subscription_id == Cycle.subscription_id)
                    & (Ledger.ownership_cycle == Cycle.ownership_cycle),
                ),
            ),
            await _aggregate(
                db,
                scopes,
                Membership.panel_scope,
                Tail,
                {
                    'node_tail_pending': Tail.state == 'pending',
                    'node_tail_attention': Tail.state == 'attention',
                    'node_tail_delayed': (Tail.state == 'pending')
                    & (Tail.last_error.is_not(None) | (Tail.next_poll_at < cutoff)),
                },
                Membership.retired_at,
                join=(Membership, Membership.id == Tail.membership_id),
            ),
            await _aggregate(
                db,
                scopes,
                Membership.panel_scope,
                Membership,
                {
                    'topology_attention': Membership.state == 'attention',
                },
                Membership.retired_at,
            ),
        ]
        for number, scope in enumerate(scopes, start=1):
            counts = {}
            oldest = []
            for group in groups:
                values = group.get(scope)
                if values is None:
                    continue
                for key, value in values.items():
                    if key in SplitDiagnosticQueues.model_fields:
                        counts[key] = counts.get(key, 0) + value
                if values['oldest'] is not None:
                    oldest.append(values['oldest'])
            queues = SplitDiagnosticQueues(**counts)
            row = rounds.get(scope)
            last_success = row.last_success_at if row else None
            fresh = last_success is not None and cutoff <= last_success <= now
            pause = pauses.get(scope)
            warnings = []
            if not fresh:
                warnings.append('no_fresh_round')
            if row is not None and (row.error_count or row.phase == 'attention'):
                warnings.append('round_errors')
            if pause is not None and pause > now:
                warnings.append('panel_backoff')
            warnings.extend(key for key, value in queues.model_dump().items() if value)
            # Pending node history and optional socket cleanup do not block
            # accounting by themselves. Attention always remains visible.
            blocking = [
                w
                for w in warnings
                if w not in ('node_tail_pending', 'connection_drop_pending', 'connection_drop_unknown')
            ]
            result.scopes.append(
                SplitDiagnosticScope(
                    number=number,
                    collection_phase=row.phase if row else None,
                    last_success_at=last_success,
                    pause_until=pause if pause is not None and pause > now else None,
                    diagnostic_ready=not blocking,
                    local_runtime_ready=runtime.ready(scope) if task is not None else None,
                    queues=queues,
                    oldest_pending_at=min(oldest) if oldest else None,
                    warnings=warnings,
                )
            )
        return result
