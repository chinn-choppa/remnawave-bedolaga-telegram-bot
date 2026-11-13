"""Finite per-pass historical polling through the shared queue and budgets."""

from datetime import timedelta

import structlog
from sqlalchemy import cast, func, or_, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert

from app.database.models import (
    BsTrafficCollectionJob as Job,
    BsTrafficLedger as Ledger,
    BsTrafficNodeMembership as Membership,
    BsTrafficNodeTail as Tail,
    BsTrafficObservation as Raw,
)
from app.services.bs_traffic.queue import enqueue_job


async def _park(db, member, reason):
    member.state = 'attention'
    await db.execute(update(Tail).where(Tail.membership_id == member.id).values(state='attention', last_error=reason))
    structlog.get_logger(__name__).warning(
        'Split removed node tail requires attention', membership_id=member.id, reason=reason
    )


async def enqueue_due_tails(db, reader, *, now, limit=4):
    if type(limit) is not int or not 1 <= limit <= 8:
        raise ValueError('Tail queue budget must be 1..8')
    await reader.assert_current()
    from app.services.bs_traffic.panel_admission import _slot_key

    await db.execute(select(func.pg_advisory_xact_lock(_slot_key(reader.panel_scope, 'node-tail-plan'))))
    # Keyset-like indexed due pages are finite; completed tasks remain pending
    # because equal counters cannot prove that the panel flushed old sessions.
    rows = (
        await db.execute(
            select(Tail, Membership, Ledger)
            .join(Membership, Membership.id == Tail.membership_id)
            .join(Ledger, Ledger.subscription_id == Membership.subscription_id)
            .where(
                Membership.panel_scope == reader.panel_scope,
                Membership.state == 'tail',
                Tail.state == 'pending',
                Tail.next_poll_at <= now,
            )
            .order_by(Tail.next_poll_at, Tail.membership_id, Tail.day)
            .limit(limit)
            .with_for_update(of=Tail, skip_locked=True)
        )
    ).all()
    queued = 0
    for task, member, ledger in rows:
        if member.state != 'tail' or task.next_poll_at > now:
            continue
        if now >= member.source_paid_end_at or (
            ledger.panel_scope,
            ledger.panel_user_id,
            ledger.account_generation,
            ledger.ownership_cycle,
        ) != (member.panel_scope, member.panel_user_id, member.account_generation, member.ownership_cycle):
            await _park(db, member, 'horizon_or_owner_changed')
            continue
        count = await db.scalar(select(func.count()).select_from(Tail).where(Tail.membership_id == member.id))
        latest = await db.scalar(select(func.max(Tail.day)).where(Tail.membership_id == member.id))
        if latest < now.date():
            if count >= 128:
                await _park(db, member, 'day_budget_exceeded')
                continue
            await db.execute(
                insert(Tail)
                .values(
                    membership_id=member.id,
                    day=latest + timedelta(days=1),
                    state='pending',
                    next_poll_at=now,
                    attempts=0,
                )
                .on_conflict_do_nothing()
            )
        task.next_poll_at = now + timedelta(seconds=30)
        job = await db.get(Job, task.job_id) if task.job_id else None
        if job is not None and job.state in ('pending', 'leased'):
            continue
        if job is not None:
            raw = await db.get(
                Raw, (member.panel_scope, member.panel_user_id, member.account_generation, member.node_uuid, task.day)
            )
            task.last_error = (
                None
                if job.state == 'complete' and raw is not None and raw.latest_poll_id == job.poll_id
                else 'collection_unresolved'
            )
        shared = await db.scalar(
            select(Job)
            .where(
                Job.panel_scope == reader.panel_scope,
                Job.day == task.day,
                Job.state.in_(('pending', 'leased', 'complete')),
                Job.created_at >= now - timedelta(seconds=30),
                cast(Job.node_uuids, JSONB).contains([member.node_uuid]),
                cast(Job.identities, JSONB).contains({str(member.panel_user_id): member.account_generation}),
            )
            .order_by(Job.id.desc())
            .limit(1)
        )
        if shared is not None:
            task.job_id = shared.id
            continue
        pending = await db.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.panel_scope == reader.panel_scope, Job.state.in_(('pending', 'leased')))
        )
        tail_pending = await db.scalar(
            select(func.count())
            .select_from(Job)
            .where(
                Job.panel_scope == reader.panel_scope,
                Job.operation_key.like('tail:%'),
                Job.state.in_(('pending', 'leased')),
            )
        )
        # Reserve the remaining shared producer slots for current fleet work.
        if pending >= 16 or tail_pending >= 4:
            task.next_poll_at = now + timedelta(seconds=1)
            task.last_error = 'backpressure'
            continue
        # Batch due owners of this exact historical node/day. A bounded page
        # shares one full-node request; raw high-water makes fleet overlap safe.
        peers = (
            await db.execute(
                select(Tail, Membership)
                .join(Membership, Membership.id == Tail.membership_id)
                .join(Ledger, Ledger.subscription_id == Membership.subscription_id)
                .outerjoin(Job, Job.id == Tail.job_id)
                .where(
                    Membership.panel_scope == reader.panel_scope,
                    Membership.node_uuid == member.node_uuid,
                    Membership.state == 'tail',
                    Membership.source_paid_end_at > now,
                    Membership.account_generation == Ledger.account_generation,
                    Membership.panel_user_id == Ledger.panel_user_id,
                    Membership.ownership_cycle == Ledger.ownership_cycle,
                    Ledger.panel_scope == Membership.panel_scope,
                    Tail.day == task.day,
                    Tail.membership_id != member.id,
                    Tail.state == 'pending',
                    Tail.next_poll_at <= now,
                    or_(Tail.job_id.is_(None), Job.state.in_(('complete', 'attention', 'cancelled'))),
                )
                .order_by(Tail.membership_id)
                .limit(499)
                .with_for_update(of=Tail)
            )
        ).all()
        group = [(task, member), *[(t, m) for t, m in peers if t.membership_id != task.membership_id]]
        task.attempts += 1
        identifier = await enqueue_job(
            db,
            operation_key=f'tail:{member.id}:{task.day}:{task.attempts}',
            panel_scope=member.panel_scope,
            day=task.day,
            node_uuids=(member.node_uuid,),
            identities={m.panel_user_id: m.account_generation for _, m in group},
        )
        for pending_task, _ in group:
            pending_task.job_id = identifier
            pending_task.next_poll_at = now + timedelta(seconds=30)
        await db.flush()
        queued += 1
    await db.flush()
    return queued
