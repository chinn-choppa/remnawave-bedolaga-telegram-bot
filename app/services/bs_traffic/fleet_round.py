"""Node-shared collection from durable SQL pages; no enrollment or panel I/O."""

from dataclasses import dataclass
from datetime import UTC, timedelta
from uuid import uuid4

from sqlalchemy import cast, delete, func, literal, select
from sqlalchemy.dialects.postgresql import JSONB, insert

from app.database.models import (
    BsTrafficCollectionJob as Job,
    BsTrafficFleetMember as Member,
    BsTrafficFleetNode as Node,
    BsTrafficFleetRound as Round,
    BsTrafficFleetWork as Work,
    BsTrafficLedger as Ledger,
)
from app.services.bs_traffic.ledger import LedgerConflict, activated_owner_query
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.queue import enqueue_job
from app.services.bs_traffic.recovery_scan import _postgres


@dataclass(frozen=True)
class PlannedPage:
    accounts: int
    more: bool


async def _round(db, reader):
    _postgres(db)
    await reader.assert_current()
    scope = reader.panel_scope
    # Stable advisory transaction lock prevents two processes creating a round.
    from app.services.bs_traffic.panel_admission import _slot_key

    await db.execute(select(func.pg_advisory_xact_lock(_slot_key(scope, 'fleet-plan'))))
    row = await db.get(Round, scope, with_for_update=True, populate_existing=True)
    now = await db.scalar(select(func.clock_timestamp()))
    if row is None:
        upper = await db.scalar(
            select(func.coalesce(func.max(Ledger.subscription_id), 0)).where(Ledger.panel_scope == scope)
        )
        row = Round(
            panel_scope=scope,
            round_id=str(uuid4()),
            day=now.astimezone(UTC).date(),
            upper_id=upper,
            cursor=0,
            phase='building',
            started_at=now,
            error_count=0,
        )
        db.add(row)
        await db.flush()
    return row, now


async def plan_page(db, reader, *, limit=500, interval_seconds=30):
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Fleet SQL page must contain 1..500 accounts')
    row, now = await _round(db, reader)
    if row.phase == 'complete':
        if now < row.started_at + timedelta(seconds=interval_seconds):
            return PlannedPage(0, False)
        # Re-poll the closing day after midnight before advancing to the next.
        day = row.day
        today = now.astimezone(UTC).date()
        if day < today and row.completed_at.astimezone(UTC).date() > day:
            day += timedelta(days=1)
        await db.execute(delete(Node).where(Node.round_id == row.round_id))
        await db.execute(delete(Member).where(Member.round_id == row.round_id))
        row.round_id, row.day = str(uuid4()), day
        row.upper_id = await db.scalar(
            select(func.coalesce(func.max(Ledger.subscription_id), 0)).where(Ledger.panel_scope == row.panel_scope)
        )
        row.cursor, row.phase, row.started_at, row.completed_at, row.error_count = 0, 'building', now, None, 0
        await db.flush()
    if row.phase != 'building':
        return PlannedPage(0, False)
    if row.cursor == 0:
        await db.execute(
            delete(Work).where(
                Work.panel_scope == reader.panel_scope,
                Work.subscription_id.not_in(
                    activated_owner_query()
                    .with_only_columns(Ledger.subscription_id, maintain_column_froms=True)
                    .where(Ledger.panel_scope == reader.panel_scope)
                ),
            )
        )
    from app.services.bs_traffic.collection_membership import collection_membership, validate_pending_member

    membership = collection_membership()
    records = (
        await db.execute(
            select(membership)
            .where(
                membership.c.panel_scope == row.panel_scope,
                membership.c.subscription_id > row.cursor,
                membership.c.subscription_id <= row.upper_id,
            )
            .order_by(membership.c.subscription_id)
            .limit(limit + 1)
        )
    ).all()
    page = records[:limit]
    from app.database.models import BsTrafficNodeMembership

    tail_members = (
        (
            await db.execute(
                select(BsTrafficNodeMembership)
                .join(Ledger, Ledger.subscription_id == BsTrafficNodeMembership.subscription_id)
                .where(
                    BsTrafficNodeMembership.ownership_cycle == Ledger.ownership_cycle,
                    BsTrafficNodeMembership.subscription_id.in_([item.subscription_id for item in page]),
                    BsTrafficNodeMembership.panel_scope == row.panel_scope,
                    BsTrafficNodeMembership.state == 'tail',
                    BsTrafficNodeMembership.source_paid_end_at > now,
                )
            )
        )
        .scalars()
        .all()
    )
    members, nodes = [], set()
    for item in page:
        try:
            if item.enrollment_id is not None:
                await validate_pending_member(db, reader, item)
            policy = _read_policy(item.access_policy)
        except (LedgerConflict, ValueError, TypeError):
            row.error_count += 1
            continue
        selected = sorted(
            {
                node
                for group in ('ordinary', 'bs')
                if policy[f'{group}_mode'] == 'finite'
                for node in policy['topology'][f'{group}_nodes']
            }
        )
        selected = sorted(
            set(selected)
            | {
                tail.node_uuid
                for tail in tail_members
                if tail.subscription_id == item.subscription_id
                and tail.account_generation == item.account_generation
                and tail.panel_user_id == item.panel_user_id
            }
        )
        nodes.update(selected)
        members.append(
            dict(
                round_id=row.round_id,
                subscription_id=item.subscription_id,
                user_id=item.user_id,
                panel_user_id=item.panel_user_id,
                account_generation=item.account_generation,
                nodes=selected,
            )
        )
    if members:
        await db.execute(insert(Member).values(members).on_conflict_do_nothing())
    if nodes:
        await db.execute(
            insert(Node)
            .values([dict(round_id=row.round_id, node_uuid=node) for node in sorted(nodes)])
            .on_conflict_do_nothing()
        )
    row.cursor = page[-1].subscription_id if page else row.upper_id
    more = len(records) > limit
    if not more:
        row.phase = 'collecting'
    await db.flush()
    return PlannedPage(len(page), more)


async def enqueue_node(db, reader):
    row, now = await _round(db, reader)
    if row.phase != 'collecting':
        return False
    node = await db.scalar(
        select(Node)
        .where(Node.round_id == row.round_id, Node.job_id.is_(None))
        .order_by(Node.node_uuid)
        .limit(1)
        .with_for_update()
    )
    if node is not None:
        members = (
            await db.execute(
                select(Member.panel_user_id, Member.account_generation)
                .where(Member.round_id == row.round_id, cast(Member.nodes, JSONB).contains([node.node_uuid]))
                .order_by(Member.panel_user_id)
                .limit(50001)
            )
        ).all()
        if len(members) > 50000:
            row.error_count += 1
            row.phase = 'attention'
            return False  # Splitting identity maps cannot shrink a full-node reply.
        node.job_id = await enqueue_job(
            db,
            operation_key=f'fleet:{row.round_id}:{node.node_uuid}',
            panel_scope=row.panel_scope,
            day=row.day,
            node_uuids=(node.node_uuid,),
            identities={item.panel_user_id: item.account_generation for item in members},
        )
        return True
    jobs = list(
        await db.scalars(select(Job.state).join(Node, Node.job_id == Job.id).where(Node.round_id == row.round_id))
    )
    if any(state in ('pending', 'leased') for state in jobs):
        return False
    failed = any(state != 'complete' for state in jobs)
    row.error_count += int(failed)
    # Even failed nodes must not prevent independent local expiry/delivery work.
    statement = insert(Work).from_select(
        ['subscription_id', 'panel_scope', 'account_generation', 'target_round', 'due_at', 'access_dirty'],
        select(
            Member.subscription_id,
            literal(row.panel_scope),
            Member.account_generation,
            literal(row.round_id),
            literal(now),
            literal(True),
        ).where(
            Member.round_id == row.round_id,
            Member.subscription_id.in_(
                activated_owner_query().with_only_columns(Ledger.subscription_id, maintain_column_froms=True)
            ),
        ),
    )
    await db.execute(
        statement.on_conflict_do_update(
            index_elements=['subscription_id'],
            set_={
                'panel_scope': statement.excluded.panel_scope,
                'target_round': statement.excluded.target_round,
                'due_at': now,
            },
        )
    )
    row.phase = 'settling'
    return True


async def finish_round(db, scope):
    row = await db.get(Round, scope, with_for_update=True, populate_existing=True)
    if row is None or row.phase != 'settling':
        return False
    remaining = await db.scalar(
        select(Work.subscription_id)
        .where(
            Work.panel_scope == scope,
            Work.target_round == row.round_id,
            (Work.completed_round.is_(None) | (Work.completed_round != row.round_id)),
        )
        .limit(1)
    )
    if remaining is not None:
        return False
    errors = await db.scalar(
        select(func.count())
        .select_from(Work)
        .where(
            Work.panel_scope == scope, Work.target_round == row.round_id, (Work.error.is_not(None) | Work.access_dirty)
        )
    )
    now = await db.scalar(select(func.clock_timestamp()))
    row.phase, row.completed_at = 'complete', now
    row.error_count += errors
    if row.error_count == 0 and row.day == now.astimezone(UTC).date():
        row.last_success_at = row.started_at
    return True
