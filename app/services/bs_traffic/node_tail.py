"""Late removed-node evidence never becomes authority to spend new grants."""

from sqlalchemy import Date, cast, exists, func, or_, select
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.orm import aliased

from app.config import settings
from app.database.models import (
    BsTrafficNodeMembership as Membership,
    BsTrafficObservation as Raw,
    BsTrafficUsageCursor as Cursor,
)
from app.services.bs_traffic.intake import _cursor, _observation, _source_poll, stage_observation
from app.services.bs_traffic.ledger import LedgerConflict, resolve_usage


async def stage_tail(db, *, state, target, membership, day):
    if membership.state == 'active' or (
        membership.subscription_id,
        membership.panel_scope,
        membership.panel_user_id,
        membership.account_generation,
    ) != (state.subscription_id, state.panel_scope, state.panel_user_id, state.account_generation):
        raise LedgerConflict('Invalid historical membership owner')
    raw = await _observation(db, state, membership.node_uuid, day)
    poll = await _source_poll(db, state, raw, membership.node_uuid, day)
    cursor = await _cursor(db, state.subscription_id, membership.node_uuid, day)
    if cursor is None:
        # Zero here is the raw intake watermark, never an enrollment baseline
        # or proof of a missing row. The entire first explicit sample is pending.
        cursor = Cursor(
            subscription_id=state.subscription_id,
            node_uuid=membership.node_uuid,
            day=day,
            ownership_cycle=membership.ownership_cycle,
            account_generation=membership.account_generation,
            traffic_group=membership.traffic_group,
            baseline_bytes=0,
            processed_bytes=0,
            needs_review=False,
            last_poll_started_at=poll.created_at,
        )
        db.add(cursor)
        await db.flush()
    if cursor.account_generation != membership.account_generation:
        raise LedgerConflict('Historical cursor account changed')
    identifier = await stage_observation(db, **target, node_uuid=membership.node_uuid, day=day)
    if identifier is not None and settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is True:
        await resolve_usage(db, **target, usage_id=identifier, decision='write_off')
    return identifier


async def settle_tails(db, *, state, target, limit=200):
    active = aliased(Membership)
    rows = (
        await db.execute(
            select(Membership, Raw.day)
            .join(
                Raw,
                (Raw.panel_scope == Membership.panel_scope)
                & (Raw.panel_user_id == Membership.panel_user_id)
                & (Raw.account_generation == Membership.account_generation)
                & (Raw.node_uuid == Membership.node_uuid),
            )
            .outerjoin(
                Cursor,
                (Cursor.subscription_id == Membership.subscription_id)
                & (Cursor.node_uuid == Raw.node_uuid)
                & (Cursor.day == Raw.day),
            )
            .where(
                Membership.subscription_id == state.subscription_id,
                Membership.state != 'active',
                Membership.panel_scope == state.panel_scope,
                Membership.panel_user_id == state.panel_user_id,
                Membership.account_generation == state.account_generation,
                cast(func.timezone('UTC', Membership.effective_at), Date) <= Raw.day,
                ~exists(
                    select(active.id).where(
                        active.subscription_id == Membership.subscription_id,
                        active.node_uuid == Raw.node_uuid,
                        active.state == 'active',
                        active.account_generation == state.account_generation,
                        active.ownership_cycle == state.ownership_cycle,
                        cast(func.timezone('UTC', active.effective_at), Date) <= Raw.day,
                    )
                ),
                or_(
                    Cursor.subscription_id.is_(None),
                    Raw.high_water_bytes > Cursor.processed_bytes,
                    Raw.needs_review != Cursor.needs_review,
                ),
            )
            .ext(distinct_on(Raw.node_uuid, Raw.day))
            .order_by(Raw.node_uuid, Raw.day, Membership.id.desc())
            .limit(limit + 1)
        )
    ).all()
    for membership, day in rows[:limit]:
        await stage_tail(db, state=state, target=target, membership=membership, day=day)
    return len(rows) <= limit
