"""Persist raw deltas as pending evidence; never infer a consumption interval.

Attachment is an explicit enrollment baseline, not a recurring daily schedule.
Caller must validate the node/group topology and account ownership beforehand.
Day rollover and reconfiguration require their own enrollment coordinator; an
absent cursor or missing panel row never silently becomes a zero baseline.
"""

from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID

from sqlalchemy import or_, select

from app.config import settings
from app.database.models import (
    BsTrafficAllowance as Grant,
    BsTrafficObservation as Observation,
    BsTrafficPoll,
    BsTrafficUsageCursor as Cursor,
    GraceAccessSessionModel,
    Subscription,
)
from app.services.bs_traffic.ledger import (
    LedgerConflict,
    _group,
    _lock,
    _require_no_pending_transfer,
    grant,
    open_usage,
    resolve_usage,
)


def _key(node_uuid, day):
    if type(day) is not date or not isinstance(node_uuid, str):
        raise LedgerConflict('Explicit node and UTC day are required')
    try:
        return str(UUID(node_uuid))
    except ValueError:
        raise LedgerConflict('Invalid node identity') from None


async def _observation(db, state, node, day):
    row = await db.scalar(
        select(Observation)
        .where(
            Observation.panel_scope == state.panel_scope,
            Observation.panel_user_id == state.panel_user_id,
            Observation.account_generation == state.account_generation,
            Observation.node_uuid == node,
            Observation.day == day,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise LedgerConflict('Missing raw observation is not a zero counter')
    return row


async def _cursor(db, subscription_id, node, day):
    return await db.scalar(
        select(Cursor)
        .where(
            Cursor.subscription_id == subscription_id,
            Cursor.node_uuid == node,
            Cursor.day == day,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )


async def _source_poll(db, state, raw, node, day):
    poll = await db.get(BsTrafficPoll, raw.latest_poll_id)
    if (
        poll is None
        or poll.panel_scope != state.panel_scope
        or poll.day != day
        or not poll.response_hash
        or node not in (poll.node_uuids or [])
    ):
        raise LedgerConflict('Reported usage has no completed source poll')
    return poll


async def _purchase_between_samples(db, cursor, started_at):
    if cursor.last_poll_started_at is None:
        return True
    return (
        await db.scalar(
            select(Grant.id)
            .where(
                Grant.subscription_id == cursor.subscription_id,
                Grant.traffic_group == cursor.traffic_group,
                Grant.starts_at > cursor.last_poll_started_at,
                Grant.starts_at <= started_at,
            )
            .limit(1)
        )
        is not None
    )


async def attach_cursor(db, *, user_id, subscription_id, account_generation, group, node_uuid, day):
    """Preserve the first explicit baseline; retries cannot skip later usage."""
    _group(group)
    node = _key(node_uuid, day)
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    old = await _cursor(db, subscription_id, node, day)
    if old is not None:
        if old.account_generation != account_generation or old.traffic_group != group:
            raise LedgerConflict('Cursor group or account changed; migration required')
        if old.ownership_cycle == state.ownership_cycle:
            return
        from app.database.models import BsTrafficOwnershipCycle

        archived = await db.get(BsTrafficOwnershipCycle, (subscription_id, old.ownership_cycle))
        if archived is None or archived.state != 'managed' or not archived.cursor_archive:
            raise LedgerConflict('Old cursor must be archived before reattachment')
    raw = await _observation(db, state, node, day)
    if raw.needs_review:
        raise LedgerConflict('Cannot enroll an ambiguous counter baseline')
    poll = await _source_poll(db, state, raw, node, day)
    if state.ownership_started_at is not None and poll.created_at < state.ownership_started_at:
        raise LedgerConflict('New cycle requires a fresh explicit raw baseline')
    replacement = Cursor(
        subscription_id=subscription_id,
        ownership_cycle=state.ownership_cycle,
        node_uuid=node,
        day=day,
        account_generation=account_generation,
        traffic_group=group,
        baseline_bytes=raw.high_water_bytes,
        processed_bytes=raw.high_water_bytes,
        needs_review=False,
        last_poll_started_at=poll.created_at,
    )
    if old is None:
        db.add(replacement)
    else:
        for column in Cursor.__table__.columns:
            setattr(old, column.name, getattr(replacement, column.name))
    await db.flush()


async def stage_observation(db, *, user_id, subscription_id, account_generation, node_uuid, day):
    """Advance a cursor and record its delta in the same caller-owned transaction."""
    node = _key(node_uuid, day)
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    cursor = await _cursor(db, subscription_id, node, day)
    if cursor is None or cursor.account_generation != account_generation:
        raise LedgerConflict('Missing or obsolete enrollment cursor')
    raw = await _observation(db, state, node, day)
    poll = await _source_poll(db, state, raw, node, day)
    if cursor.last_poll_started_at is not None and poll.created_at < cursor.last_poll_started_at:
        raise LedgerConflict('Source poll moved behind the settlement cursor')
    if raw.high_water_bytes < cursor.processed_bytes:
        raise LedgerConflict('Recorded evidence moved behind the settlement cursor')
    cursor.last_poll_started_at = poll.created_at
    review_changed = raw.needs_review and not cursor.needs_review
    cursor.needs_review = cursor.needs_review or raw.needs_review
    if raw.high_water_bytes == cursor.processed_bytes:
        # Review changes access from confirmed to unknown even with zero delta.
        # Positive deltas advance the revision through open_usage below.
        if review_changed:
            state.revision += 1
        await db.flush()
        return None
    # This is the panel history bucket, NOT the time the bytes were consumed.
    # raw_pending is forbidden from being charged by both service and DB.
    start = datetime.combine(day, time.min, tzinfo=UTC)
    event_id = await open_usage(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        group=cursor.traffic_group,
        source_key=f'raw:{node}:{day.isoformat()}:{raw.high_water_bytes}',
        amount_bytes=raw.high_water_bytes - cursor.processed_bytes,
        interval_start=start,
        interval_end=start + timedelta(days=1),
        evidence_kind='raw_pending',
    )
    from app.database.models import BsTrafficUsage

    (await db.get(BsTrafficUsage, event_id)).ownership_cycle = cursor.ownership_cycle
    cursor.processed_bytes = raw.high_water_bytes
    await db.flush()
    return event_id


async def _grace_overlaps_bucket(db, subscription_id, day):
    """A daily sum cannot distinguish grace from paid bytes within that day."""
    if await db.scalar(select(Subscription.grace_session_open).where(Subscription.id == subscription_id)):
        return True
    start = datetime.combine(day, time.min, tzinfo=UTC)
    return (
        await db.scalar(
            select(GraceAccessSessionModel.id)
            .where(
                GraceAccessSessionModel.subscription_id == subscription_id,
                GraceAccessSessionModel.started_at < start + timedelta(days=1),
                or_(
                    GraceAccessSessionModel.completed_at.is_(None),
                    GraceAccessSessionModel.completed_at > start,
                ),
            )
            .limit(1)
        )
        is not None
    )


async def settle_reported_observation(db, *, user_id, subscription_id, account_generation, node_uuid, day):
    """Charge a reported delta, not an invented exact consumption interval.

    Eligibility uses the request start: purchases after that cannot fund it.
    Excess is recorded without future debt, unlike carrying cumulative overflow
    into a later top-up. Historical days are retained as raw pending evidence
    (or explicitly opted-in losses), never charged at today's sampling time.
    Buckets overlapping grace remain raw, even after restoration. This is a
    conservative whole-day fence, not precise transition attribution. A known
    purchase between consumed samples also keeps the delta raw. This is not a
    provider flush guarantee: later polls may still contain delayed old bytes.
    Caller owns commit.
    Raw staging and this consumer share a cursor and must not run in parallel
    as competing policies. Existing pending raw records are not reclassified.
    """
    node = _key(node_uuid, day)
    async with db.begin_nested():
        state, _ = await _lock(db, user_id, subscription_id, account_generation)
        cursor = await _cursor(db, subscription_id, node, day)
        if cursor is None or cursor.account_generation != account_generation:
            raise LedgerConflict('Missing or obsolete enrollment cursor')
        raw = await _observation(db, state, node, day)
        if raw.needs_review or cursor.needs_review or raw.high_water_bytes < cursor.processed_bytes:
            raise LedgerConflict('Counter requires reconciliation before reported settlement')
        poll = await _source_poll(db, state, raw, node, day)
        request_day = poll.created_at.astimezone(UTC).date()
        if request_day < day:
            raise LedgerConflict('Future-day usage cannot be settled')
        if cursor.last_poll_started_at is not None and poll.created_at < cursor.last_poll_started_at:
            raise LedgerConflict('Source poll moved behind the settlement cursor')
        if raw.high_water_bytes == cursor.processed_bytes:
            cursor.last_poll_started_at = poll.created_at
            await db.flush()
            return None
        target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
        first_omitted_delta = (
            cursor.bootstrap_evidence is not None and cursor.bootstrap_evidence.get('first_observed_poll_id') is None
        )
        if first_omitted_delta:
            cursor.bootstrap_evidence = dict(
                cursor.bootstrap_evidence, first_observed_day=day.isoformat(), first_observed_poll_id=poll.id
            )
            # Nested raw staging refreshes this row with populate_existing.
            # Persist provenance inside this same transaction before that read.
            await db.flush()
        if (
            first_omitted_delta
            or request_day > day
            or cursor.ownership_cycle != state.ownership_cycle
            or (
                state.ownership_started_at is not None
                and (cursor.last_poll_started_at is None or cursor.last_poll_started_at < state.ownership_started_at)
            )
            or await _grace_overlaps_bucket(db, subscription_id, day)
            or await _purchase_between_samples(db, cursor, poll.created_at)
        ):
            # Request time proves neither day/grace attribution nor which
            # side of a known purchase boundary consumed these bytes.
            # Keep the delta and advance the cursor atomically without choosing
            # a new-period allowance to pay for it.
            event_id = await stage_observation(db, **target, node_uuid=node, day=day)
            if settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is True:
                await resolve_usage(db, **target, usage_id=event_id, decision='write_off')
            return event_id
        event_id = await open_usage(
            db,
            **target,
            group=cursor.traffic_group,
            source_key=f'reported:{node}:{day.isoformat()}:{raw.high_water_bytes}',
            amount_bytes=raw.high_water_bytes - cursor.processed_bytes,
            interval_start=poll.created_at,
            interval_end=poll.created_at + timedelta(microseconds=1),
            evidence_kind='reported',
        )
        await resolve_usage(db, **target, usage_id=event_id, decision='charge')
        cursor.processed_bytes = raw.high_water_bytes
        cursor.last_poll_started_at = poll.created_at
        await db.flush()
        return event_id


async def grant_with_reported_settlement(
    db, *, user_id, subscription_id, account_generation, group, operation_key, amount_bytes, starts_at, expires_at
):
    """Consume known group usage BEFORE issuing a paid allowance, atomically.

    No network or commit: payment fulfillment must share the caller transaction.
    Replays validate the original terms without doing fresh accounting work.
    A bounded snapshot avoids unbounded work while holding the owner's lock.
    This does not compensate provider data which has not arrived yet.
    """
    _group(group)
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    terms = dict(
        group=group,
        operation_key=operation_key,
        amount_bytes=amount_bytes,
        starts_at=starts_at,
        expires_at=expires_at,
    )
    async with db.begin_nested():
        state, _ = await _lock(db, **target)
        existing = await db.scalar(
            select(Grant.id).where(
                Grant.subscription_id == subscription_id,
                Grant.traffic_group == group,
                Grant.operation_key == operation_key,
            )
        )
        if existing is not None:
            return await grant(db, **target, **terms)
        await _require_no_pending_transfer(db, subscription_id)
        pending = (
            await db.execute(
                select(Cursor.node_uuid, Cursor.day)
                .join(
                    Observation,
                    (Observation.panel_scope == state.panel_scope)
                    & (Observation.panel_user_id == state.panel_user_id)
                    & (Observation.account_generation == Cursor.account_generation)
                    & (Observation.node_uuid == Cursor.node_uuid)
                    & (Observation.day == Cursor.day),
                )
                .where(
                    Cursor.subscription_id == subscription_id,
                    Cursor.account_generation == account_generation,
                    Cursor.traffic_group == group,
                    Observation.high_water_bytes > Cursor.processed_bytes,
                )
                .order_by(Cursor.day, Cursor.node_uuid)
                .limit(201)
            )
        ).all()
        if len(pending) > 200:
            raise LedgerConflict('Known usage backlog must be processed before purchase')
        for node_uuid, day in pending:
            await settle_reported_observation(db, **target, node_uuid=node_uuid, day=day)
        return await grant(db, **target, **terms)
