"""Coalesced local settlement, with live account reads only for dirty access.

Topology evidence is shared, never account identity or authorization. Local
rights and raw observations are locked and rechecked by their existing owners.
"""

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import structlog
from sqlalchemy import exists, func, or_, select, update

from app.database.models import (
    BsTrafficDelivery as Delivery,
    BsTrafficFleetRound as Round,
    BsTrafficFleetWork as Work,
    BsTrafficLedger as Ledger,
    BsTrafficObservation as Raw,
    BsTrafficPoll as Poll,
    BsTrafficRenewalIntent,
    BsTrafficUsageCursor as Cursor,
    GraceAccessSessionModel,
    Subscription,
)
from app.services.bs_traffic.calendar_renewal import refresh_calendar_base
from app.services.bs_traffic.day_rollover import stage_day_rollover
from app.services.bs_traffic.intake import _cursor, settle_reported_observation
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.policy import _read_policy, _topology, project_access
from app.services.bs_traffic.preparation import delivery_guard, prepare_delivery
from app.services.bs_traffic.sender import send_delivery


async def _calendar(db, work, target, now, sub):
    receipt = await db.scalar(
        select(BsTrafficRenewalIntent)
        .where(BsTrafficRenewalIntent.subscription_id == sub.id, BsTrafficRenewalIntent.transaction_id.is_not(None))
        .order_by(BsTrafficRenewalIntent.id.desc())
        .limit(1)
    )
    identifier = receipt.id if receipt else 0
    if work.calendar_receipt_id == identifier and work.calendar_due_at and work.calendar_due_at > now:
        return
    result = await refresh_calendar_base(db, **target, now=now)
    # An older paid BS calendar can still be due after an ordinary-only renewal.
    work.calendar_due_at = result.next_refresh_at or datetime.max.replace(tzinfo=UTC)
    work.calendar_receipt_id = identifier


async def _settle(factory, reader, cache, subscription_id, token):
    # Close the snapshot transaction before shared topology I/O.
    async with factory() as db:
        row = await db.get(Ledger, subscription_id)
        if row is None or row.panel_scope != reader.panel_scope:
            raise LedgerConflict('Fleet owner changed')
        generation, policy = row.account_generation, _read_policy(row.access_policy)
        user_id = await db.scalar(select(Subscription.user_id).where(Subscription.id == subscription_id))
        from app.services.bs_traffic.restriction import _reason, restrict_account

        restriction_due = await _reason(db, subscription_id) is not None or (
            row.restriction_attempt and row.restriction_attempt.get('state') == 'attempted'
        )
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=generation)
    if restriction_due:
        if await restrict_account(reader, **target) != 'verified':
            return False
        async with factory() as db, db.begin():
            work = await db.scalar(
                select(Work).where(Work.subscription_id == subscription_id, Work.claim_token == token).with_for_update()
            )
            if work is None:
                return False
            work.applied_squads, work.access_dirty = [], False
            work.settled_at = await db.scalar(select(func.clock_timestamp()))
        return True
    evidence = await cache.get(
        reader,
        panel_identity=reader.panel_scope,
        ordinary_squads=policy['topology']['ordinary_squads'],
        bs_squads=policy['topology']['bs_squads'],
        admission=lambda: panel_slot(factory, reader.panel_scope),
    )
    async with factory() as db, db.begin():
        await db.execute(select(func.set_config('lock_timeout', '1000', True)))
        await db.execute(select(func.set_config('statement_timeout', '5000', True)))
        work = await db.scalar(
            select(Work).where(Work.subscription_id == subscription_id, Work.claim_token == token).with_for_update()
        )
        if work is None:
            return False
        state, _ = await _lock(db, **target)
        await require_activated_owner(db, subscription_id)
        now = await db.scalar(select(func.clock_timestamp()))
        if (
            state.panel_scope != reader.panel_scope
            or _read_policy(state.access_policy) != policy
            or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
        ):
            raise LedgerConflict('Fleet snapshot changed')
        if evidence.fingerprint != policy['fingerprint'] or _topology(evidence.topology) != policy['topology']:
            raise LedgerConflict('Fleet topology changed')
        sub = await db.get(Subscription, subscription_id)
        grace = sub.grace_session_open or bool(
            await db.scalar(
                select(
                    exists().where(
                        GraceAccessSessionModel.subscription_id == subscription_id,
                        GraceAccessSessionModel.state != 'completed',
                    )
                )
            )
        )
        await _calendar(db, work, target, now, sub)
        if work.account_generation != generation:
            work.account_generation, work.applied_squads, work.access_dirty = generation, None, True
        nodes = {
            node
            for group in ('ordinary', 'bs')
            if policy[f'{group}_mode'] == 'finite'
            for node in policy['topology'][f'{group}_nodes']
        }
        if len(nodes) > 200:
            raise LedgerConflict('Settlement node budget exceeded')
        # Consume unprocessed historical evidence before today's rollover. Missing
        # observations never become synthetic zeros, and historical bytes remain
        # governed by the existing pending/write-off policy.
        raw_rows = (
            await db.execute(
                select(Raw.node_uuid, Raw.day)
                .outerjoin(
                    Cursor,
                    (Cursor.subscription_id == subscription_id)
                    & (Cursor.node_uuid == Raw.node_uuid)
                    & (Cursor.day == Raw.day),
                )
                .where(
                    Raw.panel_scope == state.panel_scope,
                    Raw.panel_user_id == state.panel_user_id,
                    Raw.account_generation == generation,
                    Raw.node_uuid.in_(nodes),
                    or_(
                        Cursor.subscription_id.is_(None),
                        Raw.high_water_bytes > Cursor.processed_bytes,
                        Raw.needs_review != Cursor.needs_review,
                    ),
                )
                .order_by(Raw.day, Raw.node_uuid)
                .limit(401)
            )
        ).all()
        historical_backlog = len(raw_rows) > 400
        raw_rows = raw_rows[:400]
        today = now.astimezone(UTC).date()
        omitted_coverage = set()
        for node, day in (
            []
            if grace
            else sorted(
                set(raw_rows) | (set() if historical_backlog else {(node, today) for node in nodes}),
                key=lambda pair: (pair[1], pair[0]),
            )
        ):
            if (
                day == today
                and await db.get(Raw, (state.panel_scope, state.panel_user_id, generation, node, day)) is None
            ):
                from app.services.bs_traffic.omitted_baseline import service_omitted_node

                current_round = await db.get(Round, reader.panel_scope)
                group = 'ordinary' if node in policy['topology']['ordinary_nodes'] else 'bs'
                if await service_omitted_node(
                    db,
                    keys=target,
                    state=state,
                    node=node,
                    group=group,
                    day=day,
                    now=now,
                    since=current_round.started_at,
                ):
                    omitted_coverage.add(node)
                    continue
            cursor = await _cursor(db, subscription_id, node, day)
            if cursor is None:
                await stage_day_rollover(db, **target, node_uuid=node, day=day, evidence=evidence)
            else:
                await settle_reported_observation(db, **target, node_uuid=node, day=day)
        if historical_backlog and not grace:
            return False  # Commit this bounded slice; revisit remaining evidence.
        # Every finite node must have a current-day completed sample from this
        # round. Missing users in a full-node response are not healthy coverage.
        round_row = await db.get(Round, reader.panel_scope)
        if round_row.day == today and nodes:
            covered = await db.scalar(
                select(func.count())
                .select_from(Raw)
                .join(Poll, Poll.id == Raw.latest_poll_id)
                .where(
                    Raw.panel_scope == state.panel_scope,
                    Raw.panel_user_id == state.panel_user_id,
                    Raw.account_generation == generation,
                    Raw.day == today,
                    Raw.node_uuid.in_(nodes),
                    Poll.created_at >= round_row.started_at,
                    Poll.response_hash.is_not(None),
                )
            )
            if covered + len(omitted_coverage) != len(nodes):
                raise LedgerConflict('Fleet sample coverage incomplete')
        projection = await project_access(db, **target, evidence=evidence, grace_active=bool(grace))
        from app.services.grace_access_service import GraceReason

        if projection.grace_candidate is GraceReason.LIMITED and not projection.preserve_grace:
            if sub.grace_candidate_reason != GraceReason.LIMITED.value:
                sub.grace_candidate_reason = GraceReason.LIMITED.value
                sub.grace_candidate_at = now
        groups = (projection.ordinary, projection.bs)
        if projection.preserve_grace:
            from app.services.bs_traffic.grace_lifecycle import confirmed_active_overlay

            confirmed = await confirmed_active_overlay(
                db,
                subscription_id=subscription_id,
                account_generation=generation,
                panel_scope=reader.panel_scope,
                now=now,
            )
            work.settled_at = now
            if confirmed:
                work.access_dirty = False
            return confirmed
        if any(group.squads is None for group in groups):
            work.settled_at = now
            return False  # Preserve committed raw/pending evidence, without a panel write.
        desired = sorted({squad for group in groups for squad in group.squads})
        if work.applied_squads is None:
            previous = await db.scalar(
                select(Delivery)
                .where(
                    Delivery.subscription_id == subscription_id,
                    Delivery.state == 'verified',
                    Delivery.guard_hash == delivery_guard(**target, evidence=evidence),
                )
                .order_by(Delivery.id.desc())
                .limit(1)
            )
            if previous:
                work.applied_squads = previous.squads
        work.access_dirty = work.applied_squads != desired
        dirty, target_round, revision = work.access_dirty, work.target_round, state.revision
        work.settled_revision = state.revision
        work.settled_at = now
    if dirty:
        prepared = await prepare_delivery(
            factory,
            reader,
            **target,
            operation_key=f'fleet:{target_round}:{subscription_id}:{revision}:{hashlib.sha256(str(desired).encode()).hexdigest()[:16]}',
            topology_cache=cache,
            settle_usage=False,
        )
        if prepared.status != 'queued':
            raise LedgerConflict('Fleet access delivery deferred')
        status = await send_delivery(
            factory,
            reader._api,
            **target,
            operation_id=prepared.operation_id,
            evidence=prepared.evidence,
            current_guard_hash=prepared.guard_hash,
            bound_reader=reader,
        )
        if status not in ('verified', 'already_verified'):
            raise LedgerConflict('Fleet access delivery unverified')
        async with factory() as db, db.begin():
            await db.execute(
                update(Work)
                .where(Work.subscription_id == subscription_id, Work.claim_token == token)
                .values(applied_squads=desired, access_dirty=False)
            )
        from app.services.bs_traffic.connection_drop import drop_after_delivery

        drop_status = await drop_after_delivery(reader, **target, operation_id=prepared.operation_id)
        if drop_status in ('unknown', 'attempted', 'waiting'):
            # Access has been verified. A best-effort socket drop is a separate
            # diagnostic, not a panel-wide ban on unrelated customers' purchases.
            structlog.get_logger(__name__).warning(
                'Split connection termination remains unconfirmed',
                subscription_id=subscription_id,
                operation_id=prepared.operation_id,
                drop_status=drop_status,
            )
    return True


async def run_local_once(factory, reader, cache, *, interval_seconds=30):
    import asyncio

    async with factory() as db, db.begin():
        now = await db.scalar(select(func.clock_timestamp()))
        work = await db.scalar(
            select(Work)
            .where(
                Work.panel_scope == reader.panel_scope,
                Work.due_at <= now,
                or_(Work.lease_until.is_(None), Work.lease_until <= now),
            )
            .order_by(Work.due_at, Work.subscription_id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if work is None:
            return False
        token, identifier, claimed_round = str(uuid4()), work.subscription_id, work.target_round
        work.claim_token, work.lease_until = token, now + timedelta(seconds=60)
    error = None
    try:
        async with asyncio.timeout(45):
            if not await _settle(factory, reader, cache, identifier, token):
                error = 'settlement_deferred'
    except Exception:
        error = 'settlement_deferred'
    async with factory() as db, db.begin():
        work = await db.scalar(
            select(Work).where(Work.subscription_id == identifier, Work.claim_token == token).with_for_update()
        )
        if work:
            now = await db.scalar(select(func.clock_timestamp()))
            work.error, work.completed_round = error, claimed_round
            work.due_at = (
                now if work.target_round != claimed_round else now + timedelta(seconds=5 if error else interval_seconds)
            )
            work.lease_until, work.claim_token = None, None
    return True
