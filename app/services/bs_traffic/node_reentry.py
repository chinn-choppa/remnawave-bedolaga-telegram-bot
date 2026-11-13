"""Freeze node history at payment; adopt fresh epochs only at real cutover."""

import hashlib
import json

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficEnrollment as Enrollment,
    BsTrafficLedger as Ledger,
    BsTrafficNodeMembership as Membership,
    BsTrafficObservation as Raw,
    BsTrafficOwnershipCycle as Cycle,
    BsTrafficPoll as Poll,
    BsTrafficUsage as Usage,
    BsTrafficUsageCursor as Cursor,
    Subscription,
)
from app.services.bs_traffic.ledger import LedgerConflict


async def _history(db, subscription_id, maximum=None):
    rows = list(
        await db.scalars(
            select(Membership)
            .where(
                Membership.subscription_id == subscription_id,
                *([Membership.id <= maximum] if maximum is not None else []),
            )
            .order_by(Membership.id)
            .limit(2001)
        )
    )
    if len(rows) > 2000:
        raise LedgerConflict('Node epoch history budget exceeded')
    values = [
        [
            row.id,
            row.panel_scope,
            row.panel_user_id,
            row.account_generation,
            row.ownership_cycle,
            row.node_uuid,
            row.traffic_group,
            row.source_fingerprint,
            row.effective_at.isoformat(),
        ]
        for row in rows
    ]
    return rows, hashlib.sha256(json.dumps(values, separators=(',', ':')).encode()).hexdigest()


async def freeze_history(db, *, subscription_id):
    """No network: a paid operation freezes bounded historical node identities."""
    rows, digest = await _history(db, subscription_id)
    if not rows:
        return None
    ledger = await db.get(Ledger, subscription_id)
    cycle = await db.get(Cycle, (subscription_id, ledger.ownership_cycle)) if ledger is not None else None
    if cycle is None or cycle.state != 'managed' or (cycle.reverse_attempt or {}).get('state') != 'verified':
        raise LedgerConflict('Historical node re-entry requires verified ownership return')
    return dict(version=1, maximum_id=rows[-1].id, count=len(rows), digest=digest)


async def adopt_enrollment_epochs(db, *, keys, policy, day):
    """Fresh cutover cursors, never a funding-time guessed topology or zero."""
    state = await db.get(Ledger, keys['subscription_id'])
    item = await db.scalar(
        select(Enrollment)
        .where(
            Enrollment.subscription_id == keys['subscription_id'],
            Enrollment.ownership_cycle == state.ownership_cycle,
            Enrollment.state.in_(('provisioned', 'transferred', 'cutover')),
        )
        .order_by(Enrollment.id.desc())
        .limit(1)
        .with_for_update()
    )
    history = item.terms.get('node_history') if item is not None else None
    if history is None:
        return
    if (
        not isinstance(history, dict)
        or set(history) != {'version', 'maximum_id', 'count', 'digest'}
        or type(history['version']) is not int
        or history['version'] != 1
        or type(history['maximum_id']) is not int
        or history['maximum_id'] <= 0
        or type(history['count']) is not int
        or not 1 <= history['count'] <= 2000
        or not isinstance(history['digest'], str)
        or len(history['digest']) != 64
    ):
        raise LedgerConflict('Invalid enrollment node history')
    rows, digest = await _history(db, state.subscription_id, history['maximum_id'])
    if len(rows) != history['count'] or digest != history['digest']:
        raise LedgerConflict('Enrollment node history changed')
    now = await db.scalar(select(func.clock_timestamp()))
    sub = await db.get(Subscription, state.subscription_id)
    current = list(
        await db.scalars(
            select(Membership)
            .where(
                Membership.subscription_id == state.subscription_id,
                Membership.state == 'active',
            )
            .with_for_update()
        )
    )
    for row in current:
        if row.ownership_cycle != state.ownership_cycle:
            row.state, row.retired_at = 'archived', now
    await db.flush()
    existing = {row.node_uuid: row for row in current if row.ownership_cycle == state.ownership_cycle}
    target_nodes = {node: group for group in ('ordinary', 'bs') for node in policy['topology'][f'{group}_nodes']}
    if len(target_nodes) > 200 or len(rows) + len(target_nodes) > 2000:
        raise LedgerConflict('Node epoch history budget exceeded')
    for node, group in target_nodes.items():
        if node in existing:
            row = existing[node]
            if (row.traffic_group, row.panel_scope, row.panel_user_id, row.account_generation) != (
                group,
                state.panel_scope,
                state.panel_user_id,
                state.account_generation,
            ):
                raise LedgerConflict('Enrollment epoch ownership changed')
            continue
        proof = None
        if policy[f'{group}_mode'] == 'finite':
            cursor = await db.get(Cursor, (state.subscription_id, node, day))
            if cursor is None or cursor.ownership_cycle != state.ownership_cycle or cursor.traffic_group != group:
                raise LedgerConflict('Fresh enrollment cursor required')
            poll = (
                await db.get(Poll, (cursor.bootstrap_evidence or {}).get('poll_id'))
                if cursor.last_poll_started_at is None
                else None
            )
            proof = dict(
                kind='enrollment',
                enrollment_id=item.id,
                day=day.isoformat(),
                baseline_bytes=cursor.baseline_bytes,
                sample_started_at=cursor.last_poll_started_at.isoformat() if cursor.last_poll_started_at else None,
                omitted_poll_id=poll.id if poll is not None else None,
                previous_cycle=state.ownership_cycle - 1,
            )
            if cursor.bootstrap_evidence is None:
                cursor.bootstrap_evidence = dict(
                    version=1, kind='topology', first_observed_poll_id=None, first_observed_day=None
                )
        db.add(
            Membership(
                subscription_id=state.subscription_id,
                panel_scope=state.panel_scope,
                panel_user_id=state.panel_user_id,
                account_generation=state.account_generation,
                ownership_cycle=state.ownership_cycle,
                node_uuid=node,
                traffic_group=group,
                source_fingerprint=policy['fingerprint'],
                effective_at=now,
                source_paid_end_at=sub.end_date,
                state='active',
                entry_evidence=proof,
            )
        )
    await db.flush()


async def stage_enrollment_history(db, *, keys, item):
    """Preserve late prior-cycle evidence BEFORE the cutover revision is frozen."""
    if 'node_history' not in item.terms:
        return True
    from datetime import UTC

    from app.config import settings
    from app.services.bs_traffic.intake import stage_observation
    from app.services.bs_traffic.ledger import resolve_usage

    state = await db.get(Ledger, keys['subscription_id'])
    now = await db.scalar(select(func.clock_timestamp()))
    day = now.astimezone(UTC).date()
    rows = list(
        await db.scalars(
            select(Cursor)
            .join(
                Raw,
                (Raw.panel_scope == state.panel_scope)
                & (Raw.panel_user_id == state.panel_user_id)
                & (Raw.account_generation == Cursor.account_generation)
                & (Raw.node_uuid == Cursor.node_uuid)
                & (Raw.day == Cursor.day),
            )
            .where(
                Cursor.subscription_id == state.subscription_id,
                Cursor.account_generation == state.account_generation,
                Cursor.ownership_cycle < state.ownership_cycle,
                Cursor.day == day,
                Raw.high_water_bytes > Cursor.processed_bytes,
            )
            .order_by(Cursor.node_uuid)
            .limit(201)
        )
    )
    complete = len(rows) <= 200
    for cursor in rows[:200]:
        await stage_observation(db, **keys, node_uuid=cursor.node_uuid, day=day)
    if settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is True:
        pending = list(
            await db.scalars(
                select(Usage.id)
                .where(
                    Usage.subscription_id == state.subscription_id,
                    Usage.account_generation == state.account_generation,
                    Usage.ownership_cycle < state.ownership_cycle,
                    Usage.state == 'pending',
                    Usage.evidence_kind == 'raw_pending',
                )
                .order_by(Usage.id)
                .limit(201)
            )
        )
        complete = complete and len(pending) <= 200
        for identifier in pending[:200]:
            await resolve_usage(db, **keys, usage_id=identifier, decision='write_off')
    await db.flush()
    return complete
