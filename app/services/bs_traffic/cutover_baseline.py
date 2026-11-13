"""Check finite-group intake coverage before activation, without creating baselines.

A fresh poll is NOT a flush watermark or proof of consumption time. This guard
only proves that the configured nodes have explicit, current intake positions
and no unprocessed delta. Late raw usage must still remain unchargeable until
classified or explicitly written off under the operator's policy.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from sqlalchemy import func, select

from app.database.models import (
    BsTrafficObservation as Observation,
    BsTrafficPoll as Poll,
    BsTrafficUsageCursor as Cursor,
)
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_cutover import _pending_snapshot
from app.services.bs_traffic.policy import _topology
from app.services.bs_traffic.topology import SplitTopology


BASELINE_MAX_AGE_SECONDS = 30


@dataclass(frozen=True)
class CutoverBaseline:
    fingerprint: str
    local_guard_hash: str
    node_count: int
    valid_until: datetime


async def inspect_cutover_baseline(db, reader, *, user_id, subscription_id, account_generation):
    """Caller owns the transaction; recheck after HTTP before activation.

    One bounded query for at most 200 configured nodes. No per-user/node HTTP,
    implicit enrollment, zero substitution, settlement or write-off.
    """
    snapshot = await _pending_snapshot(
        db, reader, user_id=user_id, subscription_id=subscription_id, account_generation=account_generation
    )
    policy = snapshot['policy']
    topology = _topology(SplitTopology(**policy['topology']))
    expected = {
        node: group
        for group in ('ordinary', 'bs')
        if policy[f'{group}_mode'] == 'finite'
        for node in topology[f'{group}_nodes']
    }
    now = await db.scalar(select(func.clock_timestamp()))
    day = now.astimezone(UTC).date()
    deadline = min(snapshot['valid_until'], datetime.combine(day + timedelta(days=1), time.min, tzinfo=UTC))
    rows = []
    if expected:
        rows = (
            await db.execute(
                select(
                    Cursor.node_uuid,
                    Cursor.traffic_group,
                    Cursor.account_generation,
                    Cursor.baseline_bytes,
                    Cursor.processed_bytes,
                    Cursor.needs_review,
                    Observation.high_water_bytes,
                    Observation.latest_bytes,
                    Observation.needs_review.label('raw_review'),
                    Observation.latest_poll_id,
                    Poll.panel_scope,
                    Poll.day,
                    Poll.node_uuids,
                    Poll.response_hash,
                    Poll.created_at,
                )
                .select_from(Cursor)
                .join(
                    Observation,
                    (Observation.panel_scope == reader.panel_scope)
                    & (Observation.panel_user_id == snapshot['panel_id'])
                    & (Observation.account_generation == account_generation)
                    & (Observation.node_uuid == Cursor.node_uuid)
                    & (Observation.day == Cursor.day),
                )
                .join(Poll, Poll.id == Observation.latest_poll_id)
                .where(
                    Cursor.subscription_id == subscription_id,
                    Cursor.ownership_cycle == snapshot['ownership_cycle'],
                    Cursor.day == day,
                    Cursor.node_uuid.in_(expected),
                )
                .order_by(Cursor.node_uuid)
                .with_for_update(of=(Cursor, Observation))
            )
        ).all()
    values = []
    present = {row.node_uuid for row in rows}
    missing = set(expected) - present
    if missing:
        from app.services.bs_traffic.omitted_baseline import verify_provenance

        omitted = list(
            await db.scalars(
                select(Cursor)
                .where(
                    Cursor.subscription_id == subscription_id,
                    Cursor.ownership_cycle == snapshot['ownership_cycle'],
                    Cursor.day == day,
                    Cursor.node_uuid.in_(missing),
                )
                .with_for_update()
            )
        )
        if len(omitted) != len(missing):
            raise LedgerConflict('Finite traffic nodes need explicit current-day intake baselines')
        for cursor in omitted:
            if cursor.traffic_group != expected[cursor.node_uuid] or cursor.processed_bytes:
                raise LedgerConflict('Omitted baseline group or lower bound changed')
            poll = await verify_provenance(
                db,
                cursor,
                scope=reader.panel_scope,
                panel_id=snapshot['panel_id'],
                generation=account_generation,
                cycle=snapshot['ownership_cycle'],
                now=now,
            )
            # An actual counter, including review/reset data, must never be
            # replaced with an absent-account lower bound.
            if (
                await db.get(
                    Observation, (reader.panel_scope, snapshot['panel_id'], account_generation, cursor.node_uuid, day)
                )
                is not None
            ):
                raise LedgerConflict('Omitted baseline now has unprocessed raw evidence')
            deadline = min(deadline, poll.created_at + timedelta(seconds=BASELINE_MAX_AGE_SECONDS))
            values.append([cursor.node_uuid, cursor.traffic_group, 'omitted', cursor.bootstrap_evidence])
    for row in rows:
        if (
            row.traffic_group != expected[row.node_uuid]
            or row.account_generation != account_generation
            or row.needs_review
            or row.raw_review
            or row.processed_bytes != row.high_water_bytes
            or row.latest_bytes != row.high_water_bytes
            or row.panel_scope != reader.panel_scope
            or row.day != day
            or not isinstance(row.node_uuids, list)
            or row.node_uuid not in row.node_uuids
            or not isinstance(row.response_hash, str)
            or len(row.response_hash) != 64
            or not row.created_at <= now < row.created_at + timedelta(seconds=BASELINE_MAX_AGE_SECONDS)
        ):
            raise LedgerConflict('Cutover intake evidence is stale, unprocessed or inconsistent')
        deadline = min(deadline, row.created_at + timedelta(seconds=BASELINE_MAX_AGE_SECONDS))
        values.append(
            [
                row.node_uuid,
                row.traffic_group,
                row.baseline_bytes,
                row.processed_bytes,
                row.latest_poll_id,
                row.response_hash,
            ]
        )
    fingerprint = hashlib.sha256(
        json.dumps([day.isoformat(), snapshot['guard_hash'], values], separators=(',', ':')).encode()
    ).hexdigest()
    return CutoverBaseline(fingerprint, snapshot['guard_hash'], len(rows), deadline)


async def enroll_cutover_baseline(db, reader, *, user_id, subscription_id, account_generation):
    """Atomically attach explicit observations before the first cutover attempt.

    Caller owns the transaction. Missing rows are never manufactured. Existing
    cursors are immutable here: retrying enrollment cannot discard a new delta.
    Once a panel write may have happened, missing cursors require reconciliation,
    not a new baseline which would hide usage after the global quota was removed.
    """
    from app.database.models import BsTrafficCutoverIntent as Intent

    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with db.begin_nested():
        snapshot = await _pending_snapshot(db, reader, **keys)
        policy = snapshot['policy']
        topology = _topology(SplitTopology(**policy['topology']))
        expected = {
            node: group
            for group in ('ordinary', 'bs')
            if policy[f'{group}_mode'] == 'finite'
            for node in topology[f'{group}_nodes']
        }
        day = (await db.scalar(select(func.clock_timestamp()))).astimezone(UTC).date()
        existing = set(
            await db.scalars(
                select(Cursor.node_uuid).where(
                    Cursor.subscription_id == subscription_id,
                    Cursor.ownership_cycle == snapshot['ownership_cycle'],
                    Cursor.day == day,
                    Cursor.node_uuid.in_(expected),
                )
            )
        )
        missing = set(expected) - existing
        if missing:
            attempted = await db.scalar(
                select(Intent.id)
                .where(
                    Intent.subscription_id == subscription_id,
                    Intent.ownership_cycle == snapshot['ownership_cycle'],
                    Intent.state.in_(('attempted', 'verified', 'activated')),
                )
                .limit(1)
            )
            if attempted is not None:
                raise LedgerConflict('Cannot establish a new baseline after a possible panel write')
            rows = (
                await db.scalars(
                    select(Observation)
                    .where(
                        Observation.panel_scope == reader.panel_scope,
                        Observation.panel_user_id == snapshot['panel_id'],
                        Observation.account_generation == account_generation,
                        Observation.day == day,
                        Observation.node_uuid.in_(missing),
                    )
                    .order_by(Observation.node_uuid)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).all()
            from app.services.bs_traffic.intake import attach_cursor
            from app.services.bs_traffic.omitted_baseline import attach_cutover_omission

            observed = {row.node_uuid for row in rows}
            now = await db.scalar(select(func.clock_timestamp()))
            for node in sorted(missing - observed):
                await attach_cutover_omission(
                    db, reader, keys=keys, snapshot=snapshot, node=node, group=expected[node], day=day, now=now
                )

            for row in rows:
                await attach_cursor(db, **keys, node_uuid=row.node_uuid, day=day, group=expected[row.node_uuid])
            await db.flush()
        from app.services.bs_traffic.node_reentry import adopt_enrollment_epochs

        await adopt_enrollment_epochs(db, keys=keys, policy=policy, day=day)
        # This also validates provenance, freshness, reviews and existing deltas.
        # A failed check rolls back *all* newly attached cursors, even if callers
        # catch the conflict and commit their surrounding transaction.
        return await inspect_cutover_baseline(db, reader, **keys)
