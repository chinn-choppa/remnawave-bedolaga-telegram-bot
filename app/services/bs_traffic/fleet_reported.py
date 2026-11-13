"""Worker-private reported-delta planning within the caller's locked batch.

Only unambiguous current-day, non-exhausting consumption is accepted. The
returned plans are not authorization outside that transaction and contain no
session/global caches. Complex/pending/boundary cases use the ordinary ledger.
"""

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import Date, DateTime, Integer, String, column, or_, select, tuple_, values

from app.database.models import BsTrafficAllowance as Grant, BsTrafficUsage as Usage, GraceAccessSessionModel as Grace
from app.services.bs_traffic.ledger import eligible_grant_conditions
from app.services.bs_traffic.usage_math import fifo_allocations, usage_request_hash


async def plan_reported_batch(db, rows, grants, now, *, ownership=None, mode_boundaries=None):
    positives = []
    from app.database.models import BsTrafficNodeMembership

    blocked = set(
        await db.scalars(
            select(BsTrafficNodeMembership.subscription_id).where(
                BsTrafficNodeMembership.subscription_id.in_({row[3] for row in rows}),
                BsTrafficNodeMembership.state != 'active',
            )
        )
    )
    for raw, cursor, poll, identifier in rows:
        if cursor is None or raw.high_water_bytes <= cursor.processed_bytes:
            continue
        cycle, boundary = (ownership or {}).get(identifier, (cursor.ownership_cycle, None))
        mode_boundary = (mode_boundaries or {}).get(identifier, {}).get(cursor.traffic_group)
        if mode_boundary is not None and (boundary is None or mode_boundary > boundary):
            boundary = mode_boundary
        if (
            cursor.ownership_cycle != cycle
            or (
                boundary is not None and (cursor.last_poll_started_at is None or cursor.last_poll_started_at < boundary)
            )
            or (
                cursor.bootstrap_evidence is not None
                and cursor.bootstrap_evidence.get('first_observed_poll_id') is None
            )
            or poll is None
            or raw.day != now.date()
            or poll.created_at.date() != raw.day
            or cursor.last_poll_started_at is None
            or poll.created_at < cursor.last_poll_started_at
            or raw.high_water_bytes >= 2**53
            or raw.needs_review
            or cursor.needs_review
        ):
            blocked.add(identifier)
            continue
        if any(
            cursor.last_poll_started_at < grant.starts_at <= poll.created_at
            for grant in grants[identifier]
            if grant.traffic_group == cursor.traffic_group
        ):
            blocked.add(identifier)
            continue
        positives.append((raw, cursor, poll, identifier))
    if not positives:
        return {}, blocked
    if len(positives) > 2000:
        return {}, blocked | {row[3] for row in positives}
    ids = {row[3] for row in positives}
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    blocked.update(
        await db.scalars(
            select(Grace.subscription_id).where(
                Grace.subscription_id.in_(ids),
                Grace.started_at < start + timedelta(days=1),
                or_(Grace.completed_at.is_(None), Grace.completed_at > start),
            )
        )
    )
    samples = (
        values(
            column('subscription_id', Integer),
            column('node', String),
            column('day', Date),
            column('traffic_group', String),
            column('start', DateTime(timezone=True)),
            column('end', DateTime(timezone=True)),
        )
        .data(
            [
                (
                    identifier,
                    raw.node_uuid,
                    raw.day,
                    cursor.traffic_group,
                    poll.created_at,
                    poll.created_at + timedelta(microseconds=1),
                )
                for raw, cursor, poll, identifier in positives
            ]
        )
        .cte('fleet_samples')
    )
    eligible = defaultdict(list)
    associations = (
        await db.execute(
            select(samples.c.subscription_id, samples.c.node, samples.c.day, Grant.id)
            .select_from(samples.join(Grant, Grant.subscription_id == samples.c.subscription_id))
            .where(
                *eligible_grant_conditions(
                    samples.c.subscription_id, samples.c.traffic_group, samples.c.start, samples.c.end
                )
            )
            .order_by(Grant.expires_at, Grant.id)
            .limit(100001)
        )
    ).all()
    if len(associations) > 100000:
        return {}, blocked | ids
    for identifier, node, day, grant_id in associations:
        eligible[identifier, node, day].append(grant_id)
    sources = [
        (identifier, f'reported:{raw.node_uuid}:{raw.day.isoformat()}:{raw.high_water_bytes}')
        for raw, _, _, identifier in positives
    ]
    blocked.update(
        await db.scalars(
            select(Usage.subscription_id).where(tuple_(Usage.subscription_id, Usage.source_key).in_(sources))
        )
    )
    balances = {grant.id: grant.remaining_bytes for items in grants.values() for grant in items}
    result = defaultdict(list)
    for raw, cursor, poll, identifier in sorted(positives, key=lambda row: (row[0].day, row[0].node_uuid)):
        if identifier in blocked:
            continue
        selected = [key for key in eligible[identifier, raw.node_uuid, raw.day] if balances[key] > 0]
        amount = raw.high_water_bytes - cursor.processed_bytes
        if len(selected) > 1000 or sum(balances[key] for key in selected) <= amount:
            # Equality is exhaustion: ordinary worker must update access.
            blocked.add(identifier)
            result.pop(identifier, None)
            continue
        allocations, excess = fifo_allocations([(key, balances[key]) for key in selected], amount)
        if excess:
            blocked.add(identifier)
            result.pop(identifier, None)
            continue
        for allocation in allocations:
            balances[allocation['grant_id']] -= allocation['bytes']
        end = poll.created_at + timedelta(microseconds=1)
        event = Usage(
            subscription_id=identifier,
            traffic_group=cursor.traffic_group,
            source_key=f'reported:{raw.node_uuid}:{raw.day.isoformat()}:{raw.high_water_bytes}',
            request_hash=usage_request_hash(
                cursor.traffic_group, 'reported', cursor.account_generation, amount, poll.created_at, end
            ),
            account_generation=cursor.account_generation,
            evidence_kind='reported',
            interval_start=poll.created_at,
            interval_end=end,
            amount_bytes=amount,
            eligible_grants=list(selected),
            allocations=allocations,
            state='charged',
            charged_bytes=amount,
            unallocated_bytes=0,
            resolved_at=now,
        )
        result[identifier].append((event, cursor, raw.high_water_bytes, poll.created_at))
    return result, blocked
