"""Bounded accounting for proven stable access; ordinary worker owns exceptions.

The batch rechecks activation, identity, paid revision and clocks under payment
locks. Unchanged samples only advance their timestamp. Optional reported-delta
plans share ledger eligibility/hash/FIFO and are committed in this transaction;
exhaustion, pending, grace and ambiguous boundaries take the ordinary path.
"""

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import and_, func, select

from app.database.models import (
    BsTrafficAllowance as Grant,
    BsTrafficBaseExtension as Extension,
    BsTrafficDelivery as Delivery,
    BsTrafficFleetRound as Round,
    BsTrafficFleetWork as Work,
    BsTrafficLedger as Ledger,
    BsTrafficObservation as Raw,
    BsTrafficPoll as Poll,
    BsTrafficRenewalIntent as Renewal,
    BsTrafficUsage as Usage,
    BsTrafficUsageCursor as Cursor,
    GraceAccessSessionModel as Grace,
    Subscription,
    User,
)
from app.services.bs_traffic.ledger import LedgerConflict, activated_owner_query
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.policy import _read_policy, _topology, effective_group_mode


async def settle_idle_batch(factory, reader, cache, *, limit=500, interval_seconds=30, settle_deltas=False):
    if not 1 <= limit <= 500:
        raise ValueError('Stable accounting batch bound exceeded')
    # Network outside business locks; bounded distinct policies share cache.
    async with factory() as db:
        policies = list(
            await db.scalars(
                select(Ledger.access_policy)
                .join(
                    Work,
                    Work.subscription_id == Ledger.subscription_id,
                )
                .where(
                    Work.panel_scope == reader.panel_scope,
                    Work.due_at <= func.clock_timestamp(),
                    Work.error.is_(None),
                    Work.settled_revision.is_not(None),
                    Work.lease_until.is_(None),
                )
                .order_by(Work.due_at, Work.subscription_id)
                .limit(limit)
            )
        )
    evidence = {}
    for value in policies:
        try:
            policy = _read_policy(value)
        except LedgerConflict:
            continue
        if policy['fingerprint'] in evidence:
            continue
        if len(evidence) >= 64:
            break
        item = await cache.get(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=policy['topology']['ordinary_squads'],
            bs_squads=policy['topology']['bs_squads'],
            admission=lambda: panel_slot(factory, reader.panel_scope),
        )
        if item.fingerprint == policy['fingerprint'] and _topology(item.topology) == policy['topology']:
            evidence[item.fingerprint] = item
    if not evidence:
        return 0
    async with factory() as db, db.begin():
        await db.execute(select(func.set_config('lock_timeout', '1000', True)))
        await db.execute(select(func.set_config('statement_timeout', '5000', True)))
        now = await db.scalar(select(func.clock_timestamp()))
        work = list(
            await db.scalars(
                select(Work)
                .where(
                    Work.panel_scope == reader.panel_scope,
                    Work.due_at <= now,
                    Work.lease_until.is_(None),
                    Work.error.is_(None),
                    Work.settled_revision.is_not(None),
                    Work.access_dirty.is_(False),
                    Work.calendar_due_at > now,
                )
                .order_by(Work.due_at, Work.subscription_id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        ids = [w.subscription_id for w in work]
        if not ids:
            return 0
        # Same order as normal worker/payment; all page locks are bounded.
        await db.execute(
            select(User.id)
            .join(Subscription, Subscription.user_id == User.id)
            .where(Subscription.id.in_(ids))
            .order_by(User.id)
            .with_for_update(of=User)
        )
        subscriptions = {
            s.id: s
            for s in await db.scalars(
                select(Subscription).where(Subscription.id.in_(ids)).order_by(Subscription.id).with_for_update()
            )
        }
        ledgers = {
            s.subscription_id: s
            for s in await db.scalars(
                select(Ledger).where(Ledger.subscription_id.in_(ids)).order_by(Ledger.subscription_id).with_for_update()
            )
        }
        active_users = set(
            await db.scalars(
                select(User.id).where(User.id.in_([s.user_id for s in subscriptions.values()]), User.status == 'active')
            )
        )
        activated = set(
            await db.scalars(
                activated_owner_query().with_only_columns(Ledger.subscription_id).where(Ledger.subscription_id.in_(ids))
            )
        )
        blocked = set()
        for model, condition in (
            (Grace, Grace.state != 'completed'),
            (Usage, Usage.state == 'pending'),
            (Delivery, Delivery.state.in_(('pending', 'attempted', 'attention'))),
        ):
            blocked.update(
                await db.scalars(select(model.subscription_id).where(model.subscription_id.in_(ids), condition))
            )
        receipts = dict(
            (
                await db.execute(
                    select(Renewal.subscription_id, func.max(Renewal.id))
                    .where(Renewal.subscription_id.in_(ids), Renewal.transaction_id.is_not(None))
                    .group_by(Renewal.subscription_id)
                )
            ).all()
        )
        grants = defaultdict(list)
        extension_boundaries = defaultdict(list)
        extensions = (
            await db.execute(
                select(Extension.subscription_id, Extension.starts_at, Extension.expires_at)
                .where(Extension.subscription_id.in_(ids))
                .limit(100001)
            )
        ).all()
        if len(extensions) > 100000:
            return 0
        for identifier, start, end in extensions:
            extension_boundaries[identifier].extend((start, end))
        grant_rows = list(
            await db.scalars(
                select(Grant)
                .where(Grant.subscription_id.in_(ids))
                .order_by(Grant.expires_at, Grant.id)
                .limit(100001)
                .with_for_update()
            )
        )
        if len(grant_rows) > 100000:
            return 0
        for grant in grant_rows:
            grants[grant.subscription_id].append(grant)
        # Any outstanding historic observation also excludes this path.
        raw = (
            await db.execute(
                select(Raw, Cursor, Poll, Ledger.subscription_id)
                .join(
                    Ledger,
                    and_(
                        Ledger.panel_scope == Raw.panel_scope,
                        Ledger.panel_user_id == Raw.panel_user_id,
                        Ledger.account_generation == Raw.account_generation,
                    ),
                )
                .outerjoin(
                    Cursor,
                    and_(
                        Cursor.subscription_id == Ledger.subscription_id,
                        Cursor.node_uuid == Raw.node_uuid,
                        Cursor.day == Raw.day,
                    ),
                )
                .outerjoin(Poll, Poll.id == Raw.latest_poll_id)
                .where(Ledger.subscription_id.in_(ids))
                .order_by(Raw.node_uuid, Raw.panel_user_id, Raw.day)
                .limit(100001)
                .with_for_update(of=Raw)
            )
        ).all()
        if len(raw) > 100000:
            return 0
        planned = {}
        if settle_deltas:
            from app.services.bs_traffic.fleet_reported import plan_reported_batch

            planned, rejected = await plan_reported_batch(
                db,
                raw,
                grants,
                now,
                ownership={
                    identifier: (ledger.ownership_cycle, ledger.ownership_started_at)
                    for identifier, ledger in ledgers.items()
                },
            )
            blocked.update(rejected)
        now = await db.scalar(select(func.clock_timestamp()))
        covered = defaultdict(set)
        cursor_updates = defaultdict(list)
        round_row = await db.get(Round, reader.panel_scope)
        if round_row is None or round_row.day != now.date():
            return 0
        for observation, cursor, poll, identifier in raw:
            if (
                cursor is None
                or cursor.account_generation != observation.account_generation
                or cursor.processed_bytes > observation.high_water_bytes
                or (cursor.processed_bytes < observation.high_water_bytes and not settle_deltas)
                or cursor.needs_review
                or observation.needs_review
                or (poll and cursor.last_poll_started_at and poll.created_at < cursor.last_poll_started_at)
            ):
                blocked.add(identifier)
                continue
            if (
                observation.day == now.date()
                and poll
                and poll.response_hash
                and poll.created_at >= round_row.started_at
                and poll.panel_scope == reader.panel_scope
                and poll.day == observation.day
                and observation.node_uuid in poll.node_uuids
            ):
                covered[identifier].add(observation.node_uuid)
                cursor_updates[identifier].append((cursor, poll.created_at))
        count = 0
        for item in work:
            sub, ledger = subscriptions.get(item.subscription_id), ledgers.get(item.subscription_id)
            if (
                sub is None
                or ledger is None
                or item.subscription_id not in activated
                or item.subscription_id in blocked
            ):
                continue
            if (
                sub.user_id not in active_users
                or sub.status not in ('active', 'trial', 'limited')
                or sub.grace_session_open
                or not sub.end_date
                or sub.end_date <= now
                or item.account_generation != ledger.account_generation
                or item.settled_revision != ledger.revision
                or item.settled_at is None
                or item.calendar_due_at is None
                or item.calendar_due_at <= now
                or receipts.get(item.subscription_id, 0) != item.calendar_receipt_id
                or item.target_round != round_row.round_id
            ):
                continue
            if (ledger.restriction_attempt or {}).get('state') == 'attempted':
                continue
            try:
                policy = _read_policy(ledger.access_policy)
            except LedgerConflict:
                continue
            if any(
                effective_group_mode(policy, group, item.settled_at) != effective_group_mode(policy, group, now)
                for group in ('ordinary', 'bs')
            ):
                continue
            proof = evidence.get(policy['fingerprint'])
            if (
                proof is None
                or not proof.is_fresh(now, panel_identity=reader.panel_scope)
                or sub.tariff_id != policy['tariff_id']
            ):
                continue
            nodes = {
                node
                for group in ('ordinary', 'bs')
                if policy[f'{group}_mode'] == 'finite'
                for node in policy['topology'][f'{group}_nodes']
            }
            if not nodes <= covered[item.subscription_id]:
                continue
            if any(
                item.settled_at < moment <= now
                for grant in grants[item.subscription_id]
                for moment in (grant.starts_at, grant.expires_at, grant.retired_at)
                if moment is not None
            ):
                continue
            # The lexical transaction owns all authorization/record locks;
            # these immutable plans are never reusable beyond this transaction.
            if any(item.settled_at < boundary <= now for boundary in extension_boundaries[item.subscription_id]):
                continue
            events = planned.get(item.subscription_id, ())
            if any(
                cursor.node_uuid not in policy['topology'][f'{cursor.traffic_group}_nodes']
                or policy[f'{cursor.traffic_group}_mode'] != 'finite'
                for _, cursor, _, _ in events
            ):
                continue
            grant_map = {grant.id: grant for grant in grants[item.subscription_id]}
            for event, cursor, high_water, sampled_at in events:
                for allocation in event.allocations:
                    grant_map[allocation['grant_id']].remaining_bytes -= allocation['bytes']
                db.add(event)
                cursor.processed_bytes, cursor.last_poll_started_at = high_water, sampled_at
                ledger.revision += 2
            item.settled_revision = ledger.revision
            for cursor, sampled_at in cursor_updates[item.subscription_id]:
                if cursor.node_uuid in nodes:
                    cursor.last_poll_started_at = sampled_at
            item.completed_round, item.settled_at = item.target_round, now
            item.due_at = now + timedelta(seconds=interval_seconds)
            count += 1
        return count
