"""Explicit unobserved lower bounds, never synthetic zero observations.

A successful full-node response may omit an account that has never used it.
The persisted provenance authorizes conservative bootstrap only; its first real
reported delta remains raw pending (or explicitly opted-in write-off).
"""

from datetime import timedelta

from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB

from app.config import settings
from app.database.models import (
    BsTrafficCollectionJob as Job,
    BsTrafficCutoverIntent as Intent,
    BsTrafficFleetMember as Member,
    BsTrafficFleetNode as Node,
    BsTrafficLedger as Ledger,
    BsTrafficManagedTransfer as Transfer,
    BsTrafficObservation as Raw,
    BsTrafficOwnershipCycle as Cycle,
    BsTrafficPoll as Poll,
    BsTrafficUsageCursor as Cursor,
)
from app.services.bs_traffic.ledger import LedgerConflict


async def complete_omission(db, *, scope, subscription_id, panel_id, generation, node, day, now, since=None):
    """Require exact membership and a complete unpaginated aggregate response.

    Official 3.4.8 GROUP BY builds returned nodes only from history rows; an
    entirely absent node is valid sparse output. Its existence/topology is
    checked separately by cutover/settlement evidence. response_nodes=None
    means legacy/unproven parsing, while [] is a successfully parsed empty
    aggregate. Neither is converted into a measured zero usage row.
    """
    existing = await db.get(Raw, (scope, panel_id, generation, node, day))
    if existing is not None:
        return None  # Includes reviewed/reset evidence: never replace it by absence.
    row = await db.scalar(
        select(Poll)
        .join(Job, Job.poll_id == Poll.id)
        .join(Node, Node.job_id == Job.id)
        .join(Member, Member.round_id == Node.round_id)
        .where(
            Member.subscription_id == subscription_id,
            Member.panel_user_id == panel_id,
            Member.account_generation == generation,
            cast(Member.nodes, JSONB).contains([node]),
            Node.node_uuid == node,
            Job.panel_scope == scope,
            Job.day == day,
            Job.state == 'complete',
            Poll.panel_scope == scope,
            Poll.day == day,
            Poll.response_hash.is_not(None),
            cast(Poll.node_uuids, JSONB).contains([node]),
            Poll.response_nodes.is_not(None),
            Poll.created_at <= now,
            Poll.created_at > now - timedelta(seconds=30),
            Poll.created_at >= since if since is not None else True,
        )
        .order_by(Poll.id.desc())
        .limit(1)
    )
    if row is None:
        return None
    return {'poll_id': row.id, 'response_hash': row.response_hash, 'sample_started_at': row.created_at.isoformat()}


async def verify_provenance(db, cursor, *, scope, panel_id, generation, cycle, now, fresh=True):
    value = cursor.bootstrap_evidence
    if (
        settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True
        or not isinstance(value, dict)
        or value.get('version') != 1
        or value.get('kind') != 'omitted'
        or value.get('scope') != scope
        or value.get('panel_id') != panel_id
        or value.get('generation') != generation
        or value.get('cycle') != cycle
        or value.get('node') != cursor.node_uuid
        or value.get('day') != cursor.day.isoformat()
        or cursor.account_generation != generation
        or cursor.ownership_cycle != cycle
        or cursor.needs_review
    ):
        raise LedgerConflict('Omitted baseline provenance changed')
    poll = await db.get(Poll, value.get('poll_id'))
    if (
        poll is None
        or poll.panel_scope != scope
        or poll.day != cursor.day
        or cursor.node_uuid not in poll.node_uuids
        or poll.response_nodes is None
        or poll.response_hash != value.get('response_hash')
        or not poll.response_hash
        or poll.created_at > now
        or (fresh and now >= poll.created_at + timedelta(seconds=30))
    ):
        raise LedgerConflict('Omitted baseline collection evidence is stale or incomplete')
    return poll


async def attach_cutover_omission(db, reader, *, keys, snapshot, node, group, day, now):
    from app.services.bs_traffic.grace_intent import _decode
    from app.services.bs_traffic.identity import account_generation

    if settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
        raise LedgerConflict('Explicit uncertainty write-off policy required for omitted bootstrap')
    ledger = await db.get(Ledger, keys['subscription_id'])
    intent = await db.scalar(
        select(Intent)
        .where(
            Intent.subscription_id == keys['subscription_id'],
            Intent.ownership_cycle == ledger.ownership_cycle,
            Intent.state == 'pending',
            Intent.account_generation == keys['account_generation'],
            Intent.panel_scope == reader.panel_scope,
            Intent.valid_until > now,
        )
        .order_by(Intent.id.desc())
        .limit(1)
    )
    if intent is None:
        raise LedgerConflict('Fresh canonical cutover source required')
    source = _decode(intent.expected_source, intent.source_hash, paid=False)
    if account_generation(panel_scope=reader.panel_scope, account=source) != keys['account_generation']:
        raise LedgerConflict('Canonical omitted baseline identity changed')
    transfer = await db.get(Transfer, keys['subscription_id'])
    if transfer is None or transfer.ownership_cycle != ledger.ownership_cycle:
        raise LedgerConflict('Current transfer receipt required')
    boundary = max(value for value in (transfer.created_at, ledger.ownership_started_at) if value is not None)
    proof = await complete_omission(
        db,
        scope=reader.panel_scope,
        subscription_id=keys['subscription_id'],
        panel_id=snapshot['panel_id'],
        generation=keys['account_generation'],
        node=node,
        day=day,
        now=now,
        since=boundary,
    )
    if proof is None:
        raise LedgerConflict('Completed exact node membership required for omitted baseline')
    old = await db.get(Cursor, (keys['subscription_id'], node, day), with_for_update=True)
    if old is not None:
        if old.ownership_cycle == ledger.ownership_cycle:
            return old
        archived = await db.get(Cycle, (keys['subscription_id'], old.ownership_cycle))
        if archived is None or archived.state != 'managed' or not archived.cursor_archive:
            raise LedgerConflict('Previous cursor must be archived before omitted reattachment')
    value = dict(
        proof,
        version=1,
        kind='omitted',
        scope=reader.panel_scope,
        panel_id=snapshot['panel_id'],
        generation=keys['account_generation'],
        cycle=ledger.ownership_cycle,
        node=node,
        day=day.isoformat(),
        source_cutover_id=intent.id,
        source_hash=intent.source_hash,
        first_observed_day=None,
        first_observed_poll_id=None,
    )
    cursor = old or Cursor(subscription_id=keys['subscription_id'], node_uuid=node, day=day)
    cursor.account_generation, cursor.ownership_cycle = keys['account_generation'], ledger.ownership_cycle
    cursor.traffic_group, cursor.baseline_bytes, cursor.processed_bytes = group, 0, 0
    cursor.needs_review, cursor.bootstrap_evidence = False, value
    cursor.last_poll_started_at = None
    if old is None:
        db.add(cursor)
    await db.flush()
    return cursor


async def service_omitted_node(db, *, keys, state, node, group, day, now, since):
    """Continue known unobserved coverage while a node omits the account."""
    proof = await complete_omission(
        db,
        scope=state.panel_scope,
        subscription_id=keys['subscription_id'],
        panel_id=state.panel_user_id,
        generation=keys['account_generation'],
        node=node,
        day=day,
        now=now,
        since=since,
    )
    if proof is None:
        return False
    cursor = await db.get(Cursor, (keys['subscription_id'], node, day), with_for_update=True)
    if cursor is None:
        prior = await db.scalar(
            select(Cursor)
            .where(
                Cursor.subscription_id == keys['subscription_id'],
                Cursor.node_uuid == node,
                Cursor.day < day,
                Cursor.account_generation == keys['account_generation'],
                Cursor.ownership_cycle == state.ownership_cycle,
            )
            .order_by(Cursor.day.desc())
            .limit(1)
            .with_for_update()
        )
        if prior is None or not prior.bootstrap_evidence:
            return False
        await verify_provenance(
            db,
            prior,
            scope=state.panel_scope,
            panel_id=state.panel_user_id,
            generation=keys['account_generation'],
            cycle=state.ownership_cycle,
            now=now,
            fresh=False,
        )
        cursor = Cursor(
            subscription_id=keys['subscription_id'],
            node_uuid=node,
            day=day,
            account_generation=keys['account_generation'],
            ownership_cycle=state.ownership_cycle,
            traffic_group=group,
            baseline_bytes=0,
            processed_bytes=0,
            needs_review=False,
            bootstrap_evidence=dict(prior.bootstrap_evidence, **proof, day=day.isoformat()),
        )
        db.add(cursor)
    else:
        if not cursor.bootstrap_evidence or cursor.processed_bytes or cursor.traffic_group != group:
            return False
        await verify_provenance(
            db,
            cursor,
            scope=state.panel_scope,
            panel_id=state.panel_user_id,
            generation=keys['account_generation'],
            cycle=state.ownership_cycle,
            now=now,
            fresh=False,
        )
        cursor.bootstrap_evidence = dict(cursor.bootstrap_evidence, **proof)
    await db.flush()
    return True
