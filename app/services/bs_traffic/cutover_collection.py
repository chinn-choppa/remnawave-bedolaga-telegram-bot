"""Plan shared raw collection for a bounded page of explicit cutovers.

Caller commits before draining the existing collection queue. This does not
charge usage, create baselines, or register a recurring fleet scheduler. A round
key identifies immutable work: changed membership requires a new round, never
rewriting an existing request's identity map.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC
from itertools import batched

from sqlalchemy import func, or_, select

from app.database.models import (
    BsTrafficCutoverIntent as Intent,
    BsTrafficLedger as Ledger,
    BsTrafficPanelBinding as Binding,
    Subscription,
)
from app.services.bs_traffic.cutover_scan import CutoverWindow, _active, cutover_page
from app.services.bs_traffic.ledger import LedgerConflict, _key
from app.services.bs_traffic.observations import MAX_NODES
from app.services.bs_traffic.panel_binding import BoundPanelReader, endpoint_fingerprint
from app.services.bs_traffic.policy import _read_policy
from app.services.bs_traffic.queue import enqueue_job


MAX_PAGE_NODES = 2000
MAX_PAGE_JOBS = 64


@dataclass(frozen=True)
class CutoverCollectionPage:
    job_ids: tuple[int, ...]
    accounts: int
    rejected: int
    next_after: int | None


async def enqueue_cutover_collection(db, reader, window, *, round_key, after_id=0, limit=100, nodes_per_job=1):
    """Atomically enqueue shared jobs; no HTTP and no per-subscription SQL loop.

    Only locally bound numeric panel IDs are mapped to their enrolled generation.
    Returned raw evidence still requires intake/ownership checks before any use.
    Invalid policies are counted and skipped, never interpreted as empty access.
    At most 500 accounts and 2000 distinct nodes can be planned per page.
    Default one node per request: panel responses include users outside this
    page, so selected account count cannot bound the size of a multi-node reply.
    """
    _key(round_key)
    if type(nodes_per_job) is not int or not 1 <= nodes_per_job <= MAX_NODES:
        raise ValueError('Collection nodes per job must be between 1 and 100')
    if not isinstance(reader, BoundPanelReader) or not isinstance(window, CutoverWindow):
        raise LedgerConflict('Bound reader and cutover window required')
    if window.panel_scope != reader.panel_scope or endpoint_fingerprint(reader._api.base_url) != reader.endpoint_hash:
        raise LedgerConflict('Collection panel binding changed')
    async with db.begin_nested():
        binding = await db.scalar(
            select(Binding.panel_scope)
            .where(
                Binding.panel_scope == reader.panel_scope,
                Binding.endpoint_hash == reader.endpoint_hash,
                Binding.retired_at.is_(None),
            )
            .with_for_update(read=True)
        )
        if binding is None:
            raise LedgerConflict('Collection panel epoch is unavailable')
        page = await cutover_page(db, window, after_id=after_id, limit=limit)
        now = await db.scalar(select(func.clock_timestamp()))
        day = now.astimezone(UTC).date()
        rows = (
            await db.execute(
                select(Ledger.panel_user_id, Ledger.account_generation, Ledger.access_policy)
                .select_from(Intent)
                .join(Ledger, Ledger.subscription_id == Intent.subscription_id)
                .join(Subscription, Subscription.id == Intent.subscription_id)
                .where(
                    Intent.id.in_([item.operation_id for item in page.items]),
                    Intent.panel_scope == reader.panel_scope,
                    Ledger.panel_scope == reader.panel_scope,
                    Ledger.account_generation == Intent.account_generation,
                    Ledger.panel_user_id == Subscription.remnawave_id,
                    Subscription.traffic_accounting_mode == 'split_pending',
                    _active(),
                    or_(Intent.state != 'pending', Intent.valid_until > now),
                )
                .order_by(Intent.id)
            )
        ).all()
        return await _enqueue_rows(
            db,
            reader,
            rows,
            round_key=round_key,
            day=day,
            upper_id=window.upper_id,
            after_id=after_id,
            limit=limit,
            nodes_per_job=nodes_per_job,
            rejected_count=len(page.items) - len(rows),
            next_after=page.next_after,
            prefix='cutover',
        )


async def _enqueue_rows(
    db, reader, rows, *, round_key, day, upper_id, after_id, limit, nodes_per_job, rejected_count, next_after, prefix
):
    by_node = {}
    accepted = set()
    rejected = rejected_count
    for panel_id, generation, stored_policy in rows:
        try:
            policy = _read_policy(stored_policy)
            if (
                type(panel_id) is not int
                or not 0 < panel_id < 2**53
                or not isinstance(generation, str)
                or len(generation) != 64
                or any(char not in '0123456789abcdef' for char in generation)
            ):
                raise LedgerConflict('Invalid collection identity')
        except LedgerConflict:
            rejected += 1
            continue
        nodes = {
            node
            for group in ('ordinary', 'bs')
            if policy[f'{group}_mode'] == 'finite'
            for node in policy['topology'][f'{group}_nodes']
        }
        if not nodes:
            continue
        accepted.add(panel_id)
        for node in nodes:
            by_node.setdefault(node, {})[panel_id] = generation
        if len(by_node) > MAX_PAGE_NODES:
            raise LedgerConflict('Collection page exceeds node budget; use a smaller page')
    if len(by_node) > MAX_PAGE_JOBS * nodes_per_job:
        raise LedgerConflict('Collection page exceeds job budget; reduce page or enlarge node batch')
    jobs = []
    for offset, nodes in enumerate(batched(sorted(by_node), nodes_per_job, strict=False)):
        identities = {key: value for node in nodes for key, value in by_node[node].items()}
        key = hashlib.sha256(
            json.dumps([round_key, day.isoformat(), upper_id, after_id, limit, nodes_per_job, offset]).encode()
        ).hexdigest()
        jobs.append(
            await enqueue_job(
                db,
                operation_key=f'{prefix}:{key}',
                panel_scope=reader.panel_scope,
                day=day,
                node_uuids=nodes,
                identities=identities,
            )
        )
    return CutoverCollectionPage(tuple(jobs), len(accepted), rejected, next_after)
