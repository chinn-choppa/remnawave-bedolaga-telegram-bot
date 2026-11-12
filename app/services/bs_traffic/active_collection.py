"""Finite raw-collection passes for activated split accounts; no runtime hook.

A scan captures a highwater. Collection jobs use existing panel admission and
budgets; this planner never settles bytes, creates baselines or changes access.
"""

from dataclasses import dataclass
from datetime import UTC, date

from sqlalchemy import func, select

from app.database.models import BsTrafficLedger as Ledger, BsTrafficPanelBinding as Binding
from app.services.bs_traffic.cutover_collection import _enqueue_rows
from app.services.bs_traffic.ledger import LedgerConflict, _key, activated_owner_query
from app.services.bs_traffic.observations import MAX_NODES
from app.services.bs_traffic.panel_binding import BoundPanelReader, endpoint_fingerprint
from app.services.bs_traffic.recovery_scan import _postgres, _scope


@dataclass(frozen=True)
class ActiveCollectionWindow:
    panel_scope: str
    upper_id: int
    day: date


def _candidates(scope):
    return (
        activated_owner_query()
        .with_only_columns(
            Ledger.subscription_id,
            Ledger.panel_user_id,
            Ledger.account_generation,
            Ledger.access_policy,
            maintain_column_froms=True,
        )
        .where(Ledger.panel_scope == scope)
    )


async def start_active_collection(db, *, panel_scope):
    _postgres(db)
    scope = _scope(panel_scope)
    # Ledger PK highwater bounds subsequent pages without scanning JSON policies.
    upper = await db.scalar(
        select(func.coalesce(func.max(Ledger.subscription_id), 0)).where(Ledger.panel_scope == scope)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    return ActiveCollectionWindow(scope, upper, now.astimezone(UTC).date())


async def enqueue_active_collection(db, reader, window, *, round_key, after_id=0, limit=100, nodes_per_job=1):
    """Enqueue one keyset page in caller transaction, without per-account queries.

    Membership is rechecked per page; later activations behind the cursor wait
    for the next pass. One node per job by default limits response size. This is
    raw evidence only: settlement must recheck owner, topology and grace later.
    """
    _postgres(db)
    _key(round_key)
    if not isinstance(reader, BoundPanelReader) or not isinstance(window, ActiveCollectionWindow):
        raise LedgerConflict('Bound reader and active collection window required')
    if (
        _scope(window.panel_scope) != reader.panel_scope
        or endpoint_fingerprint(reader._api.base_url) != reader.endpoint_hash
    ):
        raise LedgerConflict('Collection panel binding changed')
    if type(window.day) is not date:
        raise ValueError('Explicit collection day required')
    if type(window.upper_id) is not int or window.upper_id < 0:
        raise ValueError('Invalid collection highwater')
    if type(after_id) is not int or not 0 <= after_id <= window.upper_id:
        raise ValueError('Invalid collection cursor')
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Collection page limit must be between 1 and 500')
    if type(nodes_per_job) is not int or not 1 <= nodes_per_job <= MAX_NODES:
        raise ValueError('Invalid collection node batch')
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
        rows = (
            await db.execute(
                _candidates(reader.panel_scope)
                .where(
                    Ledger.subscription_id > after_id,
                    Ledger.subscription_id <= window.upper_id,
                )
                .order_by(Ledger.subscription_id)
                .limit(limit + 1)
            )
        ).all()
        selected = rows[:limit]
        next_after = selected[-1].subscription_id if len(rows) > limit else None
        now = await db.scalar(select(func.clock_timestamp()))
        if window.day > now.astimezone(UTC).date():
            raise ValueError('Future collection day is not allowed')
        return await _enqueue_rows(
            db,
            reader,
            [(row.panel_user_id, row.account_generation, row.access_policy) for row in selected],
            round_key=round_key,
            day=window.day,
            upper_id=window.upper_id,
            after_id=after_id,
            limit=limit,
            nodes_per_job=nodes_per_job,
            rejected_count=0,
            next_after=next_after,
            prefix='active',
        )
