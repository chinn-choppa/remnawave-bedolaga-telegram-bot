"""Immutable node epochs, captured under the financial owner/ledger lock."""

from datetime import UTC, datetime, time, timedelta

from sqlalchemy import func, select, union

from app.database.models import (
    BsTrafficNodeMembership as Membership,
    BsTrafficNodeTail as Tail,
    BsTrafficObservation as Raw,
    BsTrafficUsageCursor as Cursor,
)
from app.services.bs_traffic.ledger import LedgerConflict


def removals(old, new):
    result = {}
    for group in ('ordinary', 'bs'):
        before, after = set(old[f'{group}_nodes']), set(new[f'{group}_nodes'])
        result[group] = sorted(before - after)
    return result


async def capture_memberships(db, *, state, sub, receipt, transition, now, baselines=None):
    removed = removals(transition['source']['topology'], transition['target']['topology'])
    added = removals(transition['target']['topology'], transition['source']['topology'])
    if not any(removed.values()) and not any(added.values()):
        return
    count = await db.scalar(select(func.count()).select_from(Membership).where(Membership.subscription_id == sub.id))
    if (
        count
        + sum(
            len(transition['source']['topology'][f'{group}_nodes']) + len(added[group]) for group in ('ordinary', 'bs')
        )
        > 2000
    ):
        raise LedgerConflict('Node epoch history budget exceeded')
    for group in ('ordinary', 'bs'):
        for node in transition['source']['topology'][f'{group}_nodes']:
            row = await db.scalar(
                select(Membership)
                .where(Membership.subscription_id == sub.id, Membership.node_uuid == node, Membership.state == 'active')
                .with_for_update()
            )
            if row is not None and (
                row.traffic_group != group
                or row.panel_scope != state.panel_scope
                or row.panel_user_id != state.panel_user_id
                or row.account_generation != state.account_generation
                or row.ownership_cycle != state.ownership_cycle
            ):
                raise LedgerConflict('Historical node cannot be reused')
            if row is None:
                row = Membership(
                    subscription_id=sub.id,
                    panel_scope=state.panel_scope,
                    panel_user_id=state.panel_user_id,
                    account_generation=state.account_generation,
                    ownership_cycle=state.ownership_cycle,
                    node_uuid=node,
                    traffic_group=group,
                    source_fingerprint=transition['source']['fingerprint'],
                    effective_at=state.ownership_started_at or sub.start_date or now,
                    source_paid_end_at=sub.end_date,
                    state='active',
                )
                db.add(row)
                await db.flush()
            if node in removed[group]:
                row.state, row.retired_at, row.transition_receipt_id = 'tail', now, receipt.id
                row.source_paid_end_at = sub.end_date
                row.source_fingerprint = transition['source']['fingerprint']
                historical = union(
                    select(Cursor.day).where(
                        Cursor.subscription_id == sub.id,
                        Cursor.node_uuid == node,
                        Cursor.account_generation == state.account_generation,
                    ),
                    select(Raw.day).where(
                        Raw.panel_scope == state.panel_scope,
                        Raw.panel_user_id == state.panel_user_id,
                        Raw.account_generation == state.account_generation,
                        Raw.node_uuid == node,
                    ),
                ).subquery()
                days = set(
                    await db.scalars(
                        select(historical.c.day)
                        .where(historical.c.day <= now.date())
                        .order_by(historical.c.day)
                        .limit(129)
                    )
                ) | {now.date()}
                overflow = len(days) > 128
                if overflow:
                    import structlog

                    row.state = 'attention'
                    structlog.get_logger(__name__).warning(
                        'Split removed node tail requires attention', membership_id=row.id, reason='day_budget_exceeded'
                    )
                for day in sorted(days)[:128]:
                    db.add(
                        Tail(
                            membership_id=row.id,
                            day=day,
                            state='attention' if overflow else 'pending',
                            next_poll_at=now,
                            last_error='day_budget_exceeded' if overflow else None,
                        )
                    )
    await db.flush()
    entries = {item['node']: item for group in (baselines or {}).values() for item in group['nodes']}
    for group in ('ordinary', 'bs'):
        for node in added[group]:
            db.add(
                Membership(
                    subscription_id=sub.id,
                    panel_scope=state.panel_scope,
                    panel_user_id=state.panel_user_id,
                    account_generation=state.account_generation,
                    ownership_cycle=state.ownership_cycle,
                    node_uuid=node,
                    traffic_group=group,
                    source_fingerprint=transition['target']['fingerprint'],
                    effective_at=now,
                    source_paid_end_at=sub.end_date,
                    transition_receipt_id=receipt.id,
                    state='active',
                    entry_evidence=entries.get(node),
                )
            )
    await db.flush()


async def retired_membership(db, state, node, day=None):
    # The current epoch owns its current/future daily bucket. Its entry fence
    # preserves the first crossing delta; old tails must not consume it twice.
    active = await db.scalar(
        select(Membership).where(
            Membership.subscription_id == state.subscription_id,
            Membership.node_uuid == node,
            Membership.state == 'active',
            Membership.account_generation == state.account_generation,
            Membership.ownership_cycle == state.ownership_cycle,
        )
    )
    if active is not None and (day is None or day >= active.effective_at.date()):
        return None
    return await db.scalar(
        select(Membership)
        .where(
            Membership.subscription_id == state.subscription_id,
            Membership.node_uuid == node,
            Membership.state != 'active',
            Membership.panel_scope == state.panel_scope,
            Membership.panel_user_id == state.panel_user_id,
            Membership.account_generation == state.account_generation,
            *(
                [Membership.effective_at < datetime.combine(day + timedelta(days=1), time.min, tzinfo=UTC)]
                if day is not None
                else []
            ),
        )
        .order_by(Membership.retired_at.desc(), Membership.id.desc())
        .limit(1)
    )


async def validate_removal_memberships(db, *, state, receipt, transition):
    """A receipt JSON alone cannot invent removed-account ownership."""
    removed = transition.get('removed_nodes')
    if removed is None:
        return
    from app.services.bs_traffic.tariff_switch_topology import provenance_receipt_id

    root_id = await provenance_receipt_id(db, state=state, receipt=receipt, transition=transition)
    rows = list(
        await db.scalars(
            select(Membership).where(
                Membership.subscription_id == state.subscription_id,
                Membership.node_uuid.in_([node for nodes in removed.values() for node in nodes]),
                Membership.state != 'active',
                Membership.transition_receipt_id == root_id,
            )
        )
    )
    if len(rows) != sum(map(len, removed.values())):
        raise LedgerConflict('Missing removal provenance')
    added = transition.get('added_nodes', {})
    entry_rows = list(
        await db.scalars(
            select(Membership).where(
                Membership.subscription_id == state.subscription_id,
                Membership.node_uuid.in_([node for nodes in added.values() for node in nodes]),
                Membership.state == 'active',
                Membership.transition_receipt_id == root_id,
            )
        )
    )
    if len(entry_rows) != sum(map(len, added.values())) or any(
        row.node_uuid not in added[row.traffic_group]
        or (row.panel_scope, row.panel_user_id, row.account_generation, row.ownership_cycle)
        != (state.panel_scope, state.panel_user_id, state.account_generation, state.ownership_cycle)
        or row.source_fingerprint != transition['target']['fingerprint']
        for row in entry_rows
    ):
        raise LedgerConflict('Missing entry provenance')
    for row in rows:
        if (
            row.state == 'active'
            or row.node_uuid not in removed[row.traffic_group]
            or row.panel_scope != state.panel_scope
            or row.panel_user_id != state.panel_user_id
            or row.account_generation != state.account_generation
            or row.ownership_cycle != state.ownership_cycle
            or row.source_fingerprint != transition['source']['fingerprint']
            or row.transition_receipt_id is None
            or row.transition_receipt_id > receipt.id
            or (not transition.get('allow_empty_source') and row.transition_receipt_id != receipt.id)
        ):
            raise LedgerConflict('Removal provenance changed')
