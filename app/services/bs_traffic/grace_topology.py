"""Narrow removal authority for grace during a known-unsent paid squad move.

The old selection is a CAS source, never current paid entitlement. Callers hold
owner/subscription/ledger locks; grace writers collect both topology manifests
before claiming a new overlay attempt.
"""

from sqlalchemy import select

from app.database.models import BsTrafficRenewalIntent, GraceAccessSessionModel, Subscription
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.policy import _read_policy


async def unsent_grace_transition(db, state):
    from app.services.bs_traffic.ownership_cycle import renewal_floor
    from app.services.bs_traffic.renewal_delivery import _payment_receipt
    from app.services.bs_traffic.tariff_switch_topology import pending_topology_transition, receipt_transition

    row = await pending_topology_transition(db, state)
    if row is None:
        return None
    floor = await renewal_floor(db, state)
    latest = await db.scalar(
        select(BsTrafficRenewalIntent.id)
        .where(
            BsTrafficRenewalIntent.subscription_id == state.subscription_id,
            BsTrafficRenewalIntent.id > floor,
            BsTrafficRenewalIntent.transaction_id.is_not(None),
        )
        .order_by(BsTrafficRenewalIntent.id.desc())
        .limit(1)
    )
    sub = await db.get(Subscription, state.subscription_id)
    if (
        row.state != 'pending'
        or row.attempt_token is not None
        or row.attempted_at is not None
        or row.send_deadline is not None
        or latest != row.id
        or sub is None
        or sub.end_date != row.intended_expires_at
        or row.parameters.get('tariff_id') != sub.tariff_id
        or await _payment_receipt(db, row, sub.user_id) is None
    ):
        raise LedgerConflict('Unresolved topology attempt cannot authorize grace')
    value, _ = receipt_transition(row, _read_policy(state.access_policy), ownership_cycle=state.ownership_cycle)
    return row, value


async def validate_grace_selection(db, state, squads):
    """Return whether a pending paid transition authorizes the exact source."""
    policy = _read_policy(state.access_policy)
    transition = await unsent_grace_transition(db, state) if 'topology_transition_id' in policy else None
    managed = set(policy['topology']['ordinary_squads'] + policy['topology']['bs_squads'])
    if not set(squads) <= managed and (
        transition is None or tuple(sorted(squads)) != tuple(transition[1]['pinned_source'])
    ):
        raise LedgerConflict('Pre-grace selection contains unmanaged squads')
    return transition is not None


async def eligible_expired_topology_successor(db, *, state, sub, receipt, now):
    """Verified grace replaced old squads; a new payment may own its exit.

    The finance coordinator atomically supersedes this known-unsent receipt and
    removes the policy marker only after creating its successor payment receipt.
    No grace or an unconfirmed overlay deliberately grants no such permission.
    """
    from app.services.bs_traffic.grace_lifecycle import confirmed_active_overlay
    from app.services.grace_access_codec import _model_to_session

    transition = await unsent_grace_transition(db, state)
    if transition is None or transition[0].id != receipt.id or receipt.intended_expires_at > now or sub.end_date > now:
        return None
    rows = list(
        await db.scalars(
            select(GraceAccessSessionModel)
            .where(
                GraceAccessSessionModel.subscription_id == sub.id,
                GraceAccessSessionModel.state != 'completed',
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if len(rows) != 1:
        return None
    row = rows[0]
    session = _model_to_session(row)
    before = session.split_before
    if (
        row.state != 'active'
        or row.snapshot_version != 4
        or row.reason != 'expired'
        or row.started_at < receipt.created_at
        or before is None
        or before.tariff_id != sub.tariff_id
        or session.billing_before.end_at != receipt.intended_expires_at
        or session.billing_before.subscription_id != sub.id
        or session.remnawave_id != state.panel_user_id
        or not await confirmed_active_overlay(
            db,
            subscription_id=sub.id,
            account_generation=state.account_generation,
            panel_scope=state.panel_scope,
            now=now,
        )
    ):
        return None
    await validate_grace_selection(db, state, before.paid_squads)
    return row.id
