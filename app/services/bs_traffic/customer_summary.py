"""Customer accounting snapshot, not confirmation of applied panel access."""

from sqlalchemy import func, select

from app.database.models import BsTrafficLedger, Subscription
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner, summary
from app.services.bs_traffic.policy import _read_policy, effective_group_mode


class SplitSubscriptionNotFound(LedgerConflict):
    pass


async def customer_summary(db, *, user_id, subscription_id):
    """Read one owned activated subscription; caller owns the transaction.

    Decimal byte strings retain exact PostgreSQL integers in browser clients.
    calculated_at is calculation time, not last successful panel collection.
    No panel IO, settlement, activation, payment or commit is performed here.
    """
    generation = await db.scalar(
        select(BsTrafficLedger.account_generation)
        .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            Subscription.traffic_accounting_mode == 'split',
        )
    )
    if generation is None:
        raise SplitSubscriptionNotFound('Split subscription not found')
    state, _ = await _lock(db, user_id, subscription_id, generation)
    await require_activated_owner(db, subscription_id, allow_restriction=True)
    policy = _read_policy(state.access_policy)
    tariff_id, grace_active = (
        await db.execute(
            select(Subscription.tariff_id, Subscription.grace_session_open).where(Subscription.id == subscription_id)
        )
    ).one()
    if policy['tariff_id'] != tariff_id:
        raise LedgerConflict('Split tariff binding changed')
    now = await db.scalar(select(func.clock_timestamp()))
    balances = await summary(
        db, user_id=user_id, subscription_id=subscription_id, account_generation=generation, now=now
    )
    return {
        'subscription_id': subscription_id,
        'calculated_at': now,
        'grace_active': bool(grace_active),
        'groups': {
            group: {
                'mode': effective_group_mode(policy, group, now),
                'remaining_bytes': str(values['remaining_bytes'])
                if effective_group_mode(policy, group, now) == 'finite'
                else None,
                'pending_bytes': str(values['pending_bytes']),
                'pending_operations': values['pending_operations'],
                'needs_review': values['needs_review'],
            }
            for group, values in balances.items()
        },
    }
