"""Owned, verified switch receipt lookup after client confirmation state is lost."""

from sqlalchemy import select

from app.database.models import BsTrafficRenewalIntent, Subscription
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.tariff_switch import _identity, find_tariff_switch


async def find_tariff_switch_by_operation(db, *, user_id, subscription_id, operation_key):
    key = _identity(user_id, subscription_id, operation_key)
    row = await db.scalar(
        select(BsTrafficRenewalIntent)
        .join(Subscription, Subscription.id == BsTrafficRenewalIntent.subscription_id)
        .where(
            Subscription.id == subscription_id,
            Subscription.user_id == user_id,
            BsTrafficRenewalIntent.operation_key == key,
        )
    )
    if row is None:
        return None
    if not isinstance(row.parameters, dict):
        raise LedgerConflict('Switch receipt is incomplete')
    # Stored parameters are not trusted on their own: the existing verifier
    # checks the owned transaction, method, amount, external key and terms.
    return await find_tariff_switch(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        operation_key=operation_key,
        new_tariff_id=row.parameters.get('tariff_id'),
        expected_price_kopeks=row.charged_kopeks,
        expected_terms_hash=row.parameters.get('switch_terms_hash'),
    )
