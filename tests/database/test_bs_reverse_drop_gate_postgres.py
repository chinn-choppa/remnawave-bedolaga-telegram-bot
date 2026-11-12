"""Unsent cancelled socket plans cannot strand an otherwise drained account."""

from datetime import UTC, datetime, timedelta

import pytest

from app.database.models import BsTrafficDelivery, BsTrafficLedger
from app.services.bs_traffic.cutover_activation import activate_cutover
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.reverse_transfer import inspect_reverse
from tests.database.test_bs_traffic_cutover_activation_postgres import ready
from tests.database.test_bs_traffic_managed_transfer_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    'delivery_state,drop_state,blocked',
    [
        ('verified', 'pending', True),
        ('verified', 'attempted', True),
        ('cancelled', 'pending', False),
        ('verified', 'accepted', False),
    ],
)
async def test_reverse_respects_only_live_drop_obligations(
    postgres_database, monkeypatch, delivery_state, drop_state, blocked
):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, operation = await ready(db, monkeypatch)
        await db.commit()
        assert await activate_cutover(reader, **keys, operation_id=operation) == 'activated'
        ledger = await db.get(BsTrafficLedger, keys['subscription_id'], populate_existing=True)
        now = datetime.now(UTC)
        ledger.access_policy = dict(
            ledger.access_policy, bs_purchase_enabled=False, bs_drain_until=(now - timedelta(seconds=1)).isoformat()
        )
        db.add(
            BsTrafficDelivery(
                subscription_id=keys['subscription_id'],
                operation_key='synthetic-drop-gate',
                revision=ledger.revision,
                guard_hash='0' * 64,
                local_guard_hash='0' * 64,
                squads=[],
                state=delivery_state,
                token='synthetic-token' if delivery_state == 'verified' else None,
                attempted_at=now if delivery_state == 'verified' else None,
                valid_until=now + timedelta(minutes=1),
                connection_drop={'state': drop_state},
            )
        )
        await db.flush()
        if blocked:
            with pytest.raises(LedgerConflict, match='connection drop'):
                await inspect_reverse(db, reader, **keys)
        else:
            assert (await inspect_reverse(db, reader, **keys))['subscription_id'] == keys['subscription_id']
