"""Persist modes and project each group without revoking unknown or grace access."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from app.database.models import BsTrafficLedger, Subscription, Tariff, User
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.policy import configure_groups, project_access
from app.services.bs_traffic.topology import SplitTopology
from tests.database.test_bs_traffic_ledger_postgres import SCOPE, TARGET, issue, seed as seed_ledger, usage
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
ORDINARY = '22222222-2222-4222-8222-222222222222'
BS = '33333333-3333-4333-8333-333333333333'
NODE_A = '44444444-4444-4444-8444-444444444444'
NODE_B = '55555555-5555-4555-8555-555555555555'


async def seed(db):
    await seed_ledger(db)
    tariff = Tariff(name='Synthetic split tariff')
    db.add(tariff)
    await db.flush()
    await db.execute(update(Subscription).where(Subscription.id == 1).values(tariff_id=tariff.id))


def evidence():
    now = datetime.now(UTC)
    return TopologyEvidence(
        SCOPE,
        SplitTopology((ORDINARY,), (BS,), (NODE_A,), (NODE_B,)),
        'a' * 64,
        now - timedelta(seconds=1),
        now + timedelta(minutes=5),
    )


async def configure(db, ordinary='finite', bs='finite', **changes):
    value = evidence()
    state = await db.scalar(select(BsTrafficLedger).where(BsTrafficLedger.subscription_id == 1))
    arguments = dict(TARGET, ordinary_mode=ordinary, bs_mode=bs, evidence=value, expected_revision=state.revision)
    arguments.update(changes)
    await configure_groups(db, **arguments)
    return value


async def project(db, value, grace=False):
    return await project_access(db, **TARGET, evidence=value, grace_active=grace)


@pytest.mark.parametrize('ordinary', ['finite', 'unlimited'])
@pytest.mark.parametrize('bs', ['finite', 'unlimited'])
async def test_all_finite_and_unlimited_combinations_keep_both_groups(postgres_database, ordinary, bs):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db, group='ordinary', amount=100)
        await issue(db, amount=200)
        value = await configure(db, ordinary, bs)
        result = await project(db, value)
        assert result.ordinary.squads == (ORDINARY,) and result.bs.squads == (BS,)
        assert result.ordinary.remaining_bytes == (100 if ordinary == 'finite' else None)
        assert result.bs.remaining_bytes == (200 if bs == 'finite' else None)
        assert result.grace_candidate is None


async def test_empty_bs_does_not_remove_unlimited_ordinary_or_activate_grace(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'finite')
        result = await project(db, value)
        assert result.ordinary.squads == (ORDINARY,) and result.bs.squads == ()
        assert result.bs.state == 'exhausted' and result.grace_candidate is None


async def test_only_both_confirmed_empty_groups_offer_limited_grace(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db)
        result = await project(db, value)
        assert result.grace_candidate.value == 'limited'


async def test_pending_finite_group_preserves_access_instead_of_treating_unknown_as_zero(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db)
        await usage(db)
        result = await project(db, value)
        assert result.bs.state == 'pending' and result.bs.squads is None
        assert result.ordinary.squads == ()
        assert result.grace_candidate is None


async def test_unlimited_group_is_not_restricted_by_pending_usage(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, bs='unlimited')
        await usage(db)
        result = await project(db, value)
        assert result.bs.squads == (BS,) and result.bs.remaining_bytes is None


async def test_grace_overlay_on_same_node_is_preserved(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db)
        await issue(db)
        result = await project(db, value, grace=True)
        assert result.preserve_grace is True
        assert result.ordinary.squads is None and result.bs.squads is None
        assert result.bs.remaining_bytes == 100 and result.grace_candidate is None


@pytest.mark.parametrize('block', ['owner', 'subscription'])
async def test_administrative_block_overrides_pending_usage_and_grace(postgres_database, block):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db)
        await usage(db)
        if block == 'owner':
            await db.execute(update(User).where(User.id == 1).values(status='blocked'))
        else:
            await db.execute(update(Subscription).where(Subscription.id == 1).values(status='disabled'))
        result = await project(db, value, grace=True)
        assert result.ordinary.squads == () and result.bs.squads == ()
        assert result.preserve_grace is False and result.grace_candidate is None


async def test_expired_paid_access_is_removed_but_running_grace_is_preserved(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'unlimited')
        await db.execute(
            update(Subscription).where(Subscription.id == 1).values(end_date=datetime.now(UTC) - timedelta(days=1))
        )
        result = await project(db, value)
        assert result.ordinary.squads == () and result.bs.squads == ()
        assert result.grace_candidate.value == 'expired'
        assert (await project(db, value, grace=True)).preserve_grace is True


@pytest.mark.parametrize('change', ['missing', 'stale', 'other_panel', 'changed_topology'])
async def test_unverified_topology_never_expands_or_clears_access(postgres_database, change):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'unlimited')
        candidate = {
            'missing': None,
            'stale': replace(value, valid_until=datetime.now(UTC) - timedelta(seconds=1)),
            'other_panel': replace(value, panel_identity=NODE_A),
            'changed_topology': replace(value, fingerprint='b' * 64),
        }[change]
        result = await project(db, candidate)
        assert result.ordinary.squads is None and result.bs.squads is None
        assert result.bs.state == 'topology_pending'


async def test_stale_policy_save_cannot_overwrite_new_purchase_revision(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        with pytest.raises(LedgerConflict):
            await configure(db, expected_revision=0)
        assert (await db.get(BsTrafficLedger, 1)).access_policy is None


async def test_policy_refresh_does_not_clear_grants_or_pending_operations(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await issue(db)
        await usage(db)
        await configure(db)
        value = await configure(db, ordinary='unlimited')
        result = await project(db, value)
        assert result.bs.remaining_bytes == 100 and result.bs.state == 'pending'


@pytest.mark.parametrize('mode', [None, '', '0', 0, True, 'unknown'])
async def test_invalid_mode_does_not_turn_zero_into_unlimited(postgres_database, mode):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        with pytest.raises(LedgerConflict):
            await configure(db, bs=mode)


async def test_classic_subscription_cannot_configure_split_policy(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed_ledger(db)
        with pytest.raises(LedgerConflict, match='tariff'):
            await configure(db)
        assert (await db.get(BsTrafficLedger, 1)).access_policy is None


async def test_tariff_switch_requires_reconciliation_before_access_changes(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'unlimited')
        await db.execute(update(Subscription).where(Subscription.id == 1).values(tariff_id=None))
        result = await project(db, value)
        assert result.ordinary.squads is None and result.bs.squads is None
        assert result.bs.state == 'tariff_pending'


async def test_same_fingerprint_does_not_hide_topology_change(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'unlimited')
        altered = replace(value, topology=replace(value.topology, ordinary_nodes=(NODE_B,), bs_nodes=(NODE_A,)))
        result = await project(db, altered)
        assert result.ordinary.squads is None and result.bs.squads is None
        assert result.bs.state == 'topology_pending'


@pytest.mark.parametrize(
    'ordinary,bs', [('disabled', 'unlimited'), ('unlimited', 'disabled'), ('disabled', 'disabled')]
)
async def test_disabled_group_never_becomes_unlimited(postgres_database, ordinary, bs):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, ordinary, bs)
        result = await project(db, value)
        assert result.ordinary.squads == (() if ordinary == 'disabled' else (ORDINARY,))
        assert result.bs.squads == (() if bs == 'disabled' else (BS,))
        assert result.grace_candidate is None


async def test_identical_save_is_idempotent_without_invalidating_purchase_revision(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await configure(db)
        state = await db.get(BsTrafficLedger, 1)
        revision = state.revision
        await configure(db)
        assert state.revision == revision


@pytest.mark.parametrize('status', ['pending', 'expired'])
async def test_non_active_subscription_does_not_gain_groups_from_future_end_date(postgres_database, status):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'unlimited')
        await db.execute(update(Subscription).where(Subscription.id == 1).values(status=status))
        result = await project(db, value)
        assert result.bs.state == 'subscription_pending'
        assert result.ordinary.squads is None and result.bs.squads is None


async def test_limited_status_does_not_revoke_the_other_paid_group(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'finite')
        await db.execute(update(Subscription).where(Subscription.id == 1).values(status='limited'))
        result = await project(db, value)
        assert result.ordinary.squads == (ORDINARY,) and result.bs.squads == ()
        assert result.grace_candidate is None


@pytest.mark.parametrize('change', ['stale', 'panel'])
async def test_configuration_rejects_unverified_evidence_before_saving(postgres_database, change):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = evidence()
        value = replace(
            value,
            **(
                {'valid_until': datetime.now(UTC) - timedelta(seconds=1)}
                if change == 'stale'
                else {'panel_identity': NODE_A}
            ),
        )
        with pytest.raises(LedgerConflict):
            await configure(db, evidence=value)
        assert (await db.get(BsTrafficLedger, 1)).access_policy is None


async def test_other_subscription_cannot_reuse_policy_even_for_same_owner(postgres_database):
    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, 'unlimited', 'unlimited')
        with pytest.raises(LedgerConflict):
            await project_access(db, **dict(TARGET, subscription_id=2), evidence=value, grace_active=False)


async def test_concurrent_policy_saves_cannot_silently_overwrite_each_other(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)
        value = evidence()

        async def save(mode):
            async with maker() as conn, conn.begin():
                await barrier.wait()
                try:
                    await configure_groups(
                        conn, **TARGET, ordinary_mode=mode, bs_mode='finite', evidence=value, expected_revision=0
                    )
                except LedgerConflict:
                    return 'conflict'
                return mode

        results = await asyncio.wait_for(asyncio.gather(save('finite'), save('unlimited')), 10)
        assert results.count('conflict') == 1
        winner = next(item for item in results if item != 'conflict')
        state = await db.get(BsTrafficLedger, 1)
        assert state.revision == 1 and state.access_policy['ordinary_mode'] == winner


@pytest.mark.parametrize('raw', [False, True])
async def test_new_purchase_restores_access_only_with_classified_frozen_old_debits(postgres_database, raw):
    from datetime import timedelta

    from app.services.bs_traffic.ledger import open_usage
    from tests.database.test_bs_traffic_ledger_postgres import NOW

    async with postgres_session(postgres_database, [User.__table__]) as db:
        await seed(db)
        value = await configure(db, ordinary='unlimited')
        await open_usage(
            db,
            **TARGET,
            group='bs',
            source_key='old',
            amount_bytes=100,
            interval_start=NOW,
            interval_end=NOW + timedelta(seconds=10),
            evidence_kind='raw_pending' if raw else 'classified',
        )
        await issue(db, key='new', amount=200)
        result = await project(db, value)
        assert result.bs.pending_bytes == 100
        assert result.bs.remaining_bytes == 200
        assert result.bs.state == ('pending' if raw else 'active')
        assert result.bs.squads == (None if raw else (BS,))
        assert result.ordinary.squads == (ORDINARY,)
        assert result.grace_candidate is None
