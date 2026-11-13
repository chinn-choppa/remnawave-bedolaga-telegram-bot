"""Operator snapshot reads real durable PostgreSQL without accounting writes."""

from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import (
    Base,
    BsTrafficCollectionJob as Job,
    BsTrafficFleetRound as Round,
    BsTrafficPanelBinding as Binding,
)
from app.services.bs_traffic.operator_diagnostics import read_diagnostics
from tests.fixtures.postgres_db import postgres_engine


pytestmark = pytest.mark.postgres


def runtime(*, local=False, ready=False):
    return SimpleNamespace(
        task=SimpleNamespace(done=lambda: False) if local else None, interval=30, ready=lambda scope: ready
    )


async def binding(db, number=1, age=0):
    scope = f'masked-{number}'
    now = await db.scalar(select(func.clock_timestamp()))
    db.add(Binding(panel_scope=scope, endpoint_hash=f'{number:064x}'))
    db.add(
        Round(
            panel_scope=scope,
            round_id=f'round-{number}',
            day=now.date(),
            upper_id=0,
            cursor=0,
            phase='complete',
            started_at=now,
            completed_at=now,
            last_success_at=now - timedelta(seconds=age),
            error_count=0,
        )
    )
    await db.commit()
    return scope


async def test_empty_snapshot_does_not_claim_readiness(postgres_database):
    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        snapshot = await read_diagnostics(async_sessionmaker(engine), runtime=runtime())
        assert snapshot.scopes == [] and snapshot.active_bindings == 0
        assert snapshot.local_runtime_running is None


@pytest.mark.parametrize('age,expected', [(0, True), (91, False), (-10, False)])
async def test_durable_freshness_overrules_local_runtime_readiness(postgres_database, age, expected):
    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as db:
            await binding(db, age=age)
        snapshot = await read_diagnostics(factory, runtime=runtime(local=True, ready=True))
        assert snapshot.scopes[0].diagnostic_ready is expected
        assert snapshot.scopes[0].local_runtime_ready is True


async def test_http_worker_with_no_local_task_uses_durable_snapshot(postgres_database):
    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine)
        async with factory() as db:
            await binding(db)
        snapshot = await read_diagnostics(factory, runtime=runtime())
        assert snapshot.scopes[0].diagnostic_ready
        assert snapshot.scopes[0].local_runtime_ready is None


async def test_attention_is_counted_without_exposing_raw_error_or_identifiers(postgres_database):
    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine)
        async with factory() as db:
            scope = await binding(db)
            now = await db.scalar(select(func.clock_timestamp()))
            db.add(
                Job(
                    panel_scope=scope,
                    operation_key='masked-operation',
                    request_hash='a' * 64,
                    day=now.date(),
                    node_uuids=['masked-node'],
                    identities={'sensitive': 'masked-user'},
                    selected_accounts=1,
                    state='attention',
                    due_at=now,
                    last_error='masked-private-error',
                    created_at=now - timedelta(minutes=5),
                )
            )
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime(local=True, ready=True))
        row = snapshot.scopes[0]
        assert row.queues.collection_attention == 1 and not row.diagnostic_ready
        assert 'collection_attention' in row.warnings
        assert row.oldest_pending_at is not None
        serialized = snapshot.model_dump_json()
        for secret in ['masked-1', 'masked-node', 'masked-user', 'masked-private-error', 'masked-operation']:
            assert secret not in serialized
        async with factory() as db:
            assert await db.scalar(select(Job.state)) == 'attention'
            assert await db.scalar(select(func.count()).select_from(Job)) == 1


async def test_scope_limit_is_explicit_and_retired_contexts_excluded(postgres_database):
    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine)
        async with factory() as db:
            for number in range(1, 19):
                await binding(db, number)
            item = await db.get(Binding, 'masked-18')
            item.retired_at = await db.scalar(select(func.clock_timestamp()))
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime())
        assert snapshot.active_bindings == 17 and snapshot.bindings_truncated
        assert len(snapshot.scopes) == 16


async def test_snapshot_has_fixed_query_count_and_read_only_transaction(postgres_database):
    from sqlalchemy import event

    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine)
        async with factory() as db:
            await binding(db)
        statements = []
        event.listen(
            engine.sync_engine,
            'before_cursor_execute',
            lambda conn, cursor, statement, params, context, many: statements.append(statement),
        )
        await read_diagnostics(factory, runtime=runtime())
        assert len(statements) == 17
        assert statements[0] == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
        assert all(statement.startswith(('SET ', 'SELECT ')) for statement in statements)


async def test_real_account_backlog_and_verified_reverse_are_counted_correctly(postgres_database, monkeypatch):
    from app.database.models import (
        BsTrafficCollectionBudget as Budget,
        BsTrafficFleetWork as Work,
        BsTrafficLedger as Ledger,
        BsTrafficOwnershipCycle as Cycle,
    )
    from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase

    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as db:
            _, reader, _, _ = await prepared_purchase(db, monkeypatch)
            ledger = await db.get(Ledger, 1)
            now = await db.scalar(select(func.clock_timestamp()))
            db.add(
                Round(
                    panel_scope=reader.panel_scope,
                    round_id='masked-round',
                    day=now.date(),
                    upper_id=1,
                    cursor=1,
                    phase='complete',
                    started_at=now,
                    completed_at=now,
                    last_success_at=now,
                    error_count=0,
                )
            )
            db.add(
                Work(
                    subscription_id=1,
                    panel_scope=reader.panel_scope,
                    account_generation=ledger.account_generation,
                    target_round='masked-round',
                    due_at=now,
                    error='masked-private-error',
                    access_dirty=True,
                )
            )
            db.add(Budget(panel_scope=reader.panel_scope, cooldown_until=now + timedelta(minutes=5)))
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime())
        row = snapshot.scopes[0]
        assert row.queues.settlement_pending == row.queues.settlement_errors == 1
        assert row.pause_until is not None and 'panel_backoff' in row.warnings
        async with factory() as db:
            # A verified reverse leaves historical work, excluded just as in runtime._health.
            cycle = await db.get(Cycle, (1, ledger.ownership_cycle))
            if cycle is None:
                cycle = Cycle(
                    subscription_id=1,
                    ownership_cycle=ledger.ownership_cycle,
                    forward_receipt={},
                    state='managed',
                    reverse_attempt={'state': 'verified'},
                )
                db.add(cycle)
            else:
                cycle.state = 'managed'
                cycle.reverse_attempt = {'state': 'verified'}
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime())
        assert snapshot.scopes[0].queues.settlement_pending == 0
        assert snapshot.scopes[0].queues.settlement_errors == 0


async def test_removed_node_pending_attention_and_delayed_are_distinct(postgres_database, monkeypatch):
    from app.database.models import (
        BsTrafficLedger as Ledger,
        BsTrafficNodeMembership as Membership,
        BsTrafficNodeTail as Tail,
    )
    from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase

    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as db:
            _, reader, _, _ = await prepared_purchase(db, monkeypatch)
            ledger = await db.get(Ledger, 1)
            now = await db.scalar(select(func.clock_timestamp()))
            member = Membership(
                subscription_id=1,
                panel_scope=reader.panel_scope,
                panel_user_id=ledger.panel_user_id,
                account_generation=ledger.account_generation,
                ownership_cycle=ledger.ownership_cycle,
                node_uuid='masked-node',
                traffic_group='bs',
                source_fingerprint='a' * 64,
                effective_at=now - timedelta(days=1),
                retired_at=now,
                source_paid_end_at=now + timedelta(days=1),
                state='attention',
            )
            db.add(member)
            await db.flush()
            db.add(
                Tail(
                    membership_id=member.id,
                    day=now.date(),
                    state='pending',
                    next_poll_at=now - timedelta(minutes=3),
                    last_error='masked-private-error',
                )
            )
            db.add(
                Tail(membership_id=member.id, day=now.date() - timedelta(days=1), state='attention', next_poll_at=now)
            )
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime())
        row = snapshot.scopes[0]
        assert (
            row.queues.node_tail_pending
            == row.queues.node_tail_attention
            == row.queues.node_tail_delayed
            == row.queues.topology_attention
            == 1
        )
        assert not row.diagnostic_ready
        assert 'masked-private-error' not in snapshot.model_dump_json()


async def test_paid_delivery_enrollment_renewal_and_socket_unknown_are_visible(postgres_database, monkeypatch):
    from app.database.models import (
        BsTrafficDelivery as Delivery,
        BsTrafficEnrollment as Enrollment,
        BsTrafficLedger as Ledger,
        BsTrafficRenewalIntent as Renewal,
        Subscription,
    )
    from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase

    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as db:
            _, reader, _, _ = await prepared_purchase(db, monkeypatch)
            ledger = await db.get(Ledger, 1)
            sub = await db.get(Subscription, 1)
            now = await db.scalar(select(func.clock_timestamp()))
            for index, state in enumerate(('verified', 'attention')):
                db.add(
                    Delivery(
                        subscription_id=1,
                        operation_key=f'masked-delivery-{index}',
                        revision=ledger.revision,
                        guard_hash='a' * 64,
                        local_guard_hash='b' * 64,
                        squads=[],
                        state=state,
                        token=f'masked-token-{index}',
                        attempted_at=now,
                        valid_until=now + timedelta(minutes=5),
                        connection_drop={'state': 'attempted'} if state == 'verified' else None,
                    )
                )
            db.add(
                Renewal(
                    subscription_id=1,
                    operation_key='masked-renewal',
                    parameters={},
                    panel_scope=reader.panel_scope,
                    account_generation=ledger.account_generation,
                    intended_expires_at=now + timedelta(days=30),
                    state='pending',
                )
            )
            for cycle, scope in [(17, None), (18, reader.panel_scope)]:
                db.add(
                    Enrollment(
                        subscription_id=1,
                        ownership_cycle=cycle,
                        user_id=1,
                        tariff_id=sub.tariff_id,
                        source_kind='admin',
                        actor_id=1,
                        terms={},
                        terms_hash='c' * 64,
                        state='pending',
                        panel_scope=scope,
                    )
                )
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime())
        row = snapshot.scopes[0]
        assert snapshot.unbound_enrollments == 1
        assert (
            row.queues.delivery_attention
            == row.queues.renewal_pending
            == row.queues.enrollment_pending
            == row.queues.connection_drop_unknown
            == 1
        )
        assert not row.diagnostic_ready
        assert 'connection_drop_unknown' in row.warnings
        assert 'masked-token' not in snapshot.model_dump_json()


async def test_database_rejects_an_accidental_accounting_write(postgres_database, monkeypatch):
    from sqlalchemy.exc import DBAPIError

    from app.services.bs_traffic import operator_diagnostics

    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine)
        async with factory() as db:
            await binding(db)

        async def accidental_write(db, *args, **kwargs):
            await db.execute(Binding.__table__.update().values(retired_at=func.clock_timestamp()))

        monkeypatch.setattr(operator_diagnostics, '_aggregate', accidental_write)
        with pytest.raises(DBAPIError, match='read-only transaction'):
            await read_diagnostics(factory, runtime=runtime())
        async with factory() as db:
            assert await db.scalar(select(Binding.retired_at)) is None


@pytest.mark.parametrize('completed_round', [None, 'previous-round'])
@pytest.mark.parametrize('returned', [False, True])
async def test_fresh_work_waits_for_current_round_unless_ownership_return_verified(
    postgres_database, monkeypatch, completed_round, returned
):
    from app.database.models import (
        BsTrafficFleetWork as Work,
        BsTrafficLedger as Ledger,
        BsTrafficOwnershipCycle as Cycle,
    )
    from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase

    async with postgres_engine(postgres_database, list(Base.metadata.sorted_tables)) as engine:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as db:
            _, reader, _, _ = await prepared_purchase(db, monkeypatch)
            ledger = await db.get(Ledger, 1)
            now = await db.scalar(select(func.clock_timestamp()))
            db.add(
                Round(
                    panel_scope=reader.panel_scope,
                    round_id='current-round',
                    day=now.date(),
                    upper_id=1,
                    cursor=1,
                    phase='settling',
                    started_at=now,
                    last_success_at=now,
                    error_count=0,
                )
            )
            db.add(
                Work(
                    subscription_id=1,
                    panel_scope=reader.panel_scope,
                    account_generation=ledger.account_generation,
                    target_round='current-round',
                    completed_round=completed_round,
                    due_at=now,
                    settled_at=now,
                    access_dirty=False,
                )
            )
            if returned:
                db.add(
                    Cycle(
                        subscription_id=1,
                        ownership_cycle=ledger.ownership_cycle,
                        forward_receipt={},
                        state='managed',
                        reverse_attempt={'state': 'verified'},
                    )
                )
            await db.commit()
        snapshot = await read_diagnostics(factory, runtime=runtime())
        row = snapshot.scopes[0]
        assert row.queues.settlement_pending == (0 if returned else 1)
        assert row.diagnostic_ready is returned
