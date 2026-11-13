"""Paid renewal exits an existing split overlay through durable full restore."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from app.database.models import (
    BsTrafficGraceIntent,
    BsTrafficLedger,
    BsTrafficRenewalIntent,
    GraceAccessSessionModel,
    Transaction,
    User,
)
from app.services.bs_traffic.renewal import renew_from_balance
from app.services.grace_access_runtime import SQLAlchemyGraceBillingGateway, SQLAlchemyGraceSessionStore
from app.services.grace_access_service import GracePanelSnapshot, GraceReason, GraceSessionState
from tests.database.test_bs_paid_renewal_postgres import prepared as checkout_setup
from tests.database.test_bs_traffic_grace_transition_postgres import grace_session
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session
from tests.services.test_grace_split_snapshot import context


pytestmark = pytest.mark.postgres
OVERLAY = '77777777-7777-4777-8777-777777777777'


async def prepared(db, monkeypatch, *, expired_overlay=False, echoed=False):
    from app.config import settings

    monkeypatch.setattr(type(settings), 'is_multi_tariff_enabled', lambda self: True)
    reader, keys, sub = await checkout_setup(db, monkeypatch)
    sub.end_date = datetime.now(UTC) - timedelta(minutes=1)
    sub.status = 'expired'
    sub.connected_squads = [ORDINARY]
    sub.traffic_limit_gb = 10
    await db.flush()
    ledger = await db.get(BsTrafficLedger, 1)
    current = reader._api.get_split_grace_snapshot.return_value
    current = replace(
        current,
        status='EXPIRED',
        expire_at=sub.end_date,
        hwid_device_limit=sub.device_limit,
        active_internal_squads=[{'uuid': ORDINARY}],
    )
    before = GracePanelSnapshot(
        current.id,
        current.status,
        current.expire_at,
        current.traffic_limit_bytes,
        current.used_traffic_bytes,
        (ORDINARY,),
        last_traffic_reset_at=current.last_traffic_reset_at,
    )
    source = grace_session()
    source = replace(
        source,
        reason=GraceReason.EXPIRED,
        billing_before=await SQLAlchemyGraceBillingGateway(db).get_subscription(1),
        panel_before=before,
        overlay=replace(
            source.overlay, squad_uuids=(OVERLAY,), traffic_limit_bytes=current.used_traffic_bytes + 2 * 1024**3
        ),
        split_before=context(
            panel_scope=reader.panel_scope,
            account_generation=keys['account_generation'],
            ledger_revision=ledger.revision,
            topology_fingerprint=ledger.access_policy['fingerprint'],
            tariff_id=sub.tariff_id,
        ),
    )
    if expired_overlay:
        before_expiry = datetime.now(UTC) - timedelta(hours=1)
        source = replace(
            source,
            started_at=before_expiry - timedelta(hours=1),
            grace_until=before_expiry,
            overlay=replace(source.overlay, expire_at=before_expiry),
        )
    store = SQLAlchemyGraceSessionStore(db)
    saved = await store.create(source)
    saved = await store.save(replace(saved, state=GraceSessionState.ACTIVE))
    await db.commit()
    remote = {
        'state': replace(
            current,
            status='ACTIVE',
            expire_at=saved.overlay.expire_at,
            traffic_limit_bytes=saved.overlay.traffic_limit_bytes,
            active_internal_squads=[{'uuid': OVERLAY}],
        )
    }
    reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

    async def write(target):
        remote['state'] = target

    reader._api.restore_split_user_once = AsyncMock(side_effect=write)
    if echoed:
        sub.end_date = saved.overlay.expire_at
        sub.traffic_limit_gb = saved.overlay.traffic_limit_bytes // 1024**3
        sub.connected_squads = [OVERLAY]
        sub.status = 'active'
        await db.commit()
    result = await renew_from_balance(
        db, **keys, period_days=30, expected_price_kopeks=2000, operation_key='renew:grace'
    )
    await db.commit()
    return reader, dict(keys, operation_id=result.intent_id), remote, saved.id, result


async def test_paid_renewal_restores_and_completes_overlay_once(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, grace_id, receipt = await prepared(db, monkeypatch)
        used = remote['state'].used_traffic_bytes
        assert await advance_paid_renewal(reader, **keys) == 'verified'
        assert remote['state'].expire_at == receipt.new_end_date
        assert remote['state'].traffic_limit_bytes == 0 and remote['state'].traffic_limit_strategy == 'NO_RESET'
        assert remote['state'].used_traffic_bytes == used
        assert {item['uuid'] for item in remote['state'].active_internal_squads} == {ORDINARY, BS}
        grace = await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)
        assert grace.state == 'completed' and grace.completion_reason == 'paid'
        assert (await db.get(User, 1)).balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        await db.rollback()
        assert await advance_paid_renewal(reader, **keys) == 'already_verified'
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('applied', [True, False])
async def test_unknown_grace_write_recovers_without_second_patch(postgres_database, monkeypatch, applied):
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, grace_id, _ = await prepared(db, monkeypatch)

        async def uncertain(target):
            if applied:
                remote['state'] = target
            remote['unavailable_once'] = True
            raise TimeoutError

        async def read(_):
            if remote.pop('unavailable_once', False):
                raise TimeoutError
            return remote['state']

        reader._api.get_split_grace_snapshot.side_effect = read
        reader._api.restore_split_user_once.side_effect = uncertain
        assert await advance_paid_renewal(reader, **keys) == 'unconfirmed'
        result = await advance_paid_renewal(reader, **keys)
        assert result == ('verified' if applied else 'unconfirmed')
        reader._api.restore_split_user_once.assert_awaited_once()
        # The recovery worker committed through its own session.
        grace = await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)
        assert grace.state == ('completed' if applied else 'restoring')


async def test_modified_overlay_is_not_overwritten(postgres_database, monkeypatch):
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, grace_id, _ = await prepared(db, monkeypatch)
        remote['state'] = replace(remote['state'], traffic_limit_bytes=123)
        assert await advance_paid_renewal(reader, **keys) == 'source_changed'
        reader._api.restore_split_user_once.assert_not_awaited()
        assert (await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)).state == 'active'
        assert (await db.get(BsTrafficRenewalIntent, keys['operation_id'])).state == 'pending'
        assert await db.scalar(select(func.count()).select_from(BsTrafficGraceIntent)) == 0


@pytest.mark.parametrize('status', ['LIMITED', 'EXPIRED'])
async def test_exhausted_or_expired_overlay_restores_current_paid_period(postgres_database, monkeypatch, status):
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, _, receipt = await prepared(db, monkeypatch, expired_overlay=status == 'EXPIRED')
        remote['state'] = replace(remote['state'], status=status)
        assert await advance_paid_renewal(reader, **keys) == 'verified'
        assert remote['state'].expire_at == receipt.new_end_date
        assert remote['state'].status == 'ACTIVE'


@pytest.mark.parametrize('fault', ['foreign', 'disabled', 'newer_grace', 'missing_provenance', 'missing_payment'])
async def test_unrelated_or_incomplete_grace_cannot_be_completed(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, grace_id, _ = await prepared(db, monkeypatch)
        grace = await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)
        if fault == 'foreign':
            keys['user_id'] = 2
        elif fault == 'disabled':
            (await db.get(User, 1)).status = 'blocked'
        elif fault == 'newer_grace':
            grace.started_at = datetime.now(UTC) + timedelta(seconds=1)
        elif fault == 'missing_provenance':
            grace.snapshot_version = 3
            before = dict(grace.billing_before)
            before.pop('split_access', None)
            grace.billing_before = before
        else:
            receipt = await db.get(BsTrafficRenewalIntent, keys['operation_id'])
            receipt.transaction_id = None
        await db.commit()
        assert await advance_paid_renewal(reader, **keys) in ('obsolete', 'pending_receipt')
        reader._api.restore_split_user_once.assert_not_awaited()
        await db.refresh(grace)
        assert grace.state == 'active'


async def test_parallel_paid_grace_workers_send_once(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _, _ = await prepared(db, monkeypatch)
        results = await asyncio.gather(*(advance_paid_renewal(reader, **keys) for _ in range(2)))
        assert 'verified' in results
        reader._api.restore_split_user_once.assert_awaited_once()
        assert await advance_paid_renewal(reader, **keys) == 'already_verified'


async def test_unresolved_restore_blocks_new_financial_operation(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal, require_no_unresolved_grace_restore

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, _, _ = await prepared(db, monkeypatch)
        reader._api.restore_split_user_once.side_effect = TimeoutError
        assert await advance_paid_renewal(reader, **keys) == 'unconfirmed'
        with pytest.raises(LedgerConflict, match='restoration'):
            await require_no_unresolved_grace_restore(db, 1)
        owner = {key: value for key, value in keys.items() if key != 'operation_id'}
        with pytest.raises(LedgerConflict, match='restoration'):
            await renew_from_balance(
                db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='renew:grace-second'
            )
        replay = await renew_from_balance(
            db, **owner, period_days=30, expected_price_kopeks=2000, operation_key='renew:grace'
        )
        assert replay.replayed and replay.intent_id == keys['operation_id']
        assert (await db.get(User, 1)).balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_applied_restore_survives_late_paid_usage_revision(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import open_usage, resolve_usage
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, remote, grace_id, _ = await prepared(db, monkeypatch)

        async def uncertain(target):
            remote['state'] = target
            remote['unavailable_once'] = True
            raise TimeoutError

        async def read(_):
            if remote.pop('unavailable_once', False):
                raise TimeoutError
            return remote['state']

        reader._api.restore_split_user_once.side_effect = uncertain
        reader._api.get_split_grace_snapshot.side_effect = read
        assert await advance_paid_renewal(reader, **keys) == 'unconfirmed'
        owner = {key: value for key, value in keys.items() if key != 'operation_id'}
        now = datetime.now(UTC)
        usage = await open_usage(
            db,
            **owner,
            group='ordinary',
            source_key='synthetic-late-paid-usage',
            amount_bytes=1,
            interval_start=now - timedelta(microseconds=1),
            interval_end=now,
        )
        await resolve_usage(db, **owner, usage_id=usage, decision='charge')
        await db.commit()
        assert await advance_paid_renewal(reader, **keys) == 'verified'
        reader._api.restore_split_user_once.assert_awaited_once()
        assert (await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)).state == 'completed'


async def test_restart_after_restoring_checkpoint_finishes_same_payment(postgres_database, monkeypatch):
    from app.services.bs_traffic import renewal_grace

    async with postgres_session(postgres_database, TABLES) as db:
        reader, keys, _, grace_id, _ = await prepared(db, monkeypatch)
        original = renewal_grace.stage_restore

        async def crash(*args, **kwargs):
            raise RuntimeError('synthetic process interruption after durable restoring checkpoint')

        monkeypatch.setattr(renewal_grace, 'stage_restore', crash)
        with pytest.raises(RuntimeError, match='synthetic process interruption'):
            await renewal_grace.advance_paid_renewal(reader, **keys)
        # Check the durable checkpoint, not this fixture's cached active row.
        assert (await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)).state == 'restoring'
        assert await db.scalar(select(func.count()).select_from(BsTrafficGraceIntent)) == 0
        await db.rollback()
        monkeypatch.setattr(renewal_grace, 'stage_restore', original)
        assert await renewal_grace.advance_paid_renewal(reader, **keys) == 'verified'
        reader._api.restore_split_user_once.assert_awaited_once()
        assert (await db.get(User, 1)).balance_kopeks == 8000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_echoed_overlay_is_repaired_before_paid_period_calculation(postgres_database, monkeypatch):
    from app.database.models import Subscription
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from app.services.grace_access_codec import _model_to_session

    async with postgres_session(postgres_database, TABLES) as db:
        started = datetime.now(UTC)
        reader, keys, remote, grace_id, receipt = await prepared(db, monkeypatch, echoed=True)
        finished = datetime.now(UTC)
        source = _model_to_session(await db.get(GraceAccessSessionModel, grace_id, populate_existing=True))
        sub = await db.get(Subscription, 1)
        assert receipt.old_end_date == source.billing_before.end_at
        assert started + timedelta(days=30) <= receipt.new_end_date <= finished + timedelta(days=30)
        assert receipt.new_end_date < source.overlay.expire_at + timedelta(days=30)
        assert sub.connected_squads == [ORDINARY]
        assert sub.traffic_limit_gb == 10
        await db.rollback()
        assert await advance_paid_renewal(reader, **keys) == 'verified'
        assert remote['state'].expire_at == receipt.new_end_date
        assert (await db.get(User, 1)).balance_kopeks == 8000
