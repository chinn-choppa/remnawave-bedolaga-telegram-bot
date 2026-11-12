"""Durable no-reset account revocation independent of usage/topology telemetry.

Only expired or explicitly blocked accounts qualify. Open grace owns its own
revocation. An unknown write remains a fence against new entitlements and is
reconciled by GET, never blindly retried.
"""

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta
from uuid import uuid4

import structlog
from sqlalchemy import exists, func, select

from app.database.models import BsTrafficPanelBinding, GraceAccessSessionModel, Subscription, User
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.grace_intent import _decode, _encode
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, require_activated_owner
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_pause import persist_panel_pause, read_panel_pause
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.panel_sync.writer import patch_split_restriction_target


logger = structlog.get_logger(__name__)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def _reason(db, subscription_id):
    row = (
        await db.execute(
            select(
                Subscription.grace_session_open,
                Subscription.status,
                Subscription.end_date,
                User.status,
                exists().where(
                    GraceAccessSessionModel.subscription_id == subscription_id,
                    GraceAccessSessionModel.state != 'completed',
                ),
                func.clock_timestamp(),
            )
            .join(User, User.id == Subscription.user_id)
            .where(Subscription.id == subscription_id)
        )
    ).one_or_none()
    if row is None:
        raise LedgerConflict('Restriction subscription disappeared')
    grace_marker, sub_status, end_at, owner_status, open_grace, now = row
    if grace_marker or open_grace:
        return None
    if owner_status in ('blocked', 'deleted') or sub_status == 'disabled':
        return 'blocked'
    if end_at is not None and end_at <= now:
        return 'expired'
    return None


async def _read(reader, panel_id, generation):
    async with panel_slot(reader._factory, reader.panel_scope):
        current = await reader.get_split_grace_snapshot(panel_id)
    if current is None or generation_of(panel_scope=reader.panel_scope, account=current) != generation:
        raise LedgerConflict('Restriction account identity changed')
    return current


async def restrict_account(reader, *, user_id, subscription_id, account_generation):
    target_keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    await reader.assert_current()
    if await read_panel_pause(reader._factory, reader.panel_scope) > 0:
        return 'deferred'
    try:
        async with asyncio.timeout(15):
            return await _restrict(reader, target_keys)
    except RemnaWaveAPIError as error:
        pause = backoff_for(error)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
    except LedgerConflict as error:
        logger.debug('Split restriction deferred', reason=str(error))
        return 'unconfirmed'
    except (TimeoutError, ValueError, TypeError):
        return 'unconfirmed'


async def _locked(db, reader, keys):
    state, panel_id = await _recovery_lock(db, keys['user_id'], keys['subscription_id'], keys['account_generation'])
    await require_activated_owner(db, keys['subscription_id'], allow_restriction=True)
    if state.panel_scope != reader.panel_scope:
        raise LedgerConflict('Restriction scope changed')
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(
            BsTrafficPanelBinding.panel_scope == reader.panel_scope,
            BsTrafficPanelBinding.endpoint_hash == reader.endpoint_hash,
            BsTrafficPanelBinding.retired_at.is_(None),
        )
        .with_for_update(read=True)
    )
    if binding is None:
        raise LedgerConflict('Restriction binding is obsolete')
    return state, panel_id


async def _restrict(reader, keys):
    factory = reader._factory
    async with factory() as db, db.begin():
        state, panel_id = await _locked(db, reader, keys)
        reason = await _reason(db, keys['subscription_id'])
        prior = state.restriction_attempt
        revision = state.revision
        if reason is None and not (prior and prior['state'] == 'attempted'):
            return 'not_required'
    current = await _read(reader, panel_id, keys['account_generation'])
    if prior and prior['state'] == 'attempted':
        expected = _decode(prior['target'], prior['target_hash'], paid=False)
        if not full_restore_confirmed(current, expected):
            return 'unconfirmed'
        async with factory() as db, db.begin():
            state, _ = await _locked(db, reader, keys)
            if state.restriction_attempt != prior:
                return 'unconfirmed'
            state.restriction_attempt = dict(prior, state='verified')
        return 'verified'
    target = replace(current, status='DISABLED', active_internal_squads=[], external_squad_uuid=None)
    token = str(uuid4())
    async with factory() as db, db.begin():
        state, _ = await _locked(db, reader, keys)
        if (
            state.revision != revision
            or state.restriction_attempt != prior
            or await _reason(db, keys['subscription_id']) != reason
        ):
            return 'not_required'
        if full_restore_confirmed(current, target):
            return 'verified'
        now = await db.scalar(select(func.clock_timestamp()))
        source, encoded = _encode(current, paid=False), _encode(target, paid=False)
        state.revision += 1
        attempt = dict(
            state='attempted',
            token=token,
            revision=state.revision,
            reason=reason,
            source=source,
            source_hash=_hash(source),
            target=encoded,
            target_hash=_hash(encoded),
            deadline=(now + timedelta(seconds=10)).isoformat(),
        )
        state.restriction_attempt = attempt
    # Commit the single-attempt fence before network I/O, then revalidate under
    # the same owner/grace locks used by payments and grace activation.
    async with factory() as db, db.begin():
        state, _ = await _locked(db, reader, keys)
        if state.restriction_attempt != attempt:
            return 'unconfirmed'
        now = await db.scalar(select(func.clock_timestamp()))
        if (
            state.revision != attempt['revision']
            or now >= datetime.fromisoformat(attempt['deadline'])
            or await _reason(db, keys['subscription_id']) != reason
        ):
            state.restriction_attempt = dict(attempt, state='not_sent')
            return 'not_required'
        fresh = await _read(reader, panel_id, keys['account_generation'])
        if not full_restore_confirmed(fresh, current):
            state.restriction_attempt = dict(attempt, state='not_sent')
            return 'unconfirmed'
        await reader.assert_current()
        async with panel_slot(factory, reader.panel_scope):
            now = await db.scalar(select(func.clock_timestamp()))
            if (
                now >= datetime.fromisoformat(attempt['deadline'])
                or await _reason(db, keys['subscription_id']) != reason
            ):
                state.restriction_attempt = dict(attempt, state='not_sent')
                return 'not_required'
            await patch_split_restriction_target(reader._api, target=target)
    # A response is not confirmation. Recovery only compares a fresh GET.
    return await _restrict(reader, keys)
