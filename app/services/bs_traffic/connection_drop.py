"""One opt-in IP-based disconnect after a verified group restriction.

202 means accepted, not proven socket termination. An unknown outcome is never
replayed. The panel cannot fence a queued action against later remote changes.
"""

import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficDelivery, Subscription, User
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, require_activated_owner
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_pause import persist_panel_pause, read_panel_pause
from app.services.bs_traffic.policy import _read_policy, project_access
from app.services.bs_traffic.readback import _observed_squads
from app.services.bs_traffic.recovery_backoff import backoff_for
from app.services.bs_traffic.restriction import _locked, _reason


async def stage_drop(db, *, operation_id, projection, observed, evidence):
    if settings.BS_TRAFFIC_FORCE_DISCONNECT_ENABLED is not True or projection.preserve_grace:
        return
    groups = [
        group
        for group in ('ordinary', 'bs')
        if getattr(projection, group).mode == 'finite'
        and getattr(projection, group).state == 'exhausted'
        and set(observed).intersection(getattr(evidence.topology, f'{group}_squads'))
    ]
    if not groups:
        return
    row = await db.get(BsTrafficDelivery, operation_id)
    if row.connection_drop is None:
        row.connection_drop = {'state': 'pending', 'groups': groups, 'fingerprint': evidence.fingerprint}


async def _eligible(db, reader, keys, row, evidence):
    from app.services.bs_traffic.preparation import delivery_guard

    state, panel_id = await _locked(db, reader, keys)
    await require_activated_owner(db, keys['subscription_id'])
    now = await db.scalar(select(func.clock_timestamp()))
    sub = await db.get(Subscription, keys['subscription_id'])
    owner = await db.get(User, keys['user_id'])
    if (
        settings.BS_TRAFFIC_FORCE_DISCONNECT_ENABLED is not True
        or row.guard_hash != delivery_guard(**keys, evidence=evidence)
        or row.revision != state.revision
        or sub.is_trial
        or sub.grace_session_open
        or sub.status not in ('active', 'limited')
        or owner.status in ('blocked', 'deleted')
        or sub.end_date <= now
        or await _reason(db, sub.id) is not None
        or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
        or evidence.fingerprint != row.connection_drop['fingerprint']
    ):
        return None
    from app.database.models import GraceAccessSessionModel

    if await db.scalar(
        select(GraceAccessSessionModel.id)
        .where(GraceAccessSessionModel.subscription_id == sub.id, GraceAccessSessionModel.state != 'completed')
        .limit(1)
    ):
        return None
    projection = await project_access(db, **keys, evidence=evidence, grace_active=False)
    nodes = []
    for group in row.connection_drop['groups']:
        projected = getattr(projection, group)
        if (
            projected.mode != 'finite'
            or projected.state != 'exhausted'
            or projected.pending_bytes
            or projected.needs_review
        ):
            return None
        if set(row.squads).intersection(getattr(evidence.topology, f'{group}_squads')):
            return None
        nodes.extend(getattr(evidence.topology, f'{group}_nodes'))
    return panel_id, sorted(set(nodes))


async def drop_after_delivery(reader, *, user_id, subscription_id, account_generation, operation_id):
    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    try:
        async with asyncio.timeout(15):
            return await _drop(reader, keys, operation_id)
    except RemnaWaveAPIError as error:
        pause = backoff_for(error)
        if pause:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unknown'
    except (TimeoutError, LedgerConflict, ValueError, TypeError):
        return 'unknown'


async def _drop(reader, keys, operation_id):
    async with reader._factory() as db, db.begin():
        state, _ = await _locked(db, reader, keys)
        row = await db.get(BsTrafficDelivery, operation_id)
        if row is None or row.subscription_id != keys['subscription_id']:
            raise LedgerConflict('Drop belongs to another subscription')
        request = row.connection_drop
        if request is None:
            return 'not_required'
        if request['state'] != 'pending':
            return request['state']
        if settings.BS_TRAFFIC_FORCE_DISCONNECT_ENABLED is not True:
            row.connection_drop = dict(request, state='cancelled')
            return 'cancelled'
        if row.state != 'verified':
            return 'waiting'
        policy = _read_policy(state.access_policy)
    if await read_panel_pause(reader._factory, reader.panel_scope) > 0:
        return 'waiting'
    async with panel_slot(reader._factory, reader.panel_scope):
        evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=policy['topology']['ordinary_squads'],
            bs_squads=policy['topology']['bs_squads'],
        )
    async with reader._factory() as db, db.begin():
        await _locked(db, reader, keys)
        row = await db.get(BsTrafficDelivery, operation_id)
        if row.connection_drop != request:
            return row.connection_drop['state']
        eligible = await _eligible(db, reader, keys, row, evidence)
        if eligible is None:
            row.connection_drop = dict(request, state='cancelled')
            return 'cancelled'
        now = await db.scalar(select(func.clock_timestamp()))
        attempt = dict(
            request, state='attempted', token=str(uuid4()), deadline=(now + timedelta(seconds=10)).isoformat()
        )
        row.connection_drop = attempt
    async with reader._factory() as db, db.begin():
        await _locked(db, reader, keys)
        row = await db.get(BsTrafficDelivery, operation_id)
        if row.connection_drop != attempt:
            return 'unknown'
        async with panel_slot(reader._factory, reader.panel_scope):
            eligible = await _eligible(db, reader, keys, row, evidence)
            if eligible is None:
                row.connection_drop = dict(attempt, state='cancelled')
                return 'cancelled'
            panel_id, nodes = eligible
            account = await reader.get_split_access_snapshot(panel_id)
            now = await db.scalar(select(func.clock_timestamp()))
            if (
                account is None
                or generation_of(panel_scope=reader.panel_scope, account=account) != keys['account_generation']
                or list(_observed_squads(account)) != sorted(row.squads)
                or now >= datetime.fromisoformat(attempt['deadline'])
            ):
                row.connection_drop = dict(attempt, state='cancelled')
                return 'cancelled'
            await reader._api.request_user_connection_drop(panel_id, nodes)
            row.connection_drop = dict(attempt, state='accepted')
    return 'accepted'
