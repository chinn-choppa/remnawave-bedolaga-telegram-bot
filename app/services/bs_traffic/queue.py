"""PostgreSQL collection queue, inactive until explicit jobs are enrolled.

One job represents one read, not a permanent subscription schedule. No billing,
account writes, topology discovery or automatic enrollment happens here. The
operator/panel scope and identity map must come from a verified caller.
"""

import asyncio
import hashlib
import json
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert

from app.database.models import BsTrafficCollectionBudget as Budget, BsTrafficCollectionJob as Job
from app.external.remnawave_api import RemnaWaveAPIError, RemnaWaveTransientError
from app.services.bs_traffic.observations import begin_poll, prepare_poll, record_poll
from app.services.bs_traffic.panel_admission import PanelBusy, panel_slot
from app.services.bs_traffic.recovery_backoff import backoff_for, valid_retry_delay


LEASE_SECONDS = 60
FETCH_SECONDS = 15
MAX_ATTEMPTS = 8


@dataclass(frozen=True)
class CollectionClaim:
    job_id: int
    token: str
    poll_id: int
    panel_scope: str
    day: date
    node_uuids: tuple[str, ...]


def _scope(value):
    if not isinstance(value, str):
        raise ValueError('Invalid panel scope')
    return str(UUID(value))


def _postgres(db):
    if db.get_bind().dialect.name != 'postgresql':
        raise ValueError('Traffic collection queue requires PostgreSQL')


def collection_request_hash(*, panel_scope, day, node_uuids, identities):
    """Hash the same validated immutable identity selection as an enqueued job."""
    request = prepare_poll(panel_scope=panel_scope, day=day, node_uuids=node_uuids, identities=identities)
    canonical = {**request, 'day': day.isoformat()}
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def enqueue_job(db, *, operation_key, panel_scope, day, node_uuids, identities) -> int:
    """Caller commits a bounded immutable job; replay never rewrites its mapping."""
    _postgres(db)
    if (
        not isinstance(operation_key, str)
        or not 1 <= len(operation_key) <= 160
        or operation_key.strip() != operation_key
    ):
        raise ValueError('Invalid collection operation key')
    request = prepare_poll(panel_scope=panel_scope, day=day, node_uuids=node_uuids, identities=identities)
    request_hash = collection_request_hash(
        panel_scope=panel_scope, day=day, node_uuids=node_uuids, identities=identities
    )
    statement = insert(Job).values(
        **request, request_hash=request_hash, operation_key=operation_key, state='pending', attempts=0
    )
    await db.execute(statement.on_conflict_do_nothing(index_elements=['panel_scope', 'operation_key']))
    job = (
        await db.scalars(
            select(Job)
            .where(
                Job.panel_scope == request['panel_scope'],
                Job.operation_key == operation_key,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one()
    if job.request_hash != request_hash:
        raise ValueError('Collection operation already exists with different request')
    return job.id


def _release(job, state):
    job.state = state
    job.lease_token = None
    job.lease_until = None


async def _budget(db, scope):
    await db.execute(insert(Budget).values(panel_scope=scope).on_conflict_do_nothing())
    return (
        await db.scalars(
            select(Budget)
            .where(Budget.panel_scope == scope)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one()


async def claim_job(db, panel_scope, *, job_id=None) -> CollectionClaim | None:
    """Claim at most one due job without waiting for other workers' row locks.

    Commit before using the returned claim. Expired claims get a NEW poll ID;
    late responses are fenced by the random lease token and deadline.
    """
    _postgres(db)
    if job_id is not None and (type(job_id) is not int or job_id <= 0):
        raise ValueError('Invalid collection job identity')
    scope = _scope(panel_scope)
    budget = await _budget(db, scope)
    now = await db.scalar(select(func.clock_timestamp()))
    if budget.cooldown_until is not None and budget.cooldown_until > now:
        return None
    if budget.slot_until is not None and budget.slot_until > now:
        return None
    # A short scope-row lock serializes the check+claim across processes.
    # No lock remains held while HTTP runs; the job lease represents ownership.
    active = await db.scalar(
        select(Job.id).where(Job.panel_scope == scope, Job.state == 'leased', Job.lease_until > now).limit(1)
    )
    if active is not None:
        return None
    job = (
        await db.scalars(
            select(Job)
            .where(
                Job.panel_scope == scope,
                *([Job.id == job_id] if job_id is not None else []),
                or_(
                    and_(Job.state == 'pending', Job.due_at <= func.clock_timestamp()),
                    and_(Job.state == 'leased', Job.lease_until <= func.clock_timestamp()),
                ),
            )
            .order_by(Job.due_at, Job.id)
            .limit(1)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).first()
    if job is None:
        return None
    if job.attempts >= MAX_ATTEMPTS:
        _release(job, 'attention')
        job.last_error = 'attempts_exhausted'
        await db.flush()
        return None
    now = await db.scalar(select(func.clock_timestamp()))
    poll = await begin_poll(
        db,
        panel_scope=job.panel_scope,
        day=job.day,
        node_uuids=tuple(job.node_uuids),
        identities={int(k): v for k, v in job.identities.items()},
    )
    job.state = 'leased'
    job.lease_token = str(uuid4())
    job.lease_until = now + timedelta(seconds=LEASE_SECONDS)
    budget.slot_token = job.lease_token
    budget.slot_until = job.lease_until
    job.attempts += 1
    job.poll_id = poll.id
    job.last_error = None
    await db.flush()
    return CollectionClaim(job.id, job.lease_token, poll.id, job.panel_scope, job.day, tuple(job.node_uuids))


def _clear_slot(budget, claim):
    if budget is not None and budget.slot_token == claim.token:
        budget.slot_token = None
        budget.slot_until = None


async def _owned(db, claim):
    # Lock order is scope budget -> job -> poll, including stale/cancelled replies.
    budget = (
        await db.scalars(
            select(Budget)
            .where(Budget.panel_scope == claim.panel_scope)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).first()
    job = (
        await db.scalars(
            select(Job)
            .where(Job.id == claim.job_id, Job.panel_scope == claim.panel_scope)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).first()
    now = await db.scalar(select(func.clock_timestamp()))
    if (
        budget is None
        or budget.slot_token != claim.token
        or budget.slot_until is None
        or budget.slot_until <= now
        or job is None
        or job.state != 'leased'
        or job.panel_scope != claim.panel_scope
        or job.lease_token != claim.token
        or job.poll_id != claim.poll_id
        or job.lease_until <= now
    ):
        _clear_slot(budget, claim)
        await db.flush()
        return None
    return job


async def finish_job(db, claim, payload) -> bool:
    job = await _owned(db, claim)
    if job is None:
        return False
    await record_poll(db, claim.poll_id, payload)
    _release(job, 'complete')
    _clear_slot(await db.get(Budget, claim.panel_scope), claim)
    job.identities = {}
    job.last_error = None
    await db.flush()
    return True


async def fail_job(db, claim, *, reason: str, retry: bool, retry_after_seconds: float | None = None) -> bool:
    if (
        reason not in {'fetch_timeout', 'panel_unavailable', 'invalid_response', 'panel_rejected'}
        or type(retry) is not bool
    ):
        raise ValueError('Invalid collection failure classification')
    if retry_after_seconds is not None and not valid_retry_delay(retry_after_seconds):
        raise ValueError('Invalid retry delay')
    budget = await _budget(db, claim.panel_scope)
    job = await _owned(db, claim)
    # A provider backoff applies to this panel even if the job was cancelled
    # while HTTP was in flight. Do not release another worker's slot or revive
    # a terminal job, but never let cancellation bypass Retry-After.
    if retry_after_seconds is not None:
        observed_at = await db.scalar(select(func.clock_timestamp()))
        try:
            provider_deadline = observed_at + timedelta(seconds=retry_after_seconds)
        except OverflowError:
            provider_deadline = datetime.max.replace(tzinfo=UTC)
        if budget.cooldown_until is None or budget.cooldown_until < provider_deadline:
            budget.cooldown_until = provider_deadline
    if job is None:
        await db.flush()
        return False
    can_retry = retry and job.attempts < MAX_ATTEMPTS
    _release(job, 'pending' if can_retry else 'attention')
    _clear_slot(budget, claim)
    job.last_error = reason
    now = await db.scalar(select(func.clock_timestamp()))
    delay = min(300, 5 * 2 ** (job.attempts - 1)) if can_retry else 60
    delay = max(delay + random.uniform(0, delay / 4), retry_after_seconds or 0)
    try:
        next_allowed = now + timedelta(seconds=delay)
    except OverflowError:
        next_allowed = datetime.max.replace(tzinfo=UTC)
    job.due_at = next_allowed
    if budget.cooldown_until is None or budget.cooldown_until < next_allowed:
        budget.cooldown_until = next_allowed
    await db.flush()
    return True


async def cancel_job(db, job_id, *, panel_scope) -> bool:
    job = (
        await db.scalars(
            select(Job)
            .where(Job.id == job_id, Job.panel_scope == _scope(panel_scope))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).first()
    if job is None or job.state not in ('pending', 'leased'):
        return False
    _release(job, 'cancelled')
    await db.flush()
    return True


async def run_once(session_factory, api, panel_scope, *, job_id=None) -> bool:
    """Admit before claiming, so capacity pressure consumes no job attempt."""
    try:
        async with panel_slot(session_factory, panel_scope):
            return await _run_admitted_once(session_factory, api, panel_scope, job_id=job_id)
    except PanelBusy:
        return False
    except TimeoutError:
        # The outer admission deadline includes claim/finalization, not just
        # HTTP. Keep any committed lease for recovery; never kill the worker
        # or immediately replay a request whose completion is unknown.
        return False


async def _run_admitted_once(session_factory, api, panel_scope, *, job_id=None) -> bool:
    """Process one job; only the admission transaction spans HTTP. Cancellation leaves the job lease."""
    async with session_factory() as db, db.begin():
        claim = await claim_job(db, panel_scope, job_id=job_id)
    if claim is None:
        return False
    retry_after = None
    try:
        async with asyncio.timeout(FETCH_SECONDS):
            payload = await api.get_daily_nodes_usage(
                claim.node_uuids, claim.day, max_response_bytes=8 * 1024**2, timeout_seconds=FETCH_SECONDS
            )
        async with session_factory() as db, db.begin():
            await finish_job(db, claim, payload)
        return True
    except TimeoutError:
        reason, retry = 'fetch_timeout', True
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        retry_after = pause.retry_after_seconds if pause is not None else None
        reason, retry = (
            ('panel_unavailable', True)
            if isinstance(exc, RemnaWaveTransientError) or exc.status_code in (429, 500, 502, 503, 504)
            else ('panel_rejected', False)
        )
    except ValueError:
        reason, retry = 'invalid_response', False
    async with session_factory() as db, db.begin():
        await fail_job(db, claim, reason=reason, retry=retry, retry_after_seconds=retry_after)
    return True


async def run_worker(session_factory, api, panel_scope, stop: asyncio.Event) -> None:
    """Drain explicit jobs, one request at a time; stop cleanly between requests.

    In-flight HTTP is bounded to15s. Forced cancellation leaves the lease for
    recovery; no artificial success, zero observations or error-body logging.
    SQL/programming errors propagate to the lifecycle supervisor, not a busy loop.
    """
    scope = _scope(panel_scope)
    while not stop.is_set():
        if not await run_once(session_factory, api, scope):
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except TimeoutError:
                pass


async def queue_health(db, panel_scope) -> dict:
    """Bounded aggregate diagnostics without account maps, tokens or payloads."""
    _postgres(db)
    scope = _scope(panel_scope)
    states = dict(
        (await db.execute(select(Job.state, func.count()).where(Job.panel_scope == scope).group_by(Job.state))).all()
    )
    errors = dict(
        (
            await db.execute(
                select(Job.last_error, func.count())
                .where(
                    Job.panel_scope == scope,
                    Job.last_error.is_not(None),
                    Job.state.in_(('pending', 'attention')),
                )
                .group_by(Job.last_error)
            )
        ).all()
    )
    expired = await db.scalar(
        select(func.count())
        .select_from(Job)
        .where(
            Job.panel_scope == scope,
            Job.state == 'leased',
            Job.lease_until <= func.clock_timestamp(),
        )
    )
    oldest = await db.scalar(select(func.min(Job.due_at)).where(Job.panel_scope == scope, Job.state == 'pending'))
    slot_until = await db.scalar(select(Budget.slot_until).where(Budget.panel_scope == scope))
    cooldown = await db.scalar(select(Budget.cooldown_until).where(Budget.panel_scope == scope))
    return {
        'states': states,
        'errors': errors,
        'expired_leases': expired,
        'oldest_due_at': oldest,
        'cooldown_until': cooldown,
        'in_flight_until': slot_until,
    }
