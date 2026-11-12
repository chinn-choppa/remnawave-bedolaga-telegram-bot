"""Explicit per-panel read-only recovery admission; not a running worker.

Commit claims before HTTP and use recovery_preflight to revalidate each hint.
No SQL lock spans HTTP. An expired lease allows another READ, never a PATCH.
Provider pauses share the collector budget with preparation and delivery;
recovery due_at additionally controls local pacing. Aggregate concurrency limits
are still required before runtime activation. No continuous worker is registered.
"""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.database.models import BsTrafficRecoverySchedule as Schedule
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_pause import record_pause, remaining_pause
from app.services.bs_traffic.recovery_backoff import RecoveryBackoff, valid_retry_delay
from app.services.bs_traffic.recovery_preflight import recover_from_panel
from app.services.bs_traffic.recovery_scan import (
    RecoveryCandidate,
    RecoveryWindow,
    _postgres,
    _scope,
    recovery_page,
    start_recovery_scan,
)


LEASE_SECONDS = 60
SPACING_SECONDS = 1
PASS_PAUSE_SECONDS = 60
RESULTS = frozenset(
    (
        'verified',
        'already_verified',
        'not_sent',
        'not_attempted',
        'in_progress',
        'unconfirmed',
        'obsolete',
        'attention',
    )
)


@dataclass(frozen=True)
class RecoveryClaim:
    panel_scope: str
    token: str
    candidate: RecoveryCandidate
    lease_until: datetime


async def register_recovery_scope(db, *, panel_scope):
    """Explicit opt-in, idempotent; never reset an existing cursor or lease."""
    _postgres(db)
    await db.execute(insert(Schedule).values(panel_scope=_scope(panel_scope)).on_conflict_do_nothing())


async def _locked(db, scope, *, skip_locked=False):
    return await db.scalar(
        select(Schedule)
        .where(Schedule.panel_scope == scope)
        .with_for_update(skip_locked=skip_locked)
        .execution_options(populate_existing=True)
    )


def _clear_lease(row):
    row.lease_token = row.lease_until = row.operation_id = None


async def claim_recovery(db, *, panel_scope) -> RecoveryClaim | None:
    """Reserve one hint; caller commits. Missing registration does no work."""
    _postgres(db)
    scope = _scope(panel_scope)
    if await remaining_pause(db, panel_scope=scope) > 0:
        return None
    row = await _locked(db, scope, skip_locked=True)
    if row is None:
        return None
    now = await db.scalar(select(func.clock_timestamp()))
    if row.due_at > now or (row.lease_until is not None and row.lease_until > now):
        return None
    _clear_lease(row)
    if row.upper_id is None:
        window = await start_recovery_scan(db, panel_scope=scope)
        row.upper_id, row.eligible_before = window.upper_id, window.eligible_before
    else:
        window = RecoveryWindow(scope, row.upper_id, row.eligible_before)
    page = await recovery_page(db, window, after_id=row.cursor, limit=1)
    now = await db.scalar(select(func.clock_timestamp()))
    if not page.items:
        row.cursor, row.upper_id, row.eligible_before = 0, None, None
        row.due_at = now + timedelta(seconds=PASS_PAUSE_SECONDS)
        await db.flush()
        return None
    candidate = page.items[0]
    row.lease_token = str(uuid4())
    row.lease_until = now + timedelta(seconds=LEASE_SECONDS)
    row.operation_id = candidate.operation_id
    await db.flush()
    return RecoveryClaim(scope, row.lease_token, candidate, row.lease_until)


async def finish_recovery(db, claim: RecoveryClaim, *, result: str, retry_after_seconds: float | None = None) -> bool:
    """Advance only this live lease; no mutation of the underlying delivery."""
    _postgres(db)
    if not isinstance(claim, RecoveryClaim) or not isinstance(result, str) or result not in RESULTS:
        raise ValueError('Explicit recovery claim and classified result required')
    if retry_after_seconds is not None and not valid_retry_delay(retry_after_seconds):
        raise ValueError('Invalid recovery retry delay')
    if retry_after_seconds is not None:
        await record_pause(db, panel_scope=claim.panel_scope, seconds=retry_after_seconds)
    row = await _locked(db, _scope(claim.panel_scope))
    now = await db.scalar(select(func.clock_timestamp()))
    if row is not None and retry_after_seconds is not None:
        try:
            deadline = now + timedelta(seconds=retry_after_seconds)
        except OverflowError:
            deadline = datetime.max.replace(tzinfo=UTC)
        row.due_at = max(row.due_at, deadline)
    if (
        row is None
        or row.lease_token != claim.token
        or row.operation_id != claim.candidate.operation_id
        or row.lease_until is None
        or row.lease_until <= now
    ):
        await db.flush()
        return False
    row.cursor = row.operation_id
    row.last_result = result
    row.due_at = max(row.due_at, now + timedelta(seconds=SPACING_SECONDS))
    _clear_lease(row)
    await db.flush()
    return True


async def run_recovery_once(session_factory, api, *, panel_scope) -> bool:
    """One explicitly invoked read-only step, no loop or implicit registration.

    True means a claim was processed, not that access was verified. Cancellation
    or unexpected failure leaves the lease. Bind api to panel_scope externally.
    """
    async with session_factory() as db, db.begin():
        claim = await claim_recovery(db, panel_scope=panel_scope)
    if claim is None:
        return False
    delay = None
    try:
        result = await recover_from_panel(session_factory, api, **asdict(claim.candidate), propagate_backoff=True)
    except RecoveryBackoff as exc:
        result, delay = 'unconfirmed', exc.retry_after_seconds
    except LedgerConflict:
        result = 'obsolete'
    async with session_factory() as db, db.begin():
        await finish_recovery(db, claim, result=result, retry_after_seconds=delay)
    return True
