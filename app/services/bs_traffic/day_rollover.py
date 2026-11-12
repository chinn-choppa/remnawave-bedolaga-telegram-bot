"""Explicit daily enrollment without discarding already reported traffic.

Unlike initial attachment, rollover preserves the first counter as raw pending
usage. It does not infer when bytes were consumed or charge a fresh package.
Settlement/write-off is separate and performed by the fleet coordinator.
"""

from datetime import UTC, timedelta

from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficPoll, BsTrafficUsageCursor as Cursor
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.intake import _cursor, _key, _observation, stage_observation
from app.services.bs_traffic.ledger import LedgerConflict, _lock, require_activated_owner, resolve_usage
from app.services.bs_traffic.policy import _read_policy


async def stage_day_rollover(db, *, user_id, subscription_id, account_generation, node_uuid, day, evidence):
    """Atomically open one consecutive node/day and preserve its initial delta.

    Caller owns commit. Existing cursors are never reset or consumed again by
    this enrollment operation. A missing previous day/node requires explicit
    reconciliation, not an invented baseline. Ordinary and BS stay independent.
    """
    node = _key(node_uuid, day)
    target = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with db.begin_nested():
        state, _ = await _lock(db, **target)
        await require_activated_owner(db, subscription_id)
        now = await db.scalar(select(func.clock_timestamp()))
        if day > now.astimezone(UTC).date():
            raise LedgerConflict('Cannot enroll a future traffic day')
        existing = await _cursor(db, subscription_id, node, day)
        if existing is not None:
            if existing.account_generation != account_generation:
                raise LedgerConflict('Daily cursor belongs to another account')
            return None
        previous = await _cursor(db, subscription_id, node, day - timedelta(days=1))
        if (
            previous is None
            or previous.account_generation != account_generation
            or previous.ownership_cycle != state.ownership_cycle
            or previous.needs_review
        ):
            raise LedgerConflict('Consecutive confirmed daily cursor required')
        policy = _read_policy(state.access_policy)
        group = previous.traffic_group
        if (
            policy[f'{group}_mode'] != 'finite'
            or node not in policy['topology'][f'{group}_nodes']
            or not isinstance(evidence, TopologyEvidence)
            or not evidence.is_fresh(now, panel_identity=state.panel_scope)
            or evidence.fingerprint != policy['fingerprint']
        ):
            raise LedgerConflict('Current finite node topology must be confirmed')
        raw = await _observation(db, state, node, day)
        poll = await db.get(BsTrafficPoll, raw.latest_poll_id)
        if (
            raw.needs_review
            or poll is None
            or poll.panel_scope != state.panel_scope
            or poll.day != day
            or not isinstance(poll.response_hash, str)
            or len(poll.response_hash) != 64
            or node not in (poll.node_uuids or [])
            or poll.created_at > now
            or poll.created_at.astimezone(UTC).date() < day
        ):
            raise LedgerConflict('Completed unambiguous source poll required')
        provenance = None
        if previous.bootstrap_evidence is not None:
            provenance = dict(
                previous.bootstrap_evidence,
                day=day.isoformat(),
                poll_id=poll.id,
                response_hash=poll.response_hash,
                sample_started_at=poll.created_at.isoformat(),
            )
            if provenance.get('first_observed_poll_id') is None and raw.high_water_bytes:
                provenance.update(first_observed_poll_id=poll.id, first_observed_day=day.isoformat())
        db.add(
            Cursor(
                subscription_id=subscription_id,
                ownership_cycle=state.ownership_cycle,
                node_uuid=node,
                day=day,
                account_generation=account_generation,
                traffic_group=group,
                baseline_bytes=0,
                processed_bytes=0,
                needs_review=False,
                bootstrap_evidence=provenance,
            )
        )
        await db.flush()
        # Full day bucket is evidence, not a fabricated consumption timestamp.
        usage_id = await stage_observation(db, **target, node_uuid=node, day=day)
        if usage_id is not None and settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is True:
            await resolve_usage(db, **target, usage_id=usage_id, decision='write_off')
        return usage_id
