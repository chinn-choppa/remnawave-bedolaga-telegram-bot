"""Stored group modes and conservative access masks; no panel writes.

None in a group's squads means preserve its current access, not clear it.
An empty tuple means an explicit restriction of THAT group. A delivery
coordinator must merge masks against the verified current projection and
serialize with grace/payment changes before invoking panel_sync.
"""

from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficLedger, Subscription, Tariff, User
from app.services.bs_traffic.access import GroupAccess
from app.services.bs_traffic.base_extension import effective_expiry
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.grace import grace_candidate_reason
from app.services.bs_traffic.ledger import LedgerConflict, _lock, _require_no_pending_transfer, summary
from app.services.bs_traffic.mode_boundary import finite_since
from app.services.bs_traffic.topology import SplitTopology
from app.services.grace_access_service import GraceReason


MODES = ('disabled', 'finite', 'unlimited')


@dataclass(frozen=True)
class GroupProjection:
    mode: str
    remaining_bytes: int | None
    pending_bytes: int
    needs_review: bool
    state: str
    squads: tuple[str, ...] | None


@dataclass(frozen=True)
class AccessProjection:
    revision: int
    account_generation: str
    topology_fingerprint: str
    ordinary: GroupProjection
    bs: GroupProjection
    preserve_grace: bool = False
    grace_candidate: GraceReason | None = None


def _topology(value):
    if not isinstance(value, SplitTopology):
        raise LedgerConflict('Validated split topology is required')
    result = {}
    for key, items in asdict(value).items():
        limit = 64 if key.endswith('squads') else 100
        if not isinstance(items, (tuple, list)) or not 1 <= len(items) <= limit:
            raise LedgerConflict('Topology group is empty or exceeds its budget')
        try:
            values = tuple(str(UUID(item)) for item in items if isinstance(item, str))
        except ValueError:
            raise LedgerConflict('Invalid topology identity') from None
        if len(values) != len(items) or values != tuple(sorted(set(values))):
            raise LedgerConflict('Topology identities must be canonical, unique and sorted')
        result[key] = list(values)
    if set(result['ordinary_squads']) & set(result['bs_squads']) or set(result['ordinary_nodes']) & set(
        result['bs_nodes']
    ):
        raise LedgerConflict('Traffic groups must be separated by squads and nodes')
    return result


def _fingerprint(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


async def configure_groups(
    db, *, user_id, subscription_id, account_generation, ordinary_mode, bs_mode, evidence, expected_revision
):
    """Atomically save both modes, without modifying grants or applying access."""
    if ordinary_mode not in MODES or bs_mode not in MODES:
        raise LedgerConflict('Explicit group modes are required')
    if type(expected_revision) is not int or expected_revision < 0:
        raise LedgerConflict('Invalid policy revision')
    if not isinstance(evidence, TopologyEvidence) or not _fingerprint(evidence.fingerprint):
        raise LedgerConflict('Validated topology evidence is required')
    topology = _topology(evidence.topology)
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    tariff_id = await db.scalar(select(Subscription.tariff_id).where(Subscription.id == subscription_id))
    if tariff_id is None:
        raise LedgerConflict('Split access requires a tariff subscription')
    now = await db.scalar(select(func.clock_timestamp()))
    if state.revision != expected_revision:
        raise LedgerConflict('Subscription changed while configuring traffic groups')
    if not evidence.is_fresh(now, panel_identity=state.panel_scope):
        raise LedgerConflict('Topology evidence is stale or belongs to another panel')
    policy = {
        'version': 1,
        'tariff_id': tariff_id,
        'ordinary_mode': ordinary_mode,
        'bs_mode': bs_mode,
        'fingerprint': evidence.fingerprint,
        'topology': topology,
    }
    if isinstance(state.access_policy, dict) and 'bs_purchase_enabled' in state.access_policy:
        policy['bs_purchase_enabled'] = _read_policy(state.access_policy)['bs_purchase_enabled']
        if 'bs_drain_until' in state.access_policy:
            policy['bs_drain_until'] = state.access_policy['bs_drain_until']
    if isinstance(state.access_policy, dict) and 'finite_since' in state.access_policy:
        if state.access_policy.get('topology') != topology:
            raise LedgerConflict('Mode boundary cannot move across traffic topology')
        policy['finite_since'] = dict(state.access_policy['finite_since'])
    if state.access_policy != policy:
        await _require_no_pending_transfer(db, subscription_id)
        state.access_policy = policy
        state.revision += 1
        await db.flush()
    return state.revision


def _read_policy(value):
    if not isinstance(value, dict) or value.get('version') != 1:
        raise LedgerConflict('Split access policy is not configured')
    if type(value.get('tariff_id')) is not int or value['tariff_id'] <= 0:
        raise LedgerConflict('Invalid stored tariff binding')
    if 'finite_since' in value:
        if not isinstance(value['finite_since'], dict) or not set(value['finite_since']) <= {'ordinary', 'bs'}:
            raise LedgerConflict('Invalid finite mode boundary')
        for group in ('ordinary', 'bs'):
            finite_since(value, group)
    if 'bs_purchase_enabled' in value and type(value['bs_purchase_enabled']) is not bool:
        raise LedgerConflict('Invalid stored BS purchase state')
    if 'bs_drain_until' in value:
        try:
            deadline = datetime.fromisoformat(value['bs_drain_until'])
            if deadline.tzinfo is None or deadline.utcoffset() is None or value.get('bs_purchase_enabled') is not False:
                raise ValueError
        except (TypeError, ValueError):
            raise LedgerConflict('Invalid stored BS drain deadline') from None
    if any(value.get(f'{group}_mode') not in MODES for group in ('ordinary', 'bs')):
        raise LedgerConflict('Invalid stored group mode')
    if not _fingerprint(value.get('fingerprint')) or not isinstance(value.get('topology'), dict):
        raise LedgerConflict('Invalid stored topology')
    try:
        topology = SplitTopology(**value['topology'])
    except TypeError:
        raise LedgerConflict('Invalid stored topology') from None
    _topology(topology)
    return value


async def set_bs_purchase_enabled(db, *, user_id, subscription_id, account_generation, enabled, expected_revision):
    """Fence new BS sales without changing paid grants or access.

    Internal transition primitive, not automatic tariff enrollment/retirement.
    Caller commits; only an activated owner may change the state. Revision
    invalidates previously prepared access writes. Re-enabling grants nothing.
    """
    from app.services.bs_traffic.ledger import require_activated_owner

    if type(enabled) is not bool or type(expected_revision) is not int or expected_revision < 0:
        raise LedgerConflict('Explicit BS purchase state and revision required')
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    await require_activated_owner(db, subscription_id)
    if state.revision != expected_revision:
        raise LedgerConflict('Subscription changed while configuring BS purchases')
    policy = _read_policy(state.access_policy)
    if policy.get('bs_purchase_enabled', True) != enabled:
        changed = dict(policy, bs_purchase_enabled=enabled)
        if enabled:
            changed.pop('bs_drain_until', None)
        else:
            paid_end = await db.scalar(select(Subscription.end_date).where(Subscription.id == subscription_id))
            if paid_end is None or paid_end.tzinfo is None:
                raise LedgerConflict('Paid subscription horizon is required before stopping BS sales')
            package_end = await db.scalar(
                select(
                    func.max(
                        func.least(
                            effective_expiry(BsTrafficAllowance),
                            func.coalesce(BsTrafficAllowance.retired_at, effective_expiry(BsTrafficAllowance)),
                        )
                    )
                ).where(
                    BsTrafficAllowance.subscription_id == subscription_id,
                    BsTrafficAllowance.traffic_group == 'bs',
                    BsTrafficAllowance.remaining_bytes > 0,
                )
            )
            # Ordinary renewals must never extend a previously paid BS period.
            changed['bs_drain_until'] = max(paid_end, package_end or paid_end).astimezone(UTC).isoformat()
        state.access_policy = changed
        state.revision += 1
        await db.flush()
    return state.revision


def effective_group_mode(policy, group, now):
    """Unlimited BS is bounded by its frozen paid period during draining."""
    mode = policy[f'{group}_mode']
    if (
        group == 'bs'
        and mode == 'unlimited'
        and policy.get('bs_purchase_enabled') is False
        and policy.get('bs_drain_until') is not None
        and now >= datetime.fromisoformat(policy['bs_drain_until'])
    ):
        return 'disabled'
    return mode


async def capture_bs_drain_before_extension(db, subscription):
    """Reconcile BS sales before changing expiry; caller owns the transaction.

    Ordinary subscriptions do not enter this path. Owner/subscription/ledger
    locks precede the tariff shared lock, matching purchase and transfer order.
    The shared lock serializes renewal with an operator's disable transaction.
    """
    if getattr(subscription, 'traffic_accounting_mode', None) not in ('split', 'split_pending'):
        return
    generation = await db.scalar(
        select(BsTrafficLedger.account_generation).where(BsTrafficLedger.subscription_id == subscription.id)
    )
    state, _ = await _lock(db, subscription.user_id, subscription.id, generation)
    await db.refresh(subscription, ['end_date', 'tariff_id', 'traffic_accounting_mode'])
    if subscription.traffic_accounting_mode != 'split':
        raise LedgerConflict('Split transition must complete before extension')
    from app.services.bs_traffic.ledger import require_activated_owner
    from app.services.bs_traffic.tariff_settings import BsTrafficSettings, require_matching_traffic_policy

    await require_activated_owner(db, subscription.id)
    tariff = await db.scalar(
        select(Tariff)
        .where(Tariff.id == subscription.tariff_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    try:
        config = BsTrafficSettings.model_validate(tariff.bs_traffic_settings if tariff else None)
    except ValueError as error:
        raise LedgerConflict('Split tariff settings require reconciliation before extension') from error
    if not config.enabled:
        if config.disabled_at is None:
            raise LedgerConflict('BS disable cutoff is unknown; reconcile before extension')
        await set_bs_purchase_enabled(
            db,
            user_id=subscription.user_id,
            subscription_id=subscription.id,
            account_generation=generation,
            enabled=False,
            expected_revision=state.revision,
        )
    elif _read_policy(state.access_policy).get('bs_purchase_enabled') is False:
        # Reopening the same tariff restores sales, not allowance balances.
        # Changed groups/modes require their separate topology reconciliation.
        try:
            require_matching_traffic_policy(tariff, _read_policy(state.access_policy))
        except ValueError as error:
            raise LedgerConflict('BS tariff changed while sales were disabled') from error
        await set_bs_purchase_enabled(
            db,
            user_id=subscription.user_id,
            subscription_id=subscription.id,
            account_generation=generation,
            enabled=True,
            expected_revision=state.revision,
        )


async def project_access(
    db, *, user_id, subscription_id, account_generation, evidence, grace_active, transition_receipt=None
):
    """Return group-specific masks; caller verifies actual grace under its lease.

    This is not authorization to issue a panel write. Revision, ownership,
    topology and grace must be checked again by the delivery coordinator.
    """
    if type(grace_active) is not bool:
        raise LedgerConflict('Grace state must be explicit')
    state, _ = await _lock(db, user_id, subscription_id, account_generation)
    policy = _read_policy(state.access_policy)
    now = await db.scalar(select(func.clock_timestamp()))
    balances = await summary(
        db, user_id=user_id, subscription_id=subscription_id, account_generation=account_generation, now=now
    )
    subscription = (
        await db.execute(
            select(
                Subscription.status, Subscription.end_date, Subscription.tariff_id, Subscription.traffic_accounting_mode
            ).where(Subscription.id == subscription_id)
        )
    ).one()
    owner_status = await db.scalar(select(User.status).where(User.id == user_id))
    groups = []
    for group in ('ordinary', 'bs'):
        mode, balance = effective_group_mode(policy, group, now), balances[group]
        remaining = balance['remaining_bytes'] if mode == 'finite' else None
        pending = balance['needs_review'] or (balance['pending_operations'] > 0 and balance['unencumbered_bytes'] == 0)
        status = (
            'disabled'
            if mode == 'disabled'
            else 'pending'
            if mode == 'finite' and pending
            else 'exhausted'
            if mode == 'finite' and remaining == 0
            else 'active'
        )
        squads = (
            None if status == 'pending' else tuple(policy['topology'][f'{group}_squads']) if status == 'active' else ()
        )
        groups.append(
            GroupProjection(mode, remaining, balance['pending_bytes'], balance['needs_review'], status, squads)
        )

    def result(*, status=None, squads=None, preserve_grace=False, grace_candidate=None):
        projected = [replace(group, state=status, squads=squads) if status else group for group in groups]
        return AccessProjection(
            state.revision,
            account_generation,
            policy['fingerprint'],
            projected[0],
            projected[1],
            preserve_grace,
            grace_candidate,
        )

    if owner_status in ('blocked', 'deleted') or subscription.status == 'disabled':
        return result(status='blocked', squads=())
    if owner_status != 'active':
        return result(status='subscription_pending')
    if grace_active:
        return result(status='grace', preserve_grace=True)
    if subscription.traffic_accounting_mode == 'split_pending':
        return result(status='transfer_pending')
    if subscription.end_date is not None and subscription.end_date <= now:
        return result(status='expired', squads=(), grace_candidate=GraceReason.EXPIRED)
    if subscription.status not in ('active', 'trial', 'limited') or subscription.end_date is None:
        return result(status='subscription_pending')
    if subscription.tariff_id != policy['tariff_id']:
        return result(status='tariff_pending')
    # A pending topology migration owns the whole access write, including when
    # old access is empty or shared with the new topology. Generic planners
    # must not partially grant new groups while its delivery is unresolved.
    if 'topology_transition_id' in policy:
        from app.database.models import BsTrafficRenewalIntent
        from app.services.bs_traffic.ownership_cycle import renewal_floor
        from app.services.bs_traffic.tariff_switch_topology import pending_topology_transition

        receipt = await pending_topology_transition(db, state)
        if receipt is not None:
            floor = await renewal_floor(db, state)
            latest_id = await db.scalar(
                select(BsTrafficRenewalIntent.id)
                .where(
                    BsTrafficRenewalIntent.subscription_id == subscription_id,
                    BsTrafficRenewalIntent.id > floor,
                    BsTrafficRenewalIntent.transaction_id.is_not(None),
                )
                .order_by(BsTrafficRenewalIntent.id.desc())
                .limit(1)
            )
            if transition_receipt is not receipt or latest_id != receipt.id:
                return result(status='topology_transition_pending')
    verified = isinstance(evidence, TopologyEvidence) and evidence.is_fresh(now, panel_identity=state.panel_scope)
    if verified:
        verified = evidence.fingerprint == policy['fingerprint'] and _topology(evidence.topology) == policy['topology']
    if not verified:
        return result(status='topology_pending')
    candidate = grace_candidate_reason(
        *(GroupAccess(group.mode, group.remaining_bytes, ready=group.state != 'pending') for group in groups),
        expired=False,
        blocked=False,
    )
    return result(grace_candidate=candidate)
