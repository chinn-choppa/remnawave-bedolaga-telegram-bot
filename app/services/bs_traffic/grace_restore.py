"""Pure selection for a split-aware grace exit, not permission to write.

A coordinator must validate ownership, panel generation, the complete expected
overlay (not only squads), current paid policy and grace session/version under
its locks. It must restore status/expiry/device/external-squad fields separately
and verify the full result before completing grace. No runtime calls this yet.
Unknown usage preserves only rights proven to precede this grace session in the
same topology. It never treats a grace squad as a paid entitlement.
"""

from datetime import datetime

from app.services.bs_traffic.access import GroupAccess
from app.services.bs_traffic.delivery_plan import SquadPlan, _selection
from app.services.bs_traffic.policy import AccessProjection, _fingerprint, _topology
from app.services.bs_traffic.topology import SplitTopology


_READY_STATES = frozenset(('active', 'pending', 'disabled', 'exhausted'))


def plan_grace_restore(
    projection: AccessProjection,
    *,
    topology: SplitTopology,
    current_squads: tuple[str, ...],
    overlay_squads: tuple[str, ...],
    paid_before_squads: tuple[str, ...],
    paid_before_fingerprint: str | None,
) -> SquadPlan:
    if not isinstance(projection, AccessProjection):
        raise ValueError('Explicit paid access projection required')
    _topology(topology)
    current = _selection(current_squads)
    overlay = _selection(overlay_squads)
    before = _selection(paid_before_squads)
    if not overlay:
        raise ValueError('An explicit grace overlay selection is required')
    if projection.preserve_grace or any(
        item.state not in _READY_STATES for item in (projection.ordinary, projection.bs)
    ):
        return SquadPlan('wait', None)
    desired = set()
    managed = set(topology.ordinary_squads) | set(topology.bs_squads)
    for group, configured in (
        (projection.ordinary, topology.ordinary_squads),
        (projection.bs, topology.bs_squads),
    ):
        allowed = set(configured)
        GroupAccess(group.mode, group.remaining_bytes)  # Validate mode/balance even for a restricted group.
        if group.state == 'disabled' and group.mode != 'disabled':
            raise ValueError('Disabled group must have disabled mode')
        if group.state == 'exhausted' and (group.mode != 'finite' or group.remaining_bytes != 0):
            raise ValueError('Exhausted group must have a confirmed zero balance')
        if group.state == 'pending':
            if (
                group.mode != 'finite'
                or group.squads is not None
                or not _fingerprint(paid_before_fingerprint)
                or paid_before_fingerprint != projection.topology_fingerprint
                or not before <= managed
            ):
                return SquadPlan('wait', None)
            desired.update(before & allowed)
        else:
            selected = _selection(group.squads)
            if not selected <= allowed:
                raise ValueError('Paid selection does not belong to this topology group')
            if group.state == 'active':
                entitled = group.mode == 'unlimited' or (
                    group.mode == 'finite' and type(group.remaining_bytes) is int and group.remaining_bytes > 0
                )
                if not entitled or selected != allowed or (group.mode == 'finite' and group.needs_review):
                    raise ValueError('Inconsistent active paid group')
            elif selected:
                raise ValueError('Restricted paid group cannot grant access')
            desired.update(selected)
    result = tuple(sorted(desired))
    if current == desired:
        return SquadPlan('unchanged', result)
    if current != overlay:
        return SquadPlan('wait', None)
    return SquadPlan('patch', result, tuple(sorted(current - desired)))


def _expiry_confirmed(observed, expected):
    if not isinstance(observed, datetime) or not isinstance(expected, datetime):
        return False
    if observed.utcoffset() is None or expected.utcoffset() is None:
        return False
    # The panel's JavaScript Date serializes milliseconds. Accept exactly that
    # representation, not an arbitrary time tolerance or a nearby expiry.
    return observed == expected or observed == expected.replace(microsecond=expected.microsecond // 1000 * 1000)


def full_restore_confirmed(current, target) -> bool:
    """Confirm a no-reset restore against full strict API evidence.

    ``target`` must be built by the coordinator from locked current paid rights
    and the last pre-write counter/reset evidence, never an old paid counter.
    This does not authorize a PATCH or complete a grace session: binding, fresh
    reads, revision and session ownership remain the coordinator's responsibility.
    Expiry accepts only the exact value or its panel millisecond serialization;
    even a one-millisecond unexplained change remains a conflict.
    """
    from app.external.remnawave_api import SplitGraceSnapshot

    if not isinstance(current, SplitGraceSnapshot) or not isinstance(target, SplitGraceSnapshot):
        return False
    fields = (
        'id',
        'created_at',
        'status',
        'traffic_limit_bytes',
        'traffic_limit_strategy',
        'hwid_device_limit',
        'external_squad_uuid',
        'last_traffic_reset_at',
    )
    if any(getattr(current, name) != getattr(target, name) for name in fields):
        return False
    if not _expiry_confirmed(current.expire_at, target.expire_at):
        return False
    try:
        from app.services.bs_traffic.readback import _observed_squads

        return (
            _observed_squads(current) == _observed_squads(target)
            and type(current.used_traffic_bytes) is int
            and type(target.used_traffic_bytes) is int
            and 0 <= target.used_traffic_bytes <= current.used_traffic_bytes < 2**53
        )
    except (ValueError, TypeError, AttributeError, KeyError):
        return False


def build_paid_restore_target(
    *,
    billing,
    projection,
    topology,
    current,
    overlay_squads,
    paid_before_squads,
    paid_before_fingerprint,
    now,
):
    """Build a no-reset target for a currently valid paid split subscription.

    Pure construction, not write permission. The coordinator must supply locked
    current billing/policy, validate panel epoch/generation and the FULL expected
    overlay, then persist this target before a single write attempt. External
    squad existence and compatibility must also be checked before writing.
    Expired/admin transitions need their own target; never revive them here.
    """
    from dataclasses import replace
    from datetime import datetime
    from uuid import UUID

    from app.external.remnawave_api import SplitGraceSnapshot
    from app.services.grace_access_service import GraceBillingState

    if not isinstance(billing, GraceBillingState) or not isinstance(current, SplitGraceSnapshot):
        raise ValueError('Typed current billing and full panel evidence required')
    if (
        not isinstance(now, datetime)
        or now.tzinfo is None
        or now.utcoffset() is None
        or not isinstance(billing.end_at, datetime)
        or billing.end_at.tzinfo is None
        or billing.end_at.utcoffset() is None
        or billing.end_at <= now
        or billing.status not in ('active', 'trial', 'limited')
        or billing.user_status != 'active'
        or type(billing.remnawave_id) is not int
        or billing.remnawave_id != current.id
    ):
        raise ValueError('Current paid subscription is unavailable or belongs to another account')
    devices = billing.device_limit
    if devices is not None and (type(devices) is not int or not 0 <= devices < 2**53):
        raise ValueError('Invalid paid device limit')
    external = billing.external_squad_uuid
    if external is not None:
        try:
            if not isinstance(external, str) or str(UUID(external)) != external:
                raise ValueError
        except ValueError:
            raise ValueError('Invalid paid external squad') from None
    from app.services.bs_traffic.readback import _observed_squads

    selection = plan_grace_restore(
        projection,
        topology=topology,
        current_squads=_observed_squads(current),
        overlay_squads=overlay_squads,
        paid_before_squads=paid_before_squads,
        paid_before_fingerprint=paid_before_fingerprint,
    )
    if selection.action == 'wait':
        return None
    # Group quotas live in the ledger, not in the account-wide panel counter.
    # Keep the latest counter/reset evidence solely for readback verification.
    return replace(
        current,
        status='ACTIVE',
        expire_at=billing.end_at,
        traffic_limit_bytes=0,
        traffic_limit_strategy='NO_RESET',
        hwid_device_limit=devices,
        external_squad_uuid=external,
        active_internal_squads=[{'uuid': value} for value in selection.squads],
    )


def build_exit_target(*, completion_reason='paid', safe_expire_at=None, **kwargs):
    """Current paid projection, or an explicitly access-free revoked target."""
    from dataclasses import replace
    from datetime import timedelta

    billing, current, now = kwargs['billing'], kwargs['current'], kwargs['now']
    if completion_reason not in ('paid', 'timeout', 'revoked', 'drained', 'conflict'):
        raise ValueError('Unknown grace completion reason')
    alive = (
        billing is not None
        and billing.end_at is not None
        and billing.end_at > now
        and billing.status in ('active', 'trial', 'limited')
        and billing.user_status == 'active'
    )
    if alive:
        return build_paid_restore_target(**kwargs)
    if completion_reason == 'paid':
        raise ValueError('Paid restoration requires current rights')
    expiry = safe_expire_at or max(current.expire_at, now + timedelta(minutes=5))
    if expiry <= now and safe_expire_at is None:
        raise ValueError('Expired safe restoration target')
    return replace(
        current,
        status='DISABLED',
        expire_at=expiry,
        traffic_limit_bytes=0,
        traffic_limit_strategy='NO_RESET',
        active_internal_squads=[],
        external_squad_uuid=None,
    )


def entitled_before_squads(session, row):
    """Actual expired/exhausted issuance has no funded pre-grace fallback.

    Keep the full actual selection in the v4 source snapshot for CAS evidence;
    it may still contain exhausted squads awaiting the regular reconciler.
    """
    attempt = row.split_overlay_attempt
    if attempt is not None:
        if attempt.get('entitled_before_squads') != []:
            raise ValueError('Verified pre-grace entitlement evidence missing')
        return ()
    # Historical explicitly captured v4 contexts retain their original contract.
    return session.split_before.paid_squads
