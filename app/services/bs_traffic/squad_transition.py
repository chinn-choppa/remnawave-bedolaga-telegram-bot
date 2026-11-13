"""Pure same-node squad transition plan; no payment or delivery authorization.

The receipt-bound tariff switch coordinator binds both evidence snapshots and
the pinned source selection to the same owned account/operation.
Account generation, revision, external squads and metadata remain caller guards.
"""

from datetime import datetime

from app.services.bs_traffic.delivery_plan import SquadPlan, _selection
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.policy import AccessProjection, GroupProjection, _fingerprint, _topology


def _allowed(projection, topology, *, allow_disabled_bs=False):
    if not isinstance(projection, AccessProjection) or not _fingerprint(projection.topology_fingerprint):
        raise ValueError('Validated access projection required')
    if type(projection.preserve_grace) is not bool:
        raise ValueError('Explicit grace state required')
    uncertain = projection.preserve_grace or projection.grace_candidate is not None
    allowed = set()
    for name, group, squads in (
        ('ordinary', projection.ordinary, topology.ordinary_squads),
        ('bs', projection.bs, topology.bs_squads),
    ):
        restrictive = (
            allow_disabled_bs and name == 'bs' and isinstance(group, GroupProjection) and group.mode == 'disabled'
        )

        if (
            not isinstance(group, GroupProjection)
            or (group.mode not in ('finite', 'unlimited') and not restrictive)
            or type(group.pending_bytes) is not int
            or group.pending_bytes < 0
            or type(group.needs_review) is not bool
            or ((group.mode == 'unlimited' or restrictive) and group.remaining_bytes is not None)
            or (
                group.remaining_bytes is not None
                and (type(group.remaining_bytes) is not int or group.remaining_bytes < 0)
            )
        ):
            raise ValueError('Invalid group projection')
        if (
            group.pending_bytes
            or group.needs_review
            or group.state not in (('disabled',) if restrictive else ('active', 'exhausted'))
        ):
            uncertain = True
            continue
        if restrictive:
            if group.squads != ():
                raise ValueError('Disabled BS target must remove all BS access')
            continue
        if group.state == 'active':
            if group.mode == 'finite' and (group.remaining_bytes is None or group.remaining_bytes <= 0):
                raise ValueError('Active finite group requires positive remaining quota')
            if group.squads != tuple(squads):
                raise ValueError('Active group must select its exact topology squads')
            allowed.update(squads)
        elif group.mode != 'finite' or group.remaining_bytes != 0 or group.squads != ():
            raise ValueError('Exhausted group must be finite, empty and confirmed')
    return None if uncertain else allowed


def plan_topology_transition(
    source_projection,
    target_projection,
    *,
    source_evidence,
    target_evidence,
    expected_source_squads,
    current_squads,
    now,
    panel_identity,
    allow_disabled_bs_target=False,
):
    """Move only a pinned old selection or acknowledge the exact paid target.

    Uncertain data waits as a whole. Malformed or unsupported transitions raise
    ValueError. Fresh evidence alone is not proof of account/write authority.
    """
    if type(allow_disabled_bs_target) is not bool:
        raise ValueError('Explicit restrictive BS transition permission required')
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('Aware planning time required')
    if not isinstance(panel_identity, str) or not panel_identity:
        raise ValueError('Explicit panel identity required')
    for evidence in (source_evidence, target_evidence):
        if not isinstance(evidence, TopologyEvidence) or not _fingerprint(evidence.fingerprint):
            raise ValueError('Validated topology evidence required')
        try:
            normalized = _topology(evidence.topology)
        except LedgerConflict as error:
            raise ValueError('Invalid transition topology') from error
        if any(getattr(evidence.topology, key) != tuple(values) for key, values in normalized.items()):
            raise ValueError('Canonical immutable topology identities required')
        if any(
            not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None
            for value in (evidence.observed_at, evidence.valid_until)
        ):
            raise ValueError('Aware topology evidence required')
    old, new = source_evidence.topology, target_evidence.topology
    if set(old.ordinary_squads) & set(new.bs_squads) or set(old.bs_squads) & set(new.ordinary_squads):
        raise ValueError('Squad transition cannot change node or group assignment')
    source_allowed = _allowed(source_projection, old)
    target_allowed = _allowed(target_projection, new, allow_disabled_bs=allow_disabled_bs_target)
    source, current = _selection(expected_source_squads), _selection(current_squads)
    wait = SquadPlan('wait', None)
    if (
        source_allowed is None
        or target_allowed is None
        or source_projection.topology_fingerprint != source_evidence.fingerprint
        or target_projection.topology_fingerprint != target_evidence.fingerprint
        or not source_evidence.is_fresh(now, panel_identity=panel_identity)
        or not target_evidence.is_fresh(now, panel_identity=panel_identity)
    ):
        return wait
    if not source <= source_allowed:
        return wait
    desired = tuple(sorted(target_allowed))
    if current == target_allowed:
        return SquadPlan('unchanged', desired)
    if current != source:
        return wait
    return SquadPlan('patch', desired, tuple(sorted(source - target_allowed)))
