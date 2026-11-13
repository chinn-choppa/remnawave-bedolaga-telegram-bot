"""Receipt-bound same-node transitions; frozen source grants no current rights."""

import hashlib
import json
from dataclasses import asdict

from app.services.bs_traffic.delivery_plan import _selection
from app.services.bs_traffic.evidence import TopologyEvidence
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.policy import AccessProjection, GroupProjection, _fingerprint, _topology
from app.services.bs_traffic.squad_transition import _allowed, plan_topology_transition
from app.services.bs_traffic.topology import SplitTopology


def manifest(evidence):
    return {'topology': _topology(evidence.topology), 'fingerprint': evidence.fingerprint}


def capture_transition(projection, *, source_evidence, target_evidence, current_squads, now, panel_identity):
    """Capture historical removal authority only after definitive source access."""
    for evidence in (source_evidence, target_evidence):
        if not isinstance(evidence, TopologyEvidence) or not _fingerprint(evidence.fingerprint):
            raise LedgerConflict('Validated transition evidence required')
        topology = _topology(evidence.topology)
        if any(getattr(evidence.topology, key) != tuple(values) for key, values in topology.items()):
            raise LedgerConflict('Canonical transition topology required')
    old, new = source_evidence.topology, target_evidence.topology
    from app.services.bs_traffic.node_membership import removals

    removed = removals(_topology(old), _topology(new))
    added = removals(_topology(new), _topology(old))
    if set(old.ordinary_squads) & set(new.bs_squads) or set(old.bs_squads) & set(new.ordinary_squads):
        raise LedgerConflict('Squad groups cannot be reassigned')
    # Validate the source projection against actual source evidence. This is
    # not a projection of new rights and never substitutes target policy data.
    try:
        plan = plan_topology_transition(
            projection,
            projection,
            source_evidence=source_evidence,
            target_evidence=source_evidence,
            expected_source_squads=current_squads,
            current_squads=current_squads,
            now=now,
            panel_identity=panel_identity,
        )
        _topology(target_evidence.topology)
    except ValueError as error:
        raise LedgerConflict('Invalid source squad permission') from error
    if plan.action == 'wait' or not target_evidence.is_fresh(now, panel_identity=panel_identity):
        raise LedgerConflict('Definitive fresh source and target required')
    return json.loads(
        json.dumps(
            dict(
                version=7,
                removed_nodes=removed,
                added_nodes=added,
                source_modes=dict(ordinary=projection.ordinary.mode, bs=projection.bs.mode),
                source=manifest(source_evidence),
                target=manifest(target_evidence),
                pinned_source=list(current_squads),
                source_projection=asdict(projection),
            )
        )
    )


def _receipt_payload(row):
    parameters = row.parameters
    if not isinstance(parameters, dict):
        raise LedgerConflict('Invalid renewal parameters')
    inherited = 'topology_transition' in parameters
    if inherited:
        terms = parameters['topology_transition']
        envelope_keys = {
            'version',
            'manifest',
            'predecessor_id',
            'account_generation',
            'ownership_cycle',
            'ordinary_mode',
            'bs_mode',
        }
        if isinstance(terms, dict) and terms.get('version') == 2:
            envelope_keys.add('provenance_receipt_id')
        if (
            not isinstance(terms, dict)
            or set(terms) != envelope_keys
            or type(terms['version']) is not int
            or terms['version'] not in (1, 2)
            or type(terms['predecessor_id']) is not int
            or not 0 < terms['predecessor_id'] < row.id
            or (
                terms['version'] == 2
                and (
                    type(terms.get('provenance_receipt_id')) is not int
                    or not 0 < terms['provenance_receipt_id'] <= terms.get('predecessor_id', 0)
                )
            )
            or 'switch_terms' in parameters
            or parameters.get('kind') == 'tariff_switch'
        ):
            raise LedgerConflict('Invalid inherited topology envelope')
        value, expected = terms['manifest'], parameters.get('topology_transition_hash')
    else:
        terms = parameters.get('switch_terms')
        if not isinstance(terms, dict) or 'topology_transition' not in terms:
            return None
        if parameters.get('kind') != 'tariff_switch':
            raise LedgerConflict('Invalid original topology funding kind')
        value, expected = terms['topology_transition'], parameters.get('switch_terms_hash')
    digest = hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if digest != expected:
        raise LedgerConflict('Topology receipt hash changed')
    return terms, value, inherited


async def provenance_receipt_id(db, *, state, receipt, transition):
    """Flat v2 root pointer; bounded checked recovery for legacy v1 envelopes."""
    from app.database.models import BsTrafficRenewalIntent

    current = receipt
    visited = set()
    for _ in range(32):
        if (
            current.id in visited
            or (current.subscription_id, current.panel_scope, current.account_generation)
            != (state.subscription_id, state.panel_scope, state.account_generation)
            or current.transaction_id is None
        ):
            raise LedgerConflict('Historical topology funding owner changed')
        visited.add(current.id)
        payload = _receipt_payload(current)
        if payload is None:
            raise LedgerConflict('Historical topology funding is missing')
        terms, value, inherited = payload
        if (
            not isinstance(value, dict)
            or terms.get('ownership_cycle') != state.ownership_cycle
            or any(
                value.get(key) != transition.get(key)
                for key in (
                    'source',
                    'target',
                    'source_projection',
                    'pinned_source',
                    'removed_nodes',
                    'added_nodes',
                    'source_modes',
                )
            )
        ):
            raise LedgerConflict('Historical topology funding changed')
        if not inherited:
            return current.id
        identifier = terms.get('provenance_receipt_id', terms['predecessor_id'])
        if identifier >= current.id:
            raise LedgerConflict('Historical topology ancestry changed')
        current = await db.get(BsTrafficRenewalIntent, identifier)
        if current is None:
            raise LedgerConflict('Historical topology funding is missing')
    raise LedgerConflict('Historical topology ancestry budget exceeded')


def receipt_transition(row, policy, *, ownership_cycle):
    """Validate immutable schema and hash before using any persisted authority."""
    payload = _receipt_payload(row)
    if payload is None:
        return None
    terms, value, inherited = payload
    keys = {'version', 'source', 'target', 'pinned_source', 'source_projection'}
    epochs = isinstance(value, dict) and value.get('version') in (5, 6, 7, 8)
    removal = isinstance(value, dict) and value.get('version') in (3, 4, 5, 6, 7, 8)
    explicit_modes = isinstance(value, dict) and value.get('version') in (7, 8)
    if explicit_modes:
        keys.add('source_modes')
    if epochs:
        keys.add('added_nodes')
    if removal:
        keys.add('removed_nodes')
    if inherited:
        keys.add('allow_empty_source')
    if (
        terms.get('account_generation') != row.account_generation
        or type(terms.get('ownership_cycle')) is not int
        or terms['ownership_cycle'] != ownership_cycle
        or not isinstance(value, dict)
        or set(value) != keys
        or type(value['version']) is not int
        or value['version']
        != (
            (8 if inherited else 7)
            if explicit_modes
            else (6 if inherited else 5)
            if epochs
            else (4 if inherited else 3)
            if removal
            else (2 if inherited else 1)
        )
        or (inherited and value['allow_empty_source'] is not True)
    ):
        raise LedgerConflict('Invalid topology transition receipt')
    for side in ('source', 'target'):
        if not isinstance(value[side], dict) or set(value[side]) != {'topology', 'fingerprint'}:
            raise LedgerConflict('Invalid topology transition manifest')
        topology = value[side]['topology']
        if (
            not isinstance(topology, dict)
            or set(topology) != {'ordinary_squads', 'bs_squads', 'ordinary_nodes', 'bs_nodes'}
            or any(type(items) is not list for items in topology.values())
            or not _fingerprint(value[side]['fingerprint'])
        ):
            raise LedgerConflict('Invalid topology transition identities')
        canonical = _topology(SplitTopology(**{key: tuple(items) for key, items in topology.items()}))
        if canonical != topology:
            raise LedgerConflict('Noncanonical topology transition identities')
    if value['target'] != {'topology': policy['topology'], 'fingerprint': policy['fingerprint']}:
        raise LedgerConflict('Transition target policy changed')
    projection = value['source_projection']
    if (
        not isinstance(projection, dict)
        or set(projection)
        != {
            'revision',
            'account_generation',
            'topology_fingerprint',
            'ordinary',
            'bs',
            'preserve_grace',
            'grace_candidate',
        }
        or type(projection['revision']) is not int
        or projection['revision'] < 0
        or projection['account_generation'] != row.account_generation
        or projection['topology_fingerprint'] != value['source']['fingerprint']
        or projection['preserve_grace'] is not False
        or projection['grace_candidate'] is not None
        or type(value['pinned_source']) is not list
    ):
        raise LedgerConflict('Invalid historical source projection')
    groups = {}
    for name in ('ordinary', 'bs'):
        group = projection[name]
        if (
            not isinstance(group, dict)
            or set(group) != {'mode', 'remaining_bytes', 'pending_bytes', 'needs_review', 'state', 'squads'}
            or type(group['squads']) is not list
            or group['state'] not in ('active', 'exhausted')
            or type(group['pending_bytes']) is not int
            or group['pending_bytes'] != 0
            or group['needs_review'] is not False
        ):
            raise LedgerConflict('Invalid historical group permission')
        groups[name] = GroupProjection(**dict(group, squads=tuple(group['squads'])))
    try:
        _selection(tuple(value['pinned_source']))
    except ValueError as error:
        raise LedgerConflict('Invalid pinned source selection') from error
    historical = AccessProjection(**dict(projection, **groups))
    old, new = (
        SplitTopology(**{key: tuple(items) for key, items in value[side]['topology'].items()})
        for side in ('source', 'target')
    )
    from app.services.bs_traffic.node_membership import removals

    removed = removals(_topology(old), _topology(new))
    if (
        (removal and ((not epochs and not any(removed.values())) or value['removed_nodes'] != removed))
        or (epochs and value['added_nodes'] != removals(_topology(new), _topology(old)))
        or (
            explicit_modes and value['source_modes'] != {'ordinary': historical.ordinary.mode, 'bs': historical.bs.mode}
        )
        or (not removal and any(removed.values()))
        or set(old.ordinary_squads) & set(new.bs_squads)
        or set(old.bs_squads) & set(new.ordinary_squads)
        or (
            not epochs
            and (historical.ordinary.mode, historical.bs.mode) != (terms.get('ordinary_mode'), terms.get('bs_mode'))
        )
        or (
            inherited
            and (policy.get('ordinary_mode'), policy.get('bs_mode'))
            != (terms.get('ordinary_mode'), terms.get('bs_mode'))
        )
    ):
        raise LedgerConflict('Historical topology or modes changed')
    try:
        allowed = _allowed(historical, old)
    except ValueError as error:
        raise LedgerConflict('Invalid historical source permission') from error
    if allowed is None or not set(value['pinned_source']) <= allowed:
        raise LedgerConflict('Pinned source exceeds historical permission')
    return value, historical


def transition_plan(
    row,
    policy,
    target_projection,
    *,
    source_evidence,
    target_evidence,
    current_squads,
    now,
    panel_identity,
    ownership_cycle,
):
    value, historical = receipt_transition(row, policy, ownership_cycle=ownership_cycle)
    if manifest(source_evidence) != value['source'] or manifest(target_evidence) != value['target']:
        raise LedgerConflict('Transition topology changed')
    inherited = value.get('allow_empty_source') is True
    return plan_topology_transition(
        historical,
        target_projection,
        source_evidence=source_evidence,
        target_evidence=target_evidence,
        expected_source_squads=()
        if value.get('allow_empty_source') and current_squads == ()
        else tuple(value['pinned_source']),
        current_squads=current_squads,
        now=now,
        panel_identity=panel_identity,
        allow_disabled_bs_target=inherited,
    )


async def pending_topology_transition(db, state):
    """Exact receipt pointer; malformed/missing ownership fails closed.

    A verified historical pointer needs one primary-key lookup, not a scan of
    renewal history. The caller holds the normal owner/subscription/ledger lock.
    """
    from app.database.models import BsTrafficRenewalIntent
    from app.services.bs_traffic.policy import _read_policy

    policy = _read_policy(state.access_policy)
    if 'topology_transition_id' not in policy:
        return None
    identifier = policy['topology_transition_id']
    if type(identifier) is not int or identifier <= 0:
        raise LedgerConflict('Invalid topology transition pointer')
    row = await db.get(BsTrafficRenewalIntent, identifier, populate_existing=True)
    if (
        row is None
        or row.subscription_id != state.subscription_id
        or row.account_generation != state.account_generation
        or row.panel_scope != state.panel_scope
        or row.transaction_id is None
    ):
        raise LedgerConflict('Missing owned topology transition receipt')
    from app.services.bs_traffic.node_membership import validate_removal_memberships

    payload = _receipt_payload(row)
    if payload is not None:
        await validate_removal_memberships(db, state=state, receipt=row, transition=payload[1])
    if row.state == 'verified':
        payload = _receipt_payload(row)
        value = payload[1] if payload is not None else None
        target = value.get('target') if isinstance(value, dict) else None
        if not isinstance(target, dict) or set(target) != {'topology', 'fingerprint'}:
            raise LedgerConflict('Invalid historical topology receipt')
        receipt_transition(row, dict(policy, **target), ownership_cycle=state.ownership_cycle)
        return None
    if row.state not in ('pending', 'attempted'):
        raise LedgerConflict('Unresolved topology transition was superseded')
    if receipt_transition(row, policy, ownership_cycle=state.ownership_cycle) is None:
        raise LedgerConflict('Missing topology transition provenance')
    return row


async def inherit_expired_transition(db, *, state, sub, receipt, now):
    """Carry only old removal authority into a genuinely funded later renewal.

    The financial caller atomically attaches this flat envelope to its new
    ordinary renewal, supersedes the known-unsent predecessor and moves the
    policy pointer. No payment, grant, network or commit occurs here.
    """
    from app.services.bs_traffic.grace_capture import _open_grace
    from app.services.bs_traffic.grace_topology import unsent_grace_transition
    from app.services.bs_traffic.policy import _read_policy

    if await _open_grace(db, sub.id):
        return None
    pending = await unsent_grace_transition(db, state)
    if (
        pending is None
        or pending[0] is not receipt
        or receipt.intended_expires_at > now
        or sub.end_date != receipt.intended_expires_at
        or sub.end_date > now
    ):
        raise LedgerConflict('Expired known-unsent topology receipt required')
    policy = _read_policy(state.access_policy)
    value, historical = receipt_transition(receipt, policy, ownership_cycle=state.ownership_cycle)
    value = json.loads(json.dumps(value))
    value.update(
        version=8
        if value['version'] in (7, 8)
        else 6
        if value['version'] in (5, 6)
        else 4
        if value['version'] in (3, 4)
        else 2,
        allow_empty_source=True,
    )
    root_id = await provenance_receipt_id(db, state=state, receipt=receipt, transition=value)
    envelope = dict(
        version=2,
        provenance_receipt_id=root_id,
        manifest=value,
        predecessor_id=receipt.id,
        account_generation=state.account_generation,
        ownership_cycle=state.ownership_cycle,
        ordinary_mode=policy['ordinary_mode'],
        bs_mode=policy['bs_mode'],
    )
    digest = hashlib.sha256(json.dumps(envelope, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {'topology_transition': envelope, 'topology_transition_hash': digest}
