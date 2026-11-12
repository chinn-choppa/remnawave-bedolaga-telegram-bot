"""Store raw daily evidence without assigning bytes to any paid allowance.

Poll order is allocated in SQL before HTTP. It is request order, NOT a panel
watermark or consumption time. Missing users never become zero observations;
counter regressions require review and cannot refund quota. Callers commit
poll allocation before HTTP and commit the response separately. There is no
network I/O, billing, background scheduling or implicit commit in this module.
"""

import hashlib
import json
from datetime import date
from itertools import batched
from uuid import UUID

from sqlalchemy import case, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import BsTrafficObservation, BsTrafficPoll


MAX_NODES = 100
MAX_IDENTITIES = 50000
MAX_RESPONSE_ROWS = 100000
MAX_EXACT_INTEGER = 2**53 - 1


def _uuid(value):
    if not isinstance(value, str):
        raise ValueError('Expected UUID string')
    return str(UUID(value))


def _integer(value, *, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_EXACT_INTEGER:
        raise ValueError('Expected exact nonnegative API integer')
    return value


async def begin_poll(
    db: AsyncSession, *, panel_scope: str, day: date, node_uuids: tuple[str, ...], identities: dict[int, str]
) -> BsTrafficPoll:
    """Allocate a single UTC-day request; no implicit zero or account discovery.

    Scope is a stable operator/panel epoch, not a URL or credential. Identity
    mapping must be verified by the caller before allocation; never attach a
    delayed response to whichever account owns the numeric ID at response time.
    Budgets are local safeguards, not advertised limits of the panel API.
    """
    if db.get_bind().dialect.name != 'postgresql':
        raise ValueError('Traffic observations require PostgreSQL')
    request = prepare_poll(panel_scope=panel_scope, day=day, node_uuids=node_uuids, identities=identities)
    poll = BsTrafficPoll(**request)
    db.add(poll)
    await db.flush()
    return poll


def prepare_poll(*, panel_scope: str, day: date, node_uuids: tuple[str, ...], identities: dict[int, str]) -> dict:
    """Validate and copy the request before persisting a job or poll."""
    scope = _uuid(panel_scope)
    if type(day) is not date:
        raise ValueError('Poll requires one UTC calendar date')
    if type(node_uuids) is not tuple or not 1 <= len(node_uuids) <= MAX_NODES:
        raise ValueError('Invalid node budget')
    nodes = sorted({_uuid(value) for value in node_uuids})
    if not isinstance(identities, dict) or not 1 <= len(identities) <= MAX_IDENTITIES:
        raise ValueError('Invalid identity budget')
    mapped = {}
    for user_id, generation in identities.items():
        _integer(user_id, minimum=1)
        if not isinstance(generation, str) or not 1 <= len(generation) <= 128 or generation.strip() != generation:
            raise ValueError('Invalid account generation')
        mapped[str(user_id)] = generation
    return {
        'panel_scope': scope,
        'day': day,
        'node_uuids': nodes,
        'identities': mapped,
        'selected_accounts': len(mapped),
    }


def _rows(payload, requested):
    if not isinstance(payload, dict) or not isinstance(payload.get('nodes'), list):
        raise ValueError('Invalid usage response')
    if len(payload['nodes']) > MAX_NODES:
        raise ValueError('Usage response exceeds node budget')
    nodes = set()
    rows = []
    for node in payload['nodes']:
        if not isinstance(node, dict):
            raise ValueError('Invalid usage node')
        node_id = _uuid(node.get('uuid'))
        if node_id not in requested or node_id in nodes or not isinstance(node.get('users'), list):
            raise ValueError('Unexpected or duplicate usage node')
        nodes.add(node_id)
        if len(rows) + len(node['users']) > MAX_RESPONSE_ROWS:
            raise ValueError('Usage response exceeds row budget')
        users = set()
        for user in node['users']:
            if not isinstance(user, dict):
                raise ValueError('Invalid usage user')
            user_id = _integer(user.get('id'), minimum=1)
            value = _integer(user.get('totalBytes'))
            if user_id in users:
                raise ValueError('Duplicate usage user')
            users.add(user_id)
            rows.append((node_id, user_id, value))
    # Missing nodes/users are absence of evidence. No synthetic zero rows.
    return sorted(rows), sorted(nodes)


async def record_poll(db: AsyncSession, poll_id: int, payload: dict) -> None:
    """Idempotent response persistence; caller owns transaction/rollback.

    SQL upsert serializes overlapping polls without holding locks over HTTP.
    All rows are validated before writing any observation. Identical replay is
    harmless; a second different response for the same poll is rejected. A new
    HTTP request needs a new poll, including after timeout/restart.
    """
    _integer(poll_id, minimum=1)
    poll = (
        await db.scalars(
            select(BsTrafficPoll)
            .where(BsTrafficPoll.id == poll_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one()
    rows, nodes = _rows(payload, set(poll.node_uuids))
    digest = hashlib.sha256(json.dumps([nodes, rows], separators=(',', ':')).encode()).hexdigest()
    if poll.response_hash is not None:
        if poll.response_hash != digest:
            raise ValueError('Poll already recorded with another response')
        return
    dialect = db.get_bind().dialect.name
    if dialect != 'postgresql':
        raise ValueError('Traffic observations require PostgreSQL')
    table = BsTrafficObservation.__table__
    values = (
        {
            'panel_scope': poll.panel_scope,
            'panel_user_id': user_id,
            'account_generation': poll.identities[str(user_id)],
            'node_uuid': node_id,
            'day': poll.day,
            'high_water_bytes': value,
            'latest_bytes': value,
            'latest_poll_id': poll.id,
            'needs_review': False,
        }
        for node_id, user_id, value in rows
        if str(user_id) in poll.identities
    )
    # Bound bind parameters and round trips; every worker uses the same key order.
    for batch in batched(values, 250, strict=False):
        statement = insert(table).values(list(batch))
        incoming = statement.excluded
        newer = incoming.latest_poll_id > table.c.latest_poll_id
        regression = or_(
            newer & (incoming.latest_bytes < table.c.high_water_bytes),
            (incoming.latest_poll_id < table.c.latest_poll_id) & (incoming.latest_bytes > table.c.latest_bytes),
        )
        await db.execute(
            statement.on_conflict_do_update(
                index_elements=list(table.primary_key.columns),
                set_={
                    'high_water_bytes': case(
                        (incoming.high_water_bytes > table.c.high_water_bytes, incoming.high_water_bytes),
                        else_=table.c.high_water_bytes,
                    ),
                    'latest_bytes': case((newer, incoming.latest_bytes), else_=table.c.latest_bytes),
                    'latest_poll_id': case((newer, incoming.latest_poll_id), else_=table.c.latest_poll_id),
                    'needs_review': or_(table.c.needs_review, regression),
                },
            )
        )
    poll.observed_pairs = sum(str(user_id) in poll.identities for _, user_id, _ in rows)
    poll.response_hash = digest
    poll.response_nodes = nodes
    # The account generation is now on each persisted observation. Keep the
    # request hash/counts for replay/diagnostics, not another full account map
    # for every successful poll. Unfinished polls retain their original map.
    poll.identities = {}
    await db.flush()
