"""Explicit persistent scope binding for read-only split API clients.

Registration/retirement are trusted operator operations, not automatic startup
steps. A restored panel at the same endpoint requires a new epoch explicitly;
identical restored data cannot be detected by hashing its address. Each read is
checked before/after HTTP; retirement discards results, not remote in-flight work.
Callers still own bounded admission/timeouts and panel account authorization.
"""

import hashlib
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from app.database.models import BsTrafficPanelBinding as Binding
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.recovery_scan import _postgres, _scope


def endpoint_fingerprint(endpoint: str) -> str:
    try:
        if not isinstance(endpoint, str) or any(c.isspace() or c == '\\' for c in endpoint):
            raise ValueError
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in ('https', 'http')
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError
        host = parsed.hostname.encode('idna').decode('ascii').lower()
        if ':' in host:
            host = f'[{host}]'
        port = parsed.port
        if port is not None and port != (443 if parsed.scheme == 'https' else 80):
            host += f':{port}'
        normalized = urlunsplit((parsed.scheme, host, parsed.path.rstrip('/'), '', ''))
    except (ValueError, UnicodeError, AttributeError):
        raise LedgerConflict('Invalid panel endpoint') from None
    return hashlib.sha256(('bs-panel-endpoint-v1:' + normalized).encode()).hexdigest()


async def register_binding(db, *, panel_scope: str, endpoint: str) -> None:
    _postgres(db)
    scope, digest = _scope(panel_scope), endpoint_fingerprint(endpoint)
    await db.execute(insert(Binding).values(panel_scope=scope, endpoint_hash=digest).on_conflict_do_nothing())
    row = await db.scalar(
        select(Binding).where(Binding.panel_scope == scope).with_for_update().execution_options(populate_existing=True)
    )
    if row is None or row.retired_at is not None or row.endpoint_hash != digest:
        raise LedgerConflict('Panel epoch or endpoint already registered differently')


async def retire_binding(db, *, panel_scope: str) -> None:
    _postgres(db)
    result = await db.execute(
        update(Binding)
        .where(Binding.panel_scope == _scope(panel_scope))
        .values(retired_at=func.coalesce(Binding.retired_at, func.clock_timestamp()))
    )
    if result.rowcount != 1:
        raise LedgerConflict('Panel epoch is not registered')


@dataclass(frozen=True)
class BoundPanelReader:
    panel_scope: str
    endpoint_hash: str
    _factory: object = field(repr=False, compare=False)
    _api: object = field(repr=False, compare=False)

    async def assert_current(self) -> None:
        if endpoint_fingerprint(self._api.base_url) != self.endpoint_hash:
            raise LedgerConflict('Bound panel client endpoint changed')
        async with self._factory() as db:
            _postgres(db)
            row = await db.scalar(select(Binding).where(Binding.panel_scope == self.panel_scope))
            if row is None or row.retired_at is not None or row.endpoint_hash != self.endpoint_hash:
                raise LedgerConflict('Panel epoch is unavailable')

    async def _read(self, method, *args, **kwargs):
        if method not in (
            'get_system_configuration',
            'get_internal_squad_topology',
            'get_split_access_snapshot',
            'get_split_grace_snapshot',
            'get_split_transfer_snapshot',
            'split_external_squad_exists',
            'get_daily_nodes_usage',
        ):
            raise LedgerConflict('Unsupported bound panel read')
        await self.assert_current()
        result = await getattr(self._api, method)(*args, **kwargs)
        await self.assert_current()
        return result

    async def get_system_configuration(self):
        return await self._read('get_system_configuration')

    async def get_internal_squad_topology(self, squad_uuid):
        return await self._read('get_internal_squad_topology', squad_uuid)

    async def get_split_access_snapshot(self, user_id):
        return await self._read('get_split_access_snapshot', user_id)

    async def get_split_grace_snapshot(self, user_id):
        return await self._read('get_split_grace_snapshot', user_id)

    async def get_split_transfer_snapshot(self, user_id):
        return await self._read('get_split_transfer_snapshot', user_id)

    async def split_external_squad_exists(self, uuid):
        return await self._read('split_external_squad_exists', uuid)

    async def get_daily_nodes_usage(self, node_uuids, day, **kwargs):
        return await self._read('get_daily_nodes_usage', node_uuids, day, **kwargs)


async def bind_reader(session_factory, api, *, panel_scope: str) -> BoundPanelReader:
    reader = BoundPanelReader(_scope(panel_scope), endpoint_fingerprint(api.base_url), session_factory, api)
    await reader.assert_current()
    return reader
