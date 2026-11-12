"""Fail-fast, cross-process split admission for a single database/panel epoch.

Four transaction advisory locks bound admitted operations, not provider-side
work after a network/DB disconnect. Each holder uses a separate DB connection
for at most 15 cooperative seconds. No business row locks or expiring leases:
normal completion, cancellation and rollback release capacity automatically.
Runtime must size its pool for these connections plus business transactions.
This is not an RPS limiter or a replacement for trusted client/epoch binding.
"""

import asyncio
import hashlib
from contextlib import asynccontextmanager

from sqlalchemy import func, select

from app.services.bs_traffic.panel_pause import remaining_pause
from app.services.bs_traffic.recovery_scan import _postgres, _scope


PANEL_SLOTS = 4
REQUEST_SECONDS = 15


class PanelBusy(TimeoutError):
    """No request was admitted; callers may defer without assuming delivery."""


def _slot_key(scope, slot):
    value = f'bedolaga:bs-panel-admission:v1:{scope}:{slot}'.encode()
    return int.from_bytes(hashlib.sha256(value).digest()[:8], 'big', signed=True)


@asynccontextmanager
async def panel_slot(session_factory, panel_scope):
    scope = _scope(panel_scope)
    async with asyncio.timeout(REQUEST_SECONDS):
        async with session_factory() as db, db.begin():
            _postgres(db)
            for slot in range(PANEL_SLOTS):
                acquired = await db.scalar(select(func.pg_try_advisory_xact_lock(_slot_key(scope, slot))))
                if acquired:
                    break
            else:
                raise PanelBusy('Panel capacity is occupied')
            if await remaining_pause(db, panel_scope=scope) > 0:
                raise PanelBusy('Panel requests are paused')
            yield
