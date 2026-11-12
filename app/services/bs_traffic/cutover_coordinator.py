"""Advance one durable cutover using already collected, shared panel observations.

No per-subscription usage polling, automatic retries or registration in main.
The collection scheduler owns batching/fairness. This step consumes its explicit
fresh observations; missing data leaves the original panel limit untouched.
"""

import asyncio

from sqlalchemy import func, select

from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.cutover_activation import activate_cutover
from app.services.bs_traffic.cutover_baseline import enroll_cutover_baseline
from app.services.bs_traffic.cutover_recovery import _load
from app.services.bs_traffic.cutover_sender import send_cutover
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import PanelBusy
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.recovery_backoff import backoff_for


async def advance_cutover(reader, *, user_id, subscription_id, account_generation, operation_id):
    """Enroll pending work, send at most once, then confirm activation separately.

    Each stage commits its durable result. Cancellation/unknown outcome does not
    roll an attempted operation back to pending. A later call resumes by its
    exact operation ID; it never manufactures a replacement operation or resets
    an existing intake cursor. Historical activated results do not grant rights
    to any future purchase or access change.
    """
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound cutover reader required')
    local = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    keys = dict(local, operation_id=operation_id)
    try:
        async with asyncio.timeout(60):
            await reader.assert_current()
            async with reader._factory() as db, db.begin():
                status, row, _ = await _load(db, reader, **keys)
                if status not in ('pending', 'attempted', 'verified'):
                    return status
                if status == 'pending':
                    if row.valid_until <= await db.scalar(select(func.clock_timestamp())):
                        row.state = 'cancelled'
                        await db.flush()
                        return 'cancelled'
                    try:
                        await enroll_cutover_baseline(db, reader, **local)
                    except LedgerConflict:
                        return 'accounting_pending'
            # Never hold enrollment/business locks across panel I/O.
            result = await send_cutover(reader, **keys)
            if result != 'verified':
                return result
            return await activate_cutover(reader, **keys)
    except PanelBusy:
        return 'deferred'
    except LedgerConflict:
        return 'obsolete'
    except RemnaWaveAPIError as exc:
        pause = backoff_for(exc)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
    except TimeoutError:
        # Durable state, not this return value, determines recovery vs a send.
        return 'unconfirmed'
