"""Read-only source validation. Equal counters are NOT a flush/cutover boundary."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select

from app.external.remnawave_api import RemnaWaveAPIError, SplitTransferSnapshot
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.managed_transfer import ManagedTransferManifest, inspect_managed_transfer
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.panel_pause import persist_panel_pause
from app.services.bs_traffic.recovery_backoff import backoff_for


@dataclass(frozen=True)
class ManagedTransferObservation:
    manifest: ManagedTransferManifest
    account: SplitTransferSnapshot
    observed_at: datetime
    valid_until: datetime


async def capture_managed_transfer(reader, *, user_id, subscription_id, account_generation, destination_group):
    """Before/after locked manifests surround bounded HTTP outside business locks.

    Drift requires reconciliation by the current accountant, not guessed debits.
    This returns evidence only: no quiet-time rule, retirement, grant or PATCH.
    """
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound panel reader required')
    keys = dict(
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        destination_group=destination_group,
    )
    factory = reader._factory
    async with asyncio.timeout(30):
        await reader.assert_current()
        async with factory() as db, db.begin():
            before = await inspect_managed_transfer(db, **keys)
            started = await db.scalar(select(func.clock_timestamp()))
            if before.panel_scope != reader.panel_scope:
                raise LedgerConflict('Transfer belongs to another panel')
        try:
            async with panel_slot(factory, reader.panel_scope):
                observed = await reader.get_split_transfer_snapshot(before.panel_user_id)
        except RemnaWaveAPIError as exc:
            pause = backoff_for(exc)
            if pause is not None:
                await persist_panel_pause(factory, reader.panel_scope, pause.retry_after_seconds)
            raise
        if (
            not isinstance(observed, SplitTransferSnapshot)
            or generation_of(panel_scope=reader.panel_scope, account=observed) != account_generation
            or f'{observed.id}@{observed.created_at.astimezone(UTC).isoformat()}' != before.source_identity
        ):
            raise LedgerConflict('Managed and split identities do not match the observed account')
        if (
            observed.used_bytes != before.used_bytes
            or observed.lifetime_bytes != before.lifetime_bytes
            or observed.traffic_limit_strategy != 'NO_RESET'
        ):
            raise LedgerConflict('Current accountant must reconcile source counters first')
        expected_quota = 0 if before.source_phase == 'unlimited' else max(1, before.used_bytes + before.total_bytes)
        if observed.traffic_limit_bytes != expected_quota:
            raise LedgerConflict('Panel quota does not match the prepared managed projection')
        await reader.assert_current()
        async with factory() as db, db.begin():
            after = await inspect_managed_transfer(db, **keys)
            now = await db.scalar(select(func.clock_timestamp()))
            expiry = min(
                [
                    started + timedelta(seconds=15),
                    *(g.expires_at for g in after.grants),
                    *([after.next_refresh_at] if after.next_refresh_at is not None else []),
                ]
            )
            if before != after or expiry <= now:
                raise LedgerConflict('Transfer source changed during observation')
        return ManagedTransferObservation(after, observed, now, expiry)
