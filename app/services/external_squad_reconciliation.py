"""Repair deleted tariff external-squad references without touching paid rights."""

import asyncio
from datetime import UTC, datetime

import structlog
from sqlalchemy import select, update

from app.database.models import Tariff
from app.external.remnawave_api import EXTERNAL_SQUAD_LOOKUP_TIMEOUT


logger = structlog.get_logger(__name__)


EXTERNAL_SQUAD_RECONCILIATION_TIMEOUT = 15.0


async def _confirmed_missing_squads(api, referenced: set[str]) -> set[str]:
    missing = set()
    try:
        # Do not let an unavailable detail endpoint monopolize the main monitor.
        async with asyncio.timeout(EXTERNAL_SQUAD_RECONCILIATION_TIMEOUT):
            catalog = await api.get_external_squads()
            if not isinstance(catalog, list) or any(
                not isinstance(getattr(squad, 'uuid', None), str) or not squad.uuid for squad in catalog
            ):
                return set()
            live = {squad.uuid for squad in catalog}
            for uuid in referenced - live:
                try:
                    async with asyncio.timeout(EXTERNAL_SQUAD_LOOKUP_TIMEOUT):
                        # Only the explicit panel 404/A182 is returned as None.
                        if await api.get_external_squad_by_uuid(uuid) is None:
                            missing.add(uuid)
                except Exception:
                    logger.debug('External squad absence not confirmed; tariff mapping preserved')
    except TimeoutError:
        logger.warning('External squad reconciliation timed out; unverified mappings preserved')
    except Exception:
        logger.warning('External squad catalog unavailable; tariff mappings preserved')
    return missing


async def reconcile_deleted_external_squads(api, session_factory, *, bot=None) -> list[tuple[int, str]]:
    # Do not hold a transaction across network calls or reuse a checkout session.
    async with session_factory() as db:
        snapshot = (
            await db.execute(
                select(Tariff.id, Tariff.external_squad_uuid).where(
                    Tariff.external_squad_uuid.isnot(None), Tariff.external_squad_uuid != ''
                )
            )
        ).all()
    if not snapshot:
        return []
    missing = await _confirmed_missing_squads(api, {uuid for _, uuid in snapshot})
    if not missing:
        return []

    changed = []
    async with session_factory() as db:
        # All workers acquire tariff row locks in the same order.
        for tariff_id, uuid in sorted(snapshot):
            if uuid not in missing:
                continue
            rows = await db.execute(
                update(Tariff)
                .where(Tariff.id == tariff_id, Tariff.external_squad_uuid == uuid)
                .values(external_squad_uuid=None, updated_at=datetime.now(UTC))
                .returning(Tariff.id, Tariff.name)
            )
            changed.extend((row.id, row.name) for row in rows)
        await db.commit()
    if changed:
        logger.warning(
            'Removed confirmed deleted external squad references from tariffs',
            tariff_ids=[tariff_id for tariff_id, _ in changed],
        )
        if bot is not None:
            from app.services.admin_notification_service import AdminNotificationService

            try:
                await AdminNotificationService(bot).send_deleted_external_squad_notification(changed)
            except Exception:
                # A failed notification cannot roll back committed configuration repair.
                logger.warning('Could not notify administrator about deleted external squad references')
    return changed
