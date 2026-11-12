"""One committed reverse write; unknown outcomes are recovered with GET only."""

import asyncio
from datetime import datetime

from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficOwnershipCycle
from app.services.bs_traffic.cutover_intent import _hash
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.grace_intent import _decode
from app.services.bs_traffic.grace_restore import full_restore_confirmed
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.reverse_transfer import complete_reverse, inspect_reverse, stage_reverse
from app.services.panel_sync import writer


async def _advance_reverse(reader, *, user_id, subscription_id, account_generation):
    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound reader required for ownership return')
    keys = dict(user_id=user_id, subscription_id=subscription_id, account_generation=account_generation)
    async with asyncio.timeout(60):
        await reader.assert_current()
        async with reader._factory() as db, db.begin():
            ledger, panel_id = await _recovery_lock(db, user_id, subscription_id, account_generation)
            cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, ledger.ownership_cycle))
            if cycle is not None and cycle.state == 'managed':
                return 'managed'
            attempt = cycle.reverse_attempt if cycle is not None else None
            if attempt is not None and attempt.get('state') == 'not_sent':
                attempt = None
            if attempt is None:
                manifest = await inspect_reverse(db, reader, **keys)
        if attempt is None:
            async with panel_slot(reader._factory, reader.panel_scope):
                policy = manifest['policy']
                evidence = await collect_topology_evidence(
                    reader,
                    panel_identity=reader.panel_scope,
                    ordinary_squads=policy['topology']['ordinary_squads'],
                    bs_squads=policy['topology']['bs_squads'],
                )
                if evidence.fingerprint != policy['fingerprint']:
                    return 'topology_changed'
                source = await reader.get_split_grace_snapshot(panel_id)
                counters = await reader.get_split_transfer_snapshot(panel_id)
            async with reader._factory() as db, db.begin():
                now = await db.scalar(select(func.clock_timestamp()))
                if not evidence.is_fresh(now, panel_identity=reader.panel_scope):
                    return 'deferred'
                await stage_reverse(db, reader, **keys, source=source, counters=counters)
            # This invocation alone may send. A competing/restarted invocation
            # sees attempted and can only GET; the claim is durable before HTTP.
            async with reader._factory() as db, db.begin():
                ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
                cycle = await db.get(BsTrafficOwnershipCycle, (subscription_id, ledger.ownership_cycle))
                manifest = await inspect_reverse(db, reader, **keys, pending=True)
                attempt = cycle.reverse_attempt
                if manifest != cycle.reverse_manifest:
                    await _release_unsent(db, ledger, cycle, 'terms_changed')
                    return 'deferred'
                target = _decode(attempt['target'], _hash(attempt['target']), paid=False)
                async with panel_slot(reader._factory, reader.panel_scope):
                    fresh = await reader.get_split_grace_snapshot(panel_id)
                    now = await db.scalar(select(func.clock_timestamp()))
                    expected = _decode(attempt['source'], _hash(attempt['source']), paid=False)
                    if (
                        settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True
                        or not evidence.is_fresh(now, panel_identity=reader.panel_scope)
                        or now >= datetime.fromisoformat(attempt['deadline'])
                    ) or not full_restore_confirmed(fresh, expected):
                        await _release_unsent(db, ledger, cycle, 'source_changed_or_expired')
                        return 'deferred'
                    # Owner + subscription + binding SHARE locks survive PATCH;
                    # no renewal, disable/rebind or counter reset can race it.
                    await writer.patch_split_reverse_target(reader._api, target=target)
        async with panel_slot(reader._factory, reader.panel_scope):
            observed = await reader.get_split_grace_snapshot(panel_id)
            counters = await reader.get_split_transfer_snapshot(panel_id)
        async with reader._factory() as db, db.begin():
            return await complete_reverse(db, reader, **keys, observed=observed, counters=counters)


async def _release_unsent(db, ledger, cycle, reason):
    from app.database.models import Subscription

    cycle.reverse_attempt = dict(cycle.reverse_attempt, state='not_sent', not_sent_reason=reason)
    cycle.state = 'split'
    (await db.get(Subscription, ledger.subscription_id)).traffic_accounting_mode = 'split'
    ledger.revision += 1
    await db.flush()


async def advance_reverse(reader, *, user_id, subscription_id, account_generation):
    from app.external.remnawave_api import RemnaWaveAPIError
    from app.services.bs_traffic.panel_pause import persist_panel_pause, read_panel_pause
    from app.services.bs_traffic.recovery_backoff import backoff_for

    if not isinstance(reader, BoundPanelReader):
        raise LedgerConflict('Bound reader required for ownership return')
    if await read_panel_pause(reader._factory, reader.panel_scope):
        return 'deferred'
    try:
        return await _advance_reverse(
            reader, user_id=user_id, subscription_id=subscription_id, account_generation=account_generation
        )
    except RemnaWaveAPIError as error:
        pause = backoff_for(error)
        if pause is not None:
            await persist_panel_pause(reader._factory, reader.panel_scope, pause.retry_after_seconds)
        return 'unconfirmed'
