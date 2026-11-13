"""Opt-in bounded lifecycle for split accounts and already-funded enrollment receipts.

Readiness requires a recent fully covered collection/settlement round, not merely a live loop.
Scan sessions close before network work. Existing delivery primitives retain
their own bounded business locks and durable unknown-outcome handling.
"""

import asyncio
import math
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import structlog
from sqlalchemy import DateTime, cast, func, select

from app.config import settings
from app.database.models import (
    BsTrafficCollectionJob,
    BsTrafficDelivery,
    BsTrafficEnrollment,
    BsTrafficFleetLane,
    BsTrafficFleetRound,
    BsTrafficFleetWork,
    BsTrafficLedger,
    BsTrafficOwnershipCycle,
    BsTrafficPanelBinding,
    BsTrafficRenewalIntent,
    Subscription,
    Tariff,
)
from app.services.bs_traffic.delivery_maintenance import expire_pending
from app.services.bs_traffic.drain_worker import process_drain_page
from app.services.bs_traffic.panel_binding import bind_reader, endpoint_fingerprint
from app.services.bs_traffic.purchase_delivery import advance_purchase_delivery
from app.services.bs_traffic.purchase_delivery_worker import purchase_delivery_page
from app.services.bs_traffic.queue import run_once as collect_once


logger = structlog.get_logger(__name__)
ENROLLMENT_STEP_SECONDS = 20
REVERSE_STEP_SECONDS = 20


class SplitTrafficRuntime:
    def __init__(
        self,
        *,
        enabled=False,
        session_factory=None,
        service_factory=None,
        page_size=500,
        concurrency=2,
        interval_seconds=30,
    ):
        if type(enabled) is not bool:
            raise ValueError('Explicit runtime enabled flag required')
        if type(page_size) is not int or not 1 <= page_size <= 500:
            raise ValueError('Page size must be between 1 and 500')
        if type(concurrency) is not int or not 1 <= concurrency <= 8:
            raise ValueError('Concurrency must be between 1 and 8')
        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (float, int))
            or not math.isfinite(interval_seconds)
            or not 5 <= interval_seconds <= 300
        ):
            raise ValueError('Interval must be between 5 and 300 seconds')
        self.enabled = enabled
        self.factory = session_factory
        self.service_factory = service_factory
        self.page_size = page_size
        self.interval = interval_seconds
        self.task = None
        self._stop = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._healthy = {}
        self._drop_warnings = {}
        self._tail_warnings = {}
        self._more = False
        self._has_bindings = False
        self._scope_failures = set()
        self._lane_tokens = {}

    def ready(self, panel_scope):
        if not self.enabled or self.task is None or self.task.done():
            return False
        checked = self._healthy.get(panel_scope)
        return checked is not None and asyncio.get_running_loop().time() - checked < self.interval * 3

    async def start(self, *, enabled=None):
        if enabled is not None:
            if type(enabled) is not bool:
                raise ValueError('Explicit runtime enabled flag required')
            self.enabled = enabled
        if not self.enabled or (self.task is not None and not self.task.done()):
            return
        self._stop.clear()
        self._drop_warnings.clear()
        self._tail_warnings.clear()
        self._healthy.clear()
        self.task = asyncio.create_task(self._run(), name='split-traffic-runtime')

    async def stop(self):
        self._stop.set()
        self._drop_warnings.clear()
        self._tail_warnings.clear()
        self._healthy.clear()
        task = self.task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.task = None

    async def _run(self):
        if self.factory is None:
            from app.database.database import AsyncSessionLocal

            self.factory = AsyncSessionLocal
        if self.service_factory is None:
            from app.services.remnawave_service import RemnaWaveService

            self.service_factory = RemnaWaveService
        try:
            while not self._stop.is_set():
                try:
                    await self.run_once()
                except Exception as error:
                    self._healthy.clear()
                    logger.warning('Split runtime pass failed', error_type=type(error).__name__)
                try:
                    delay = 0.05 if self._more else min(self.interval, 1) if self._has_bindings else self.interval
                    await asyncio.wait_for(self._stop.wait(), delay)
                except TimeoutError:
                    pass
        finally:
            self._healthy.clear()

    async def run_once(self):
        self._more = False
        service = self.service_factory()
        if not service.is_configured:
            self._has_bindings = False
            self._healthy.clear()
            return
        async with service.get_api_client() as api:
            digest = endpoint_fingerprint(api.base_url)
            async with self.factory() as db:
                scopes = list(
                    await db.scalars(
                        select(BsTrafficPanelBinding.panel_scope)
                        .where(
                            BsTrafficPanelBinding.endpoint_hash == digest,
                            BsTrafficPanelBinding.retired_at.is_(None),
                        )
                        .order_by(BsTrafficPanelBinding.panel_scope)
                        .limit(17)
                    )
                )
            # An unexpected number of live epochs is configuration trouble;
            # never silently ignore later epochs while declaring health.
            self._has_bindings = bool(scopes)
            if len(scopes) > 16:
                raise ValueError('Too many live panel bindings')
            for scope in (set(self._healthy) | set(self._drop_warnings) | set(self._tail_warnings)) - set(scopes):
                self._healthy.pop(scope, None)
                self._drop_warnings.pop(scope, None)
                self._tail_warnings.pop(scope, None)
            readers = [await bind_reader(self.factory, api, panel_scope=scope) for scope in scopes]
            async with asyncio.TaskGroup() as tasks:
                for reader in readers:
                    tasks.create_task(self._guard_scope(reader))

    async def _guard_scope(self, reader):
        async with self._semaphore:
            try:
                async with asyncio.timeout(120):
                    healthy_at = await self._process_scope(reader)
            except Exception as error:
                self._healthy.pop(reader.panel_scope, None)
                logger.warning('Split scope pass failed', error_type=type(error).__name__)
            else:
                if healthy_at is None:
                    self._healthy.pop(reader.panel_scope, None)
                else:
                    age = max(0, (datetime.now(UTC) - healthy_at).total_seconds())
                    self._healthy[reader.panel_scope] = asyncio.get_running_loop().time() - age

    async def _process_scope(self, reader):
        from app.services.bs_traffic.evidence_cache import TopologyEvidenceCache
        from app.services.bs_traffic.fleet_round import finish_round

        await reader.assert_current()
        scope = reader.panel_scope
        self._scope_failures.discard(scope)
        cache = TopologyEvidenceCache(reader, panel_identity=scope)
        # The lanes receive independent cooperative budgets. A deferred
        # collection or paid receipt cannot suppress local expiry/settlement.
        async with asyncio.TaskGroup() as lanes:
            lanes.create_task(self._collection(reader))
            lanes.create_task(self._local(reader, cache))
            lanes.create_task(self._maintenance(reader))
            lanes.create_task(self._drops(reader))
            lanes.create_task(self._reverse(reader))
        # A just-finished collection may have published local tasks this pass.
        await self._local(reader, cache)
        # Cutover consumes the fresh node-shared raw collected above. Starting
        # this lane in parallel can phase-lock its retry to an expired baseline.
        await self._enrollment_pass(reader)
        async with self.factory() as db, db.begin():
            await finish_round(db, scope)
        return await self._health(reader)

    async def _health(self, reader):
        from app.services.bs_traffic.panel_pause import read_panel_pause

        scope = reader.panel_scope
        await reader.assert_current()
        if scope in self._scope_failures or await read_panel_pause(self.factory, scope) > 0:
            return None
        async with self.factory() as db:
            row = await db.get(BsTrafficFleetRound, scope)
            now = await db.scalar(select(func.clock_timestamp()))
            from app.database.models import BsTrafficNodeMembership, BsTrafficNodeTail

            tail_attention = await db.scalar(
                select(BsTrafficNodeMembership.id)
                .where(BsTrafficNodeMembership.panel_scope == scope, BsTrafficNodeMembership.state == 'attention')
                .limit(1)
            )
            self._tail_warnings.pop(scope, None)
            if tail_attention is not None:
                self._tail_warnings[scope] = 'node_tail_attention'
            elif (
                await db.scalar(
                    select(BsTrafficNodeTail.membership_id)
                    .join(BsTrafficNodeMembership, BsTrafficNodeMembership.id == BsTrafficNodeTail.membership_id)
                    .where(
                        BsTrafficNodeMembership.panel_scope == scope,
                        BsTrafficNodeMembership.state == 'tail',
                        (
                            BsTrafficNodeTail.last_error.is_not(None)
                            | (BsTrafficNodeTail.next_poll_at < now - timedelta(seconds=self.interval * 3))
                        ),
                    )
                    .limit(1)
                )
                is not None
            ):
                self._tail_warnings[scope] = 'node_tail_delayed'

            if row is None or row.last_success_at is None or row.error_count:
                return None
            returned = (
                select(BsTrafficOwnershipCycle.subscription_id)
                .join(
                    BsTrafficLedger,
                    (BsTrafficLedger.subscription_id == BsTrafficOwnershipCycle.subscription_id)
                    & (BsTrafficLedger.ownership_cycle == BsTrafficOwnershipCycle.ownership_cycle),
                )
                .where(
                    BsTrafficLedger.panel_scope == scope,
                    BsTrafficOwnershipCycle.state == 'managed',
                    BsTrafficOwnershipCycle.reverse_attempt['state'].as_string() == 'verified',
                )
            )
            bad = await db.scalar(
                select(BsTrafficFleetWork.subscription_id)
                .where(
                    BsTrafficFleetWork.panel_scope == scope,
                    BsTrafficFleetWork.subscription_id.not_in(returned),
                    (
                        BsTrafficFleetWork.error.is_not(None)
                        | BsTrafficFleetWork.access_dirty
                        | BsTrafficFleetWork.settled_at.is_(None)
                        | (BsTrafficFleetWork.settled_at < now - timedelta(seconds=self.interval * 3))
                    ),
                )
                .limit(1)
            )
            if bad is not None or row.last_success_at < now - timedelta(seconds=self.interval * 3):
                return None
            pending_reverse = await db.scalar(
                select(BsTrafficOwnershipCycle.subscription_id)
                .join(
                    BsTrafficLedger,
                    (BsTrafficLedger.subscription_id == BsTrafficOwnershipCycle.subscription_id)
                    & (BsTrafficLedger.ownership_cycle == BsTrafficOwnershipCycle.ownership_cycle),
                )
                .where(BsTrafficLedger.panel_scope == scope, BsTrafficOwnershipCycle.state == 'reverse_pending')
                .limit(1)
            )
            if pending_reverse is not None:
                return None
            stale_receipt = await db.scalar(
                select(BsTrafficRenewalIntent.id)
                .where(
                    BsTrafficRenewalIntent.panel_scope == scope,
                    BsTrafficRenewalIntent.state.in_(('pending', 'attempted')),
                    BsTrafficRenewalIntent.created_at < now - timedelta(seconds=self.interval * 3),
                )
                .limit(1)
            )
            uncertain_delivery = await db.scalar(
                select(BsTrafficDelivery.id)
                .join(BsTrafficLedger, BsTrafficLedger.subscription_id == BsTrafficDelivery.subscription_id)
                .where(
                    BsTrafficLedger.panel_scope == scope,
                    (BsTrafficDelivery.state == 'attention')
                    | (
                        BsTrafficDelivery.state.in_(('pending', 'attempted'))
                        & (BsTrafficDelivery.created_at < now - timedelta(seconds=self.interval * 3))
                    ),
                )
                .limit(1)
            )
            self._drop_warnings.pop(scope, None)
            if settings.BS_TRAFFIC_FORCE_DISCONNECT_ENABLED is True:
                drop_lag = await db.scalar(
                    select(BsTrafficDelivery.connection_drop['state'].as_string())
                    .join(BsTrafficLedger, BsTrafficLedger.subscription_id == BsTrafficDelivery.subscription_id)
                    .where(
                        BsTrafficLedger.panel_scope == scope,
                        BsTrafficDelivery.state == 'verified',
                        (BsTrafficDelivery.connection_drop['state'].as_string() == 'attempted')
                        | (
                            (BsTrafficDelivery.connection_drop['state'].as_string() == 'pending')
                            & (BsTrafficDelivery.created_at < now - timedelta(seconds=self.interval * 3))
                        ),
                    )
                    .order_by((BsTrafficDelivery.connection_drop['state'].as_string() == 'attempted').desc())
                    .limit(1)
                )
                if drop_lag is not None:
                    self._drop_warnings[scope] = (
                        'connection_drop_unknown' if drop_lag == 'attempted' else 'connection_drop_delayed'
                    )
            stale_enrollment = await db.scalar(
                select(BsTrafficEnrollment.id)
                .where(
                    (BsTrafficEnrollment.panel_scope == scope) | BsTrafficEnrollment.panel_scope.is_(None),
                    BsTrafficEnrollment.state != 'verified',
                    BsTrafficEnrollment.created_at < now - timedelta(seconds=self.interval * 3),
                )
                .limit(1)
            )
            if stale_receipt is not None or uncertain_delivery is not None or stale_enrollment is not None:
                return None
            return row.last_success_at

    async def _collection(self, reader):
        from app.services.bs_traffic.fleet_round import enqueue_node, plan_page

        deadline = asyncio.get_running_loop().time() + 2
        try:
            from app.services.bs_traffic.node_tail_collection import enqueue_due_tails

            async with self.factory() as db, db.begin():
                now = await db.scalar(select(func.clock_timestamp()))
                self._more |= bool(await enqueue_due_tails(db, reader, now=now))
            while True:
                async with self.factory() as db, db.begin():
                    page = await plan_page(db, reader, limit=self.page_size, interval_seconds=self.interval)
                if not page.more or asyncio.get_running_loop().time() >= deadline:
                    self._more |= page.more
                    break
            # Bounded producer watermark; a sleeping retry no longer prevents
            # planning unrelated nodes. Collection HTTP retains one scope lease.
            for _ in range(8):
                async with self.factory() as db, db.begin():
                    pending = await db.scalar(
                        select(func.count())
                        .select_from(BsTrafficCollectionJob)
                        .where(
                            BsTrafficCollectionJob.panel_scope == reader.panel_scope,
                            BsTrafficCollectionJob.state.in_(('pending', 'leased')),
                        )
                    )
                    more = await enqueue_node(db, reader) if pending < 16 else False
                if not more:
                    break
                self._more = True
            for _ in range(2):
                if not await collect_once(self.factory, reader, reader.panel_scope):
                    break
                self._more = True
            async with self.factory() as db, db.begin():
                self._more |= await enqueue_node(db, reader)
        except Exception as error:
            self._scope_failures.add(reader.panel_scope)
            logger.warning('Split collection lane deferred', error_type=type(error).__name__)

    async def _local(self, reader, cache):
        from app.services.bs_traffic.fleet_idle import settle_idle_batch
        from app.services.bs_traffic.fleet_settlement import run_local_once

        deadline = asyncio.get_running_loop().time() + 2
        while True:
            try:
                completed = await settle_idle_batch(
                    self.factory, reader, cache, interval_seconds=self.interval, settle_deltas=True
                )
            except Exception as error:
                completed = 0
                self._scope_failures.add(reader.panel_scope)
                logger.warning('Split idle batch deferred', error_type=type(error).__name__)
            self._more |= bool(completed)
            async with asyncio.TaskGroup() as tasks:
                running = [
                    tasks.create_task(run_local_once(self.factory, reader, cache, interval_seconds=self.interval))
                    for _ in range(4)
                ]
            outcomes = [task.result() for task in running]
            if not any(outcomes) and not completed:
                break
            self._more = True
            if asyncio.get_running_loop().time() >= deadline:
                break

    async def _maintenance(self, reader):
        for lane in (self._recover, self._renewals, self._drain):
            try:
                async with asyncio.timeout(30):
                    await lane(reader)
            except Exception as error:
                self._scope_failures.add(reader.panel_scope)
                logger.warning('Split maintenance lane deferred', error_type=type(error).__name__)

    async def _enrollment_pass(self, reader):
        try:
            async with asyncio.timeout(25):
                await self._enrollments(reader)
        except Exception as error:
            self._scope_failures.add(reader.panel_scope)
            logger.warning('Split enrollment lane deferred', error_type=type(error).__name__)

    async def _load_lane(self, scope, kind, model, key):
        async with self.factory() as db, db.begin():
            from sqlalchemy.dialects.postgresql import insert

            now = await db.scalar(select(func.clock_timestamp()))
            await db.execute(
                insert(BsTrafficFleetLane)
                .values(panel_scope=scope, kind=kind, cursor=0, upper_id=0, due_at=now)
                .on_conflict_do_nothing()
            )
            row = await db.get(BsTrafficFleetLane, (scope, kind), with_for_update=True, populate_existing=True)
            if row.due_at > now or (row.lease_until is not None and row.lease_until > now):
                return None
            if row.upper_id == 0:
                row.upper_id = await db.scalar(select(func.coalesce(func.max(key), 0)).select_from(model))
            row.lease_until = now + timedelta(seconds=60)
            row.lease_token = str(uuid4())
            self._lane_tokens[(scope, kind)] = row.lease_token
            return row.cursor, row.upper_id

    async def _save_lane(self, scope, kind, cursor, *, complete=False, release=False):
        async with self.factory() as db, db.begin():
            row = await db.get(BsTrafficFleetLane, (scope, kind), with_for_update=True, populate_existing=True)
            now = await db.scalar(select(func.clock_timestamp()))
            if (
                row.lease_token != self._lane_tokens.get((scope, kind))
                or row.lease_until is None
                or row.lease_until <= now
            ):
                return
            row.cursor = 0 if complete else max(row.cursor, cursor)
            if release:
                row.lease_until, row.lease_token = None, None
            if complete:
                row.upper_id, row.completed_at, row.due_at = 0, now, now + timedelta(seconds=self.interval)
            else:
                self._more = True

    async def _drain(self, reader):
        bounds = await self._load_lane(reader.panel_scope, 'drain', BsTrafficLedger, BsTrafficLedger.subscription_id)
        if bounds is None:
            return
        async with self.factory() as db:
            rows = list(
                await db.scalars(
                    select(BsTrafficLedger.subscription_id)
                    .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
                    .join(Tariff, Tariff.id == Subscription.tariff_id)
                    .where(
                        BsTrafficLedger.panel_scope == reader.panel_scope,
                        BsTrafficLedger.subscription_id > bounds[0],
                        BsTrafficLedger.subscription_id <= bounds[1],
                        func.coalesce(Tariff.bs_traffic_settings['enabled'].as_boolean(), True)
                        != func.coalesce(BsTrafficLedger.access_policy['bs_purchase_enabled'].as_boolean(), True),
                    )
                    .order_by(BsTrafficLedger.subscription_id)
                    .limit(4)
                )
            )
        for identifier in rows:
            try:
                outcome = await process_drain_page(
                    self.factory, panel_scope=reader.panel_scope, after_id=identifier - 1, upper_id=identifier, limit=1
                )
                if outcome.deferred:
                    self._scope_failures.add(reader.panel_scope)
            except Exception:
                self._scope_failures.add(reader.panel_scope)
            await self._save_lane(reader.panel_scope, 'drain', identifier)
        await self._save_lane(
            reader.panel_scope,
            'drain',
            rows[-1] if rows else 0,
            complete=len(rows) < 4 or rows[-1] >= bounds[1],
            release=True,
        )

    async def _recover(self, reader):
        from app.services.bs_traffic.recovery_schedule import register_recovery_scope, run_recovery_once

        scope = reader.panel_scope
        async with self.factory() as db, db.begin():
            await expire_pending(db, panel_scope=scope, limit=self.page_size)
            await register_recovery_scope(db, panel_scope=scope)
        # Existing durable recovery scheduling has a finite highwater and
        # provider-aware backoff; never construct a fresh window per page.
        try:
            await run_recovery_once(self.factory, reader, panel_scope=scope)
        except Exception:
            self._scope_failures.add(scope)
        bounds = await self._load_lane(scope, 'purchase', BsTrafficDelivery, BsTrafficDelivery.id)
        if bounds is None:
            return
        async with self.factory() as db:
            page = await purchase_delivery_page(
                db, panel_scope=scope, after_id=bounds[0], upper_id=bounds[1], limit=min(self.page_size, 4)
            )
        for candidate in page.items:
            try:
                await advance_purchase_delivery(reader, **asdict(candidate))
            except Exception:
                self._scope_failures.add(scope)
            # An individual poison receipt never pins a whole fleet page.
            await self._save_lane(scope, 'purchase', candidate.operation_id)
        await self._save_lane(scope, 'purchase', page.next_after or 0, complete=page.next_after is None, release=True)

    async def _reverse(self, reader):
        try:
            async with asyncio.timeout(25):
                await self._reverse_page(reader)
        except Exception as error:
            self._scope_failures.add(reader.panel_scope)
            logger.warning('Split reverse lane deferred', error_type=type(error).__name__)

    async def _reverse_page(self, reader):
        from app.services.bs_traffic.ledger import LedgerConflict, activated_owner_query
        from app.services.bs_traffic.reverse_delivery import advance_reverse

        scope = reader.panel_scope
        bounds = await self._load_lane(scope, 'reverse', BsTrafficLedger, BsTrafficLedger.subscription_id)
        if bounds is None:
            return
        async with self.factory() as db:
            active = activated_owner_query().with_only_columns(BsTrafficLedger.subscription_id)
            pending = select(BsTrafficOwnershipCycle.subscription_id).where(
                BsTrafficOwnershipCycle.ownership_cycle == BsTrafficLedger.ownership_cycle,
                BsTrafficOwnershipCycle.state == 'reverse_pending',
            )
            rows = (
                await db.execute(
                    select(BsTrafficLedger.subscription_id, BsTrafficLedger.account_generation, Subscription.user_id)
                    .join(Subscription, Subscription.id == BsTrafficLedger.subscription_id)
                    .where(
                        BsTrafficLedger.panel_scope == scope,
                        BsTrafficLedger.subscription_id > bounds[0],
                        BsTrafficLedger.subscription_id <= bounds[1],
                        (
                            BsTrafficLedger.subscription_id.in_(pending)
                            & (Subscription.traffic_accounting_mode == 'split_pending')
                        )
                        | (
                            BsTrafficLedger.subscription_id.in_(active)
                            & (BsTrafficLedger.access_policy['bs_purchase_enabled'].as_boolean().is_(False))
                            & (
                                cast(
                                    BsTrafficLedger.access_policy['bs_drain_until'].as_string(), DateTime(timezone=True)
                                )
                                <= func.clock_timestamp()
                            )
                        ),
                    )
                    .order_by(BsTrafficLedger.subscription_id)
                    .limit(1)
                )
            ).all()
        for row in rows:
            try:
                async with asyncio.timeout(REVERSE_STEP_SECONDS):
                    outcome = await advance_reverse(
                        reader,
                        user_id=row.user_id,
                        subscription_id=row.subscription_id,
                        account_generation=row.account_generation,
                    )
                if outcome not in ('managed', 'obsolete', 'deferred'):
                    self._scope_failures.add(scope)
            except LedgerConflict:
                # A pre-claim wait (grace, pending usage, future paid rights)
                # keeps split ownership. Durable reverse_pending is checked
                # independently by _health even when its recovery conflicts.
                logger.debug('Split reverse awaits local obligations')
            except Exception as error:
                self._scope_failures.add(scope)
                logger.warning('Split reverse lane deferred', error_type=type(error).__name__)
            await self._save_lane(scope, 'reverse', row.subscription_id)
        await self._save_lane(
            scope,
            'reverse',
            rows[-1].subscription_id if rows else 0,
            complete=not rows or rows[-1].subscription_id >= bounds[1],
            release=True,
        )

    async def _drops(self, reader):
        if settings.BS_TRAFFIC_FORCE_DISCONNECT_ENABLED is not True:
            return
        try:
            async with asyncio.timeout(20):
                await self._drop_page(reader)
        except Exception as error:
            self._drop_warnings[reader.panel_scope] = 'connection_drop_delayed'
            logger.warning('Split disconnect lane deferred', error_type=type(error).__name__)

    async def _drop_page(self, reader):
        from app.services.bs_traffic.connection_drop import drop_after_delivery

        scope = reader.panel_scope
        bounds = await self._load_lane(scope, 'drop', BsTrafficDelivery, BsTrafficDelivery.id)
        if bounds is None:
            return
        async with self.factory() as db:
            rows = (
                await db.execute(
                    select(
                        BsTrafficDelivery.id,
                        BsTrafficDelivery.subscription_id,
                        BsTrafficLedger.account_generation,
                        Subscription.user_id,
                    )
                    .join(BsTrafficLedger, BsTrafficLedger.subscription_id == BsTrafficDelivery.subscription_id)
                    .join(Subscription, Subscription.id == BsTrafficDelivery.subscription_id)
                    .where(
                        BsTrafficLedger.panel_scope == scope,
                        BsTrafficDelivery.state == 'verified',
                        BsTrafficDelivery.connection_drop['state'].as_string() == 'pending',
                        BsTrafficDelivery.id > bounds[0],
                        BsTrafficDelivery.id <= bounds[1],
                    )
                    .order_by(BsTrafficDelivery.id)
                    .limit(1)
                )
            ).all()
        for row in rows:
            try:
                outcome = await drop_after_delivery(
                    reader,
                    user_id=row.user_id,
                    subscription_id=row.subscription_id,
                    account_generation=row.account_generation,
                    operation_id=row.id,
                )
                if outcome not in ('accepted', 'cancelled', 'not_required'):
                    self._drop_warnings[scope] = 'connection_drop_delayed'
            except Exception:
                self._drop_warnings[scope] = 'connection_drop_delayed'
            await self._save_lane(scope, 'drop', row.id)
        await self._save_lane(
            scope, 'drop', rows[-1].id if rows else 0, complete=not rows or rows[-1].id >= bounds[1], release=True
        )

    async def _enrollments(self, reader):
        from app.services.bs_traffic.enrollment_service import enrollment_page, resume_enrollment_from_service

        scope = reader.panel_scope
        bounds = await self._load_lane(scope, 'enrollment', BsTrafficEnrollment, BsTrafficEnrollment.id)
        if bounds is None:
            return
        async with self.factory() as db:
            rows = await enrollment_page(db, panel_scope=scope, after_id=bounds[0], upper_id=bounds[1], limit=1)
        for identifier in rows:
            try:
                # Canonical service owns missing-account identity resolution;
                # all transfer/payment checkpoints remain owned by its driver.
                async with asyncio.timeout(ENROLLMENT_STEP_SECONDS):
                    outcome = await resume_enrollment_from_service(reader, enrollment_id=identifier)
                if outcome not in ('verified', 'already_verified'):
                    self._scope_failures.add(scope)
            except Exception:
                self._scope_failures.add(scope)
            await self._save_lane(scope, 'enrollment', identifier)
        await self._save_lane(
            scope, 'enrollment', rows[-1] if rows else 0, complete=not rows or rows[-1] >= bounds[1], release=True
        )

    async def _renewals(self, reader):
        from app.services.bs_traffic.renewal_grace import advance_paid_renewal

        scope = reader.panel_scope
        bounds = await self._load_lane(scope, 'renewal', BsTrafficRenewalIntent, BsTrafficRenewalIntent.id)
        if bounds is None:
            return
        async with self.factory() as db:
            rows = (
                await db.execute(
                    select(
                        BsTrafficRenewalIntent.id,
                        BsTrafficRenewalIntent.subscription_id,
                        BsTrafficRenewalIntent.account_generation,
                        Subscription.user_id,
                    )
                    .join(Subscription, Subscription.id == BsTrafficRenewalIntent.subscription_id)
                    .where(
                        BsTrafficRenewalIntent.panel_scope == scope,
                        BsTrafficRenewalIntent.state.in_(('pending', 'attempted')),
                        BsTrafficRenewalIntent.id > bounds[0],
                        BsTrafficRenewalIntent.id <= bounds[1],
                    )
                    .order_by(BsTrafficRenewalIntent.id)
                    .limit(4)
                )
            ).all()
        for row in rows:
            try:
                await advance_paid_renewal(
                    reader,
                    user_id=row.user_id,
                    subscription_id=row.subscription_id,
                    account_generation=row.account_generation,
                    operation_id=row.id,
                )
            except Exception:
                self._scope_failures.add(scope)
            await self._save_lane(scope, 'renewal', row.id)
        await self._save_lane(
            scope, 'renewal', rows[-1].id if rows else 0, complete=not rows or rows[-1].id >= bounds[1], release=True
        )


split_traffic_runtime = SplitTrafficRuntime()


def runtime_ready(panel_scope):
    return split_traffic_runtime.ready(panel_scope)


def runtime_status(panel_scope):
    """Accounting readiness and optional delivery warnings, never socket proof."""
    warning = split_traffic_runtime._drop_warnings.get(panel_scope)
    tail_warning = split_traffic_runtime._tail_warnings.get(panel_scope)
    return {
        'purchase_ready': split_traffic_runtime.ready(panel_scope),
        'warnings': [value for value in (warning, tail_warning) if value],
    }
