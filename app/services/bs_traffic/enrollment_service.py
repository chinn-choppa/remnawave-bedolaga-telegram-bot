"""Shared post-commit panel entry for all funded initial-purchase adapters."""

import structlog
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficEnrollment, BsTrafficPanelBinding
from app.services.bs_traffic.enrollment import pending_enrollment, validate_funding
from app.services.bs_traffic.enrollment_delivery import advance_enrollment
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_admission import panel_slot
from app.services.bs_traffic.panel_binding import bind_reader, endpoint_fingerprint


logger = structlog.get_logger(__name__)


async def deliver_enrollment_from_service(service, db, subscription, *, create, sync_squads=True):
    """Return (handled, panel result). Pending work never reports final delivery.

    Ordinary provisioning retains the original canonical identity resolver. Its
    counter reset hint is disabled: the funded managed base replaces entitlement,
    not panel counters. No caller's uncommitted financial transaction is committed.
    """
    job = await pending_enrollment(db, subscription.id)
    if job is None:
        return False, None
    job_id, subscription_id = job.id, subscription.id
    if db.new or db.dirty:
        raise LedgerConflict('Commit funded enrollment before panel delivery')
    await validate_funding(db, job)
    factory = async_sessionmaker(db.bind, expire_on_commit=False, autoflush=False)
    try:
        async with service.get_api_client() as api:
            scopes = list(
                await db.scalars(
                    select(BsTrafficPanelBinding.panel_scope).where(
                        BsTrafficPanelBinding.endpoint_hash == endpoint_fingerprint(api.base_url),
                        BsTrafficPanelBinding.retired_at.is_(None),
                    )
                )
            )
            if job.panel_scope is not None:
                scopes = [scope for scope in scopes if scope == job.panel_scope]
            if len(scopes) != 1:
                return True, None
            scope = scopes[0]
            # Release the caller session before separately checkpointed ownership work.
            await db.commit()
            reader = await bind_reader(factory, api, panel_scope=scope)
            await db.refresh(subscription)
            if subscription.traffic_accounting_mode == 'legacy':
                async with panel_slot(factory, scope):
                    if create:
                        result = await service._create_before_split_enrollment(db, subscription, reset_traffic=False)
                    else:
                        result = await service._update_before_split_enrollment(
                            db, subscription, reset_traffic=False, sync_squads=sync_squads
                        )
                if result is None:
                    return True, None
                await db.refresh(subscription)
            await db.commit()
            outcome = await advance_enrollment(reader, enrollment_id=job_id)
            await db.refresh(subscription)
            if outcome not in ('verified', 'already_verified'):
                return True, None
            async with panel_slot(factory, scope):
                return True, await api.get_user_by_id(subscription.remnawave_id)
    except Exception as error:
        logger.warning(
            'Paid BS enrollment remains pending',
            enrollment_id=job_id,
            subscription_id=subscription_id,
            error_type=type(error).__name__,
        )
        await db.rollback()
        return True, None


async def enrollment_page(db, *, after_id=0, limit=100, panel_scope=None, upper_id=None):
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Bounded enrollment page required')
    return list(
        await db.scalars(
            select(BsTrafficEnrollment.id)
            .where(
                BsTrafficEnrollment.id > after_id,
                BsTrafficEnrollment.id <= upper_id if upper_id is not None else True,
                BsTrafficEnrollment.state != 'verified',
                or_(BsTrafficEnrollment.panel_scope == panel_scope, BsTrafficEnrollment.panel_scope.is_(None))
                if panel_scope is not None
                else True,
            )
            .order_by(BsTrafficEnrollment.id)
            .limit(limit)
        )
    )


async def resume_enrollment_from_service(reader, *, enrollment_id):
    """Fleet entry: checkpointed driver also provisions a missing ordinary account."""
    from app.database.models import Subscription
    from app.services.subscription_service import SubscriptionService

    await reader.assert_current()
    async with reader._factory() as db:
        job = await db.get(BsTrafficEnrollment, enrollment_id)
        if job is None or (job.panel_scope is not None and job.panel_scope != reader.panel_scope):
            return 'blocked'
        if job.state == 'verified':
            return 'already_verified'
        sub = await db.get(Subscription, job.subscription_id)
        if sub is None or sub.user_id != job.user_id:
            return 'blocked'
        service = SubscriptionService()
        # The canonical provisioner must use the same current endpoint/epoch.
        async with service.get_api_client() as api:
            if endpoint_fingerprint(api.base_url) != endpoint_fingerprint(reader._api.base_url):
                return 'blocked'
        if sub.traffic_accounting_mode != 'legacy':
            await db.rollback()
            return await advance_enrollment(reader, enrollment_id=enrollment_id)
        handled, result = await deliver_enrollment_from_service(service, db, sub, create=True)
        return 'verified' if handled and result is not None else 'pending'
