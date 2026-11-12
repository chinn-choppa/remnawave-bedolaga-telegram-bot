"""Atomic local transfer into split_pending; no panel write or runtime activation.

Caller commits/rolls back the transaction. The enrollment coordinator supplies
validated funding and follows this transfer with guarded cutover delivery.
Uncertain late usage may never be charged against newly purchased grants.
"""

from datetime import UTC, timedelta

from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficManagedTransfer,
    BsTrafficPanelBinding,
    Subscription,
    Tariff,
    TrafficAccountingState,
)
from app.services.bs_traffic.identity import account_generation as generation_of
from app.services.bs_traffic.ledger import LedgerConflict, _key, grant
from app.services.bs_traffic.managed_transfer import inspect_managed_transfer
from app.services.bs_traffic.managed_transfer_capture import ManagedTransferObservation
from app.services.bs_traffic.readback import _recovery_lock
from app.services.bs_traffic.tariff_settings import BsTrafficSettings


async def _commit_managed_transfer(
    db, *, user_id, subscription_id, account_generation, operation_key, observation, enrollment_id=None
):
    _key(operation_key)
    if not isinstance(observation, ManagedTransferObservation):
        raise LedgerConflict('Bound managed transfer observation required')
    manifest = observation.manifest
    if (
        manifest.user_id != user_id
        or manifest.subscription_id != subscription_id
        or manifest.account_generation != account_generation
    ):
        raise LedgerConflict('Transfer observation belongs to another account')
    ledger, _ = await _recovery_lock(db, user_id, subscription_id, account_generation)
    from app.services.bs_traffic.ownership_cycle import current_transfer

    old = await current_transfer(db, subscription_id)
    if old is not None:
        if (old.operation_key, old.account_generation, old.manifest_hash, old.destination_group) != (
            operation_key,
            account_generation,
            manifest.fingerprint,
            manifest.destination_group,
        ):
            raise LedgerConflict('A different transfer already owns this subscription')
        return old.subscription_id  # Historical receipt, no grant or activation.
    if not settings.is_tariffs_mode():
        raise LedgerConflict('Split enrollment requires tariff sales mode')
    # Recheck after observation and under the ownership locks. A dry-run or a
    # global uncertainty opt-in is not permission to enroll a disabled tariff.
    tariff = await db.scalar(
        select(Tariff)
        .join(Subscription, Subscription.tariff_id == Tariff.id)
        .where(Subscription.id == subscription_id)
        .with_for_update(read=True, of=Tariff)
        .execution_options(populate_existing=True)
    )
    try:
        enabled = tariff is not None and BsTrafficSettings.model_validate(tariff.bs_traffic_settings).enabled
    except ValueError:
        enabled = False
    if enrollment_id is not None:
        from app.database.models import BsTrafficEnrollment
        from app.services.bs_traffic.enrollment import validate_funding

        enrollment = await db.scalar(
            select(BsTrafficEnrollment).where(BsTrafficEnrollment.id == enrollment_id).with_for_update()
        )
        if (
            enrollment is None
            or enrollment.user_id != user_id
            or enrollment.subscription_id != subscription_id
            or enrollment.account_generation != account_generation
            or enrollment.panel_scope != manifest.panel_scope
            or enrollment.tariff_id != manifest.tariff_id
            or enrollment.state != 'provisioned'
            or enrollment.ownership_cycle != ledger.ownership_cycle
            or enrollment.terms['paid_until'] != manifest.paid_until.isoformat()
            or operation_key != f'enrollment:{enrollment_id}:transfer'
        ):
            raise LedgerConflict('Funded enrollment does not authorize this transfer')
        await validate_funding(db, enrollment)
        # Stopping new sales cannot revoke terms frozen in an accepted payment.
        enabled = True
    if not enabled:
        raise LedgerConflict('Split enrollment requires an enabled BS tariff')
    if settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
        raise LedgerConflict('Transfer requires explicit operator acceptance of uncertain usage write-off')
    fresh = await inspect_managed_transfer(
        db,
        user_id=user_id,
        subscription_id=subscription_id,
        account_generation=account_generation,
        destination_group=manifest.destination_group,
    )
    binding = await db.scalar(
        select(BsTrafficPanelBinding.panel_scope)
        .where(BsTrafficPanelBinding.panel_scope == manifest.panel_scope, BsTrafficPanelBinding.retired_at.is_(None))
        .with_for_update(read=True)
    )
    now = await db.scalar(select(func.clock_timestamp()))
    account = observation.account
    if (
        binding is None
        or fresh != manifest
        or ledger.panel_scope != manifest.panel_scope
        or not observation.observed_at <= now < observation.valid_until
        or observation.valid_until > observation.observed_at + timedelta(seconds=15)
        or generation_of(panel_scope=manifest.panel_scope, account=account) != account_generation
        or f'{account.id}@{account.created_at.astimezone(UTC).isoformat()}' != manifest.source_identity
        or account.used_bytes != manifest.used_bytes
        or account.lifetime_bytes != manifest.lifetime_bytes
        or account.traffic_limit_strategy != 'NO_RESET'
        or account.traffic_limit_bytes
        != (0 if manifest.source_phase == 'unlimited' else max(1, manifest.used_bytes + manifest.total_bytes))
    ):
        raise LedgerConflict('Transfer observation is stale or inconsistent')
    entries = []
    for item in manifest.grants:
        identifier = await grant(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            group=manifest.destination_group,
            operation_key=item.destination_key,
            amount_bytes=item.remaining_bytes,
            starts_at=now,
            expires_at=item.expires_at,
        )
        entries.append(
            dict(
                source_key=item.source_key,
                original_bytes=item.original_bytes,
                imported_bytes=item.remaining_bytes,
                expires_at=item.expires_at.isoformat(),
                grant_id=identifier,
            )
        )
    source = await db.get(TrafficAccountingState, subscription_id)
    from app.services.bs_traffic.initial_calendar import capture_initial_calendar
    from app.services.bs_traffic.policy import _read_policy

    calendar_terms = await capture_initial_calendar(
        db,
        manifest=manifest,
        source=source,
        policy=_read_policy(ledger.access_policy) if ledger.access_policy is not None else None,
        now=now,
    )
    source.phase = 'transferred'
    source.revision += 1
    (await db.get(Subscription, subscription_id)).traffic_accounting_mode = 'split_pending'
    ledger.revision += 1
    previous = await db.get(BsTrafficManagedTransfer, subscription_id)
    if previous is not None:
        from app.database.models import BsTrafficOwnershipCycle
        from app.services.bs_traffic.ownership_cycle import encode_row

        archived = await db.get(BsTrafficOwnershipCycle, (subscription_id, previous.ownership_cycle))
        if archived is None or archived.state != 'managed' or archived.forward_receipt != encode_row(previous):
            raise LedgerConflict('Previous ownership receipt must be archived before replacement')
    replacement = BsTrafficManagedTransfer(
        subscription_id=subscription_id,
        ownership_cycle=ledger.ownership_cycle,
        operation_key=operation_key,
        account_generation=account_generation,
        panel_scope=manifest.panel_scope,
        manifest_hash=manifest.fingerprint,
        destination_group=manifest.destination_group,
        source_identity=manifest.source_identity,
        source_phase=manifest.source_phase,
        source_revision=manifest.source_revision,
        source_lifetime_bytes=manifest.lifetime_bytes,
        source_used_bytes=manifest.used_bytes,
        imported_bytes=manifest.total_bytes,
        grants=entries,
        calendar_terms=calendar_terms,
        uncertain_policy='operator_write_off',
        created_at=now,
    )
    if previous is None:
        db.add(replacement)
    else:
        for column in BsTrafficManagedTransfer.__table__.columns:
            setattr(previous, column.name, getattr(replacement, column.name))
    await db.flush()
    # Changes must remain in the caller transaction until this final check.
    finished = await db.scalar(select(func.clock_timestamp()))
    if finished >= observation.valid_until or settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
        raise LedgerConflict('Transfer authorization expired before commit')
    return subscription_id


async def commit_managed_transfer(
    db, *, user_id, subscription_id, account_generation, operation_key, observation, enrollment_id=None
):
    # A caller may catch LedgerConflict and commit unrelated work. Savepoint
    # rollback must still remove every imported grant and ownership change.
    async with db.begin_nested():
        return await _commit_managed_transfer(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            account_generation=account_generation,
            operation_key=operation_key,
            observation=observation,
            enrollment_id=enrollment_id,
        )
