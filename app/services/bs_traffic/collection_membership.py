"""Collection-only discovery, including funded transfers awaiting cutover.

This query is never authority to settle paid usage or apply access. Pending
candidates are revalidated against their funding and current transfer before
being added to a node-shared collection round.
"""

from sqlalchemy import Integer, literal, select, union_all

from app.database.models import (
    BsTrafficEnrollment as Enrollment,
    BsTrafficLedger as Ledger,
    BsTrafficManagedTransfer as Transfer,
    BsTrafficPanelBinding as Binding,
    Subscription,
    TrafficAccountingState as Source,
)
from app.services.bs_traffic.ledger import activated_owner_query


def collection_membership():
    columns = (
        Ledger.subscription_id,
        Ledger.panel_user_id,
        Ledger.account_generation,
        Ledger.access_policy,
        Subscription.user_id,
    )
    active = activated_owner_query().with_only_columns(
        *columns, literal(None, type_=Integer).label('enrollment_id'), maintain_column_froms=True
    )
    pending = (
        select(*columns, Enrollment.id.label('enrollment_id'))
        .join(Subscription, Subscription.id == Ledger.subscription_id)
        .join(Enrollment, Enrollment.subscription_id == Ledger.subscription_id)
        .join(Transfer, Transfer.subscription_id == Ledger.subscription_id)
        .join(Source, Source.subscription_id == Ledger.subscription_id)
        .join(Binding, Binding.panel_scope == Ledger.panel_scope)
        .where(
            Subscription.traffic_accounting_mode == 'split_pending',
            Subscription.user_id == Enrollment.user_id,
            Subscription.remnawave_id == Ledger.panel_user_id,
            Enrollment.state == 'transferred',
            Enrollment.ownership_cycle == Ledger.ownership_cycle,
            Enrollment.panel_scope == Ledger.panel_scope,
            Enrollment.account_generation == Ledger.account_generation,
            Transfer.ownership_cycle == Ledger.ownership_cycle,
            Transfer.account_generation == Ledger.account_generation,
            Transfer.panel_scope == Ledger.panel_scope,
            Source.phase == 'transferred',
            Source.panel_identity == Transfer.source_identity,
            Binding.retired_at.is_(None),
        )
    )
    return union_all(active.add_columns(Ledger.panel_scope), pending.add_columns(Ledger.panel_scope)).subquery()


async def validate_pending_member(db, reader, item):
    from app.services.bs_traffic.enrollment_source import load_enrollment
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.managed_cutover import _pending_snapshot

    enrollment, _ = await load_enrollment(db, reader, item.enrollment_id)
    if enrollment.state != 'transferred':
        raise LedgerConflict('Collection enrollment changed')
    await _pending_snapshot(
        db,
        reader,
        user_id=item.user_id,
        subscription_id=item.subscription_id,
        account_generation=item.account_generation,
    )
