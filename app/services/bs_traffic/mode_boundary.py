"""Persisted finite-mode cutoffs exclude earlier unlimited observations."""

from datetime import UTC, datetime

from app.services.bs_traffic.ledger import LedgerConflict


def finite_since(policy, group):
    values = (policy or {}).get('finite_since', {})
    if not isinstance(values, dict):
        raise LedgerConflict('Invalid finite mode boundary')
    value = values.get(group)
    if value is None:
        return None
    try:
        boundary = datetime.fromisoformat(value)
        if boundary.tzinfo is None:
            raise ValueError
        return boundary.astimezone(UTC)
    except (TypeError, ValueError):
        raise LedgerConflict('Invalid finite mode boundary') from None
