"""Shared immutable usage identity and FIFO allocation (no database or auth)."""

import hashlib
import json
from datetime import UTC


def usage_request_hash(group, evidence_kind, generation, amount, start, end):
    return hashlib.sha256(
        json.dumps(
            [
                group,
                evidence_kind,
                generation,
                amount,
                start.astimezone(UTC).isoformat(),
                end.astimezone(UTC).isoformat(),
            ],
            separators=(',', ':'),
        ).encode()
    ).hexdigest()


def fifo_allocations(ordered_balances, amount):
    """Input is already ordered by original expires_at then grant id."""
    pending, allocations = amount, []
    for identifier, remaining in ordered_balances:
        charged = min(pending, remaining)
        if charged:
            pending -= charged
            allocations.append({'grant_id': identifier, 'bytes': charged})
    return allocations, pending
