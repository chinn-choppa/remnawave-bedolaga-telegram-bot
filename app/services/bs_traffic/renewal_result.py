"""Immutable acknowledgment shared by renewal and initial enrollment receipts."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RenewalResult:
    intent_id: int | None
    transaction_id: int
    grants: dict[str, int]
    old_end_date: datetime
    new_end_date: datetime
    charged_kopeks: int
    replayed: bool = False
    delivery_pending: bool = True
