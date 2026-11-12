"""Stable accounting identity, independent of subscription-link credentials.

The caller must verify ownership using a fresh panel account. This fingerprint
does not authorize access. Panel scope is a persisted operator/panel epoch, not
a URL or API token; a restored/replaced panel database needs a new epoch before
enrollment. Identical restored rows cannot be distinguished through these fields.
"""

import hashlib
import json
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID


class PanelAccount(Protocol):
    id: int
    created_at: datetime


def account_generation(*, panel_scope: str, account: PanelAccount) -> str:
    if not isinstance(panel_scope, str):
        raise ValueError('Explicit panel epoch is required')
    try:
        scope = str(UUID(panel_scope))
    except ValueError:
        raise ValueError('Explicit panel epoch is required') from None
    if type(account.id) is not int or not 0 < account.id < 2**53:
        raise ValueError('Exact positive panel account ID is required')
    created = account.created_at
    if not isinstance(created, datetime) or created.tzinfo is None or created.utcoffset() is None:
        raise ValueError('Known timezone-aware account creation time is required')
    fields = ['bs-account-v1', scope, account.id, created.astimezone(UTC).isoformat(timespec='microseconds')]
    return hashlib.sha256(json.dumps(fields, separators=(',', ':')).encode()).hexdigest()
