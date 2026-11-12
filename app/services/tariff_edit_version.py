"""Stateless precondition for saving an already loaded tariff form."""

import hashlib
import json
from datetime import UTC, datetime

from app.database.models import Tariff


def tariff_edit_version(tariff: Tariff) -> str:
    values = {column.key: getattr(tariff, column.key) for column in Tariff.__mapper__.column_attrs}
    values['promo_group_ids'] = sorted(group.id for group in tariff.allowed_promo_groups)
    for key, value in values.items():
        if isinstance(value, datetime):
            aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            values[key] = aware.isoformat()
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
