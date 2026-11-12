"""Sanitized scheduling signal; never retain provider bodies or credentials."""

import math

from app.external.remnawave_api import RemnaWaveAPIError, RemnaWaveTransientError


class RecoveryBackoff(Exception):
    def __init__(self, retry_after_seconds: float):
        super().__init__('Panel recovery requires a pause')
        self.retry_after_seconds = retry_after_seconds


def valid_retry_delay(value: object) -> bool:
    # Integers are finite without converting them to float (which can overflow).
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and (isinstance(value, int) or math.isfinite(value))
        and value >= 0
    )


def backoff_for(error: RemnaWaveAPIError) -> RecoveryBackoff | None:
    delay = getattr(error, 'retry_after_seconds', None)
    if valid_retry_delay(delay):
        return RecoveryBackoff(delay)
    status = getattr(error, 'status_code', None)
    if isinstance(error, RemnaWaveTransientError) or status == 429 or (type(status) is int and 500 <= status <= 599):
        # Local conservative fallback, not a claimed provider-supplied deadline.
        return RecoveryBackoff(60)
    return None
