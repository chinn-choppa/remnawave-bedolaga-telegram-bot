import pytest

from app.external.remnawave_api import RemnaWaveAPIError, RemnaWaveTransientError
from app.services.bs_traffic.recovery_backoff import backoff_for


@pytest.mark.parametrize('value', [0, 2.5, 300, 1e100, pytest.param(10**1000, id='huge-int')])
def test_provider_delay_is_preserved_without_exposing_body(value):
    error = RemnaWaveAPIError('private response', status_code=429)
    error.retry_after_seconds = value
    backoff = backoff_for(error)
    assert backoff.retry_after_seconds == value
    assert 'private' not in str(backoff)


@pytest.mark.parametrize('value', [None, True, -1, float('nan'), float('inf'), '120'])
def test_invalid_provider_metadata_uses_transient_fallback(value):
    error = RemnaWaveAPIError('private response', status_code=429)
    error.retry_after_seconds = value
    assert backoff_for(error).retry_after_seconds == 60


def test_transient_without_status_has_fallback():
    assert backoff_for(RemnaWaveTransientError('unavailable')).retry_after_seconds == 60


def test_nontransient_failure_does_not_invent_provider_pause():
    assert backoff_for(RemnaWaveAPIError('bad request', status_code=400)) is None
