"""Real PostgreSQL coverage of the three daily resume money boundaries."""

import pytest

from tests.services import test_managed_daily_resume as checks


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('endpoint', ['cabinet', 'miniapp', 'telegram'])
@pytest.mark.parametrize('failure', [False, True])
@pytest.mark.parametrize('reset', [False, True])
async def test_managed_daily_resume_postgres(postgres_database, monkeypatch, endpoint, failure, reset):
    await checks.test_managed_daily_resume_atomic(postgres_database, monkeypatch, endpoint, failure, reset)
