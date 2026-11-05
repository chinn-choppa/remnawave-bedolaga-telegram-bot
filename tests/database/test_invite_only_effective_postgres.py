"""Регистрация Cabinet читает эффективную политику из PostgreSQL, а не снимок процесса."""

import pytest

from app.cabinet.auth.registration_access import evaluate_public_registration
from app.config import settings
from app.database.models import SystemSetting
from app.services import registration_access_service as policy
from app.services.registration_access_service import RegistrationAccessReason, RegistrationChannel
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    ('stored', 'local', 'expected_reason'),
    [
        ('true', False, RegistrationAccessReason.CHANNEL_NOT_ALLOWED),
        ('false', True, RegistrationAccessReason.INVITE_ONLY_DISABLED),
    ],
)
async def test_invite_only_uses_committed_db_value_instead_of_local_snapshot(
    postgres_database, monkeypatch, stored, local, expected_reason
):
    monkeypatch.setattr(settings, 'INVITE_ONLY_ENABLED', local)
    monkeypatch.setattr(policy, 'ENV_OVERRIDE_KEYS', set(policy.ENV_OVERRIDE_KEYS) - {'INVITE_ONLY_ENABLED'})

    async with postgres_session(postgres_database, [SystemSetting.__table__]) as db:
        db.add(SystemSetting(key='INVITE_ONLY_ENABLED', value=stored))
        await db.commit()

        decision = await evaluate_public_registration(db, channel=RegistrationChannel.CABINET_OAUTH)

    assert decision.reason is expected_reason
    assert decision.allowed is (expected_reason is RegistrationAccessReason.INVITE_ONLY_DISABLED)


async def test_invite_only_env_override_wins_over_committed_db_value(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'INVITE_ONLY_ENABLED', True)
    monkeypatch.setattr(policy, 'ENV_OVERRIDE_KEYS', set(policy.ENV_OVERRIDE_KEYS) | {'INVITE_ONLY_ENABLED'})

    async with postgres_session(postgres_database, [SystemSetting.__table__]) as db:
        db.add(SystemSetting(key='INVITE_ONLY_ENABLED', value='false'))
        await db.commit()
        decision = await evaluate_public_registration(db, channel=RegistrationChannel.CABINET_OAUTH)

    assert decision.allowed is False
    assert decision.reason is RegistrationAccessReason.CHANNEL_NOT_ALLOWED


async def test_invite_only_fails_closed_when_reading_policy_fails(monkeypatch):
    monkeypatch.setattr(policy, 'ENV_OVERRIDE_KEYS', set(policy.ENV_OVERRIDE_KEYS) - {'INVITE_ONLY_ENABLED'})

    class BrokenDatabase:
        async def execute(self, _statement):
            raise RuntimeError('database unavailable')

    decision = await evaluate_public_registration(BrokenDatabase(), channel=RegistrationChannel.CABINET_OAUTH)

    assert decision.allowed is False
    assert decision.reason is RegistrationAccessReason.CHECK_UNAVAILABLE
