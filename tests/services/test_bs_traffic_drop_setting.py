import json
from pathlib import Path

import pytest

from app.cabinet.routes.admin_settings import _serialize_definition
from app.config import Settings
from app.services.system_settings_service import bot_configuration_service as service


KEY = 'BS_TRAFFIC_FORCE_DISCONNECT_ENABLED'


def test_disconnect_is_opt_in_and_in_traffic_category():
    assert Settings.model_fields[KEY].default is False
    definition = service.get_definition(KEY)
    assert definition.python_type is bool
    assert definition.category_key == 'TRAFFIC'
    assert service.parse_user_value(KEY, 'false') is False
    assert service.parse_user_value(KEY, 'true') is True


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'zh', 'fa'])
def test_drop_risk_copy_reaches_bot_and_cabinet_in_every_language(language):
    locale = json.loads((Path('app/localization/locales') / f'{language}.json').read_text())
    guidance = service.get_setting_guidance(KEY, language=language)
    for field in ('description', 'warning'):
        assert guidance[field] == locale[f'SETTINGS_HINT_{KEY}_{field.upper()}']
        assert 'IP' in guidance[field]
    serialized = _serialize_definition(service.get_definition(KEY), language=language)
    assert serialized.hint.description == guidance['description']
    assert serialized.hint.warning == guidance['warning']


async def test_setting_persists_and_applies_through_existing_settings_store(monkeypatch):
    from sqlalchemy import select

    from app.config import settings
    from app.database.models import SystemSetting
    from tests.services.test_traffic_accounting_store import database

    monkeypatch.setattr(settings, KEY, False)
    monkeypatch.setattr(service, '_overrides_raw', dict(service._overrides_raw))
    monkeypatch.setattr(service, '_env_override_keys', set(service._env_override_keys) - {KEY})
    monkeypatch.setattr(service, '_original_values', {**service._original_values, KEY: False})
    async with database(monkeypatch) as db:
        await service.set_value(db, KEY, True)
        assert getattr(settings, KEY) is True
        assert (await db.scalar(select(SystemSetting).where(SystemSetting.key == KEY))).value == 'true'
        await service.reset_value(db, KEY)
        assert getattr(settings, KEY) is False
        assert await db.scalar(select(SystemSetting).where(SystemSetting.key == KEY)) is None
