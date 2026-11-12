import json
from pathlib import Path

import pytest

from app.cabinet.routes.admin_settings import _serialize_definition
from app.config import Settings
from app.services.system_settings_service import bot_configuration_service as service


KEY = 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED'


def test_uncertain_usage_write_off_is_disabled_by_default():
    assert Settings.model_fields[KEY].default is False
    definition = service.get_definition(KEY)
    assert definition.python_type is bool and definition.category_key == 'TRAFFIC'
    assert service.parse_user_value(KEY, 'false') is False
    assert service.parse_user_value(KEY, 'true') is True


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'zh', 'fa'])
def test_risk_description_is_localized_in_bot_and_cabinet_metadata(language):
    locale = json.loads((Path('app/localization/locales') / f'{language}.json').read_text())
    value = _serialize_definition(service.get_definition(KEY), language=language)
    assert value.hint.description == locale[f'SETTINGS_HINT_{KEY}_DESCRIPTION']
    assert value.hint.warning == locale[f'SETTINGS_HINT_{KEY}_WARNING']
