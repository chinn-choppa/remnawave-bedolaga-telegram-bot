import subprocess
import sys

import pytest

from app.cabinet.services.email_templates import EmailNotificationTemplates
from app.localization.texts import get_texts
from app.services.bs_traffic.enrollment_customer import pending_message
from app.services.notification_types import NotificationType


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'zh', 'fa'])
def test_pending_purchase_template_describes_delivery_without_active_claim(language):
    texts = get_texts(language)
    template = EmailNotificationTemplates().get_template(NotificationType.SUBSCRIPTION_PENDING, language, {})
    assert template['subject'] == texts.t('SPLIT_ENROLLMENT_PENDING_TITLE')
    assert pending_message(language) in template['body_html']
    assert 'SPLIT_ENROLLMENT' not in template['body_html']


def test_payment_and_pending_notification_imports_in_fresh_interpreter():
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            'import app.services.payment.cashera; import app.services.cashera_recurring_cancel; import app.services.bs_traffic.enrollment_customer',
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
