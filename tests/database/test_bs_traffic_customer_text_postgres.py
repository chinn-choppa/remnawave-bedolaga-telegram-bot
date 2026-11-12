import pytest

from app.database.models import Subscription
from app.localization.texts import get_texts
from app.services.bs_traffic.customer_text import subscription_traffic_text
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'fa', 'zh'])
async def test_telegram_uses_persisted_owned_balances_without_writes(postgres_database, monkeypatch, language):
    async with postgres_session(postgres_database, TABLES) as db:
        _, reader, target, _ = await prepared_purchase(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        before = await totals(db)
        texts = get_texts(language)
        result = await subscription_traffic_text(db, user_id=target['user_id'], subscription=sub, texts=texts)
        assert texts.t('SPLIT_TRAFFIC_BS') in result
        assert texts.t('SPLIT_TRAFFIC_ERROR') not in result
        assert await totals(db) == before
        reader._api.get_split_access_snapshot.assert_not_awaited()
        reader._api.update_internal_squads_once.assert_not_awaited()
