from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import Tariff
from app.external.remnawave_api import RemnaWaveAPIError
from tests.services.test_traffic_accounting_store import database


@pytest.mark.parametrize('outcome', ['missing', 'live', 'timeout', 'forbidden', 'concurrent'])
async def test_reconciliation_only_clears_confirmed_unchanged_tariff_mapping(monkeypatch, outcome):
    from app.services.external_squad_reconciliation import reconcile_deleted_external_squads

    async with database(monkeypatch) as db:
        db.add(Tariff(id=1, name='Example tariff', external_squad_uuid='old', period_prices={}))
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def lookup(uuid):
            if outcome == 'timeout':
                raise TimeoutError
            if outcome == 'forbidden':
                raise RemnaWaveAPIError('Forbidden', 403)
            if outcome == 'live':
                return SimpleNamespace(uuid=uuid)
            if outcome == 'concurrent':
                async with factory() as other:
                    tariff = await other.get(Tariff, 1)
                    tariff.external_squad_uuid = 'new'
                    await other.commit()
            return None

        api = SimpleNamespace(get_external_squads=AsyncMock(return_value=[]), get_external_squad_by_uuid=lookup)
        notify = AsyncMock(return_value=True)
        monkeypatch.setattr(
            'app.services.admin_notification_service.AdminNotificationService.send_deleted_external_squad_notification',
            notify,
        )
        changes = await reconcile_deleted_external_squads(api, factory, bot=object())
        db.expire_all()
        tariff = await db.get(Tariff, 1)
        assert tariff.external_squad_uuid == (
            None if outcome == 'missing' else 'new' if outcome == 'concurrent' else 'old'
        )
        assert bool(changes) is (outcome == 'missing')
        assert notify.await_count == (1 if outcome == 'missing' else 0)
        if outcome == 'missing':
            assert await reconcile_deleted_external_squads(api, factory, bot=object()) == []
            assert notify.await_count == 1


@pytest.mark.parametrize('catalog', [None, {}, [SimpleNamespace(uuid=None)], [SimpleNamespace(uuid='')]])
async def test_invalid_catalog_never_clears_tariffs(monkeypatch, catalog):
    from app.services.external_squad_reconciliation import reconcile_deleted_external_squads

    async with database(monkeypatch) as db:
        db.add(Tariff(id=1, name='Example tariff', external_squad_uuid='old', period_prices={}))
        await db.commit()
        api = SimpleNamespace(
            get_external_squads=AsyncMock(return_value=catalog), get_external_squad_by_uuid=AsyncMock(return_value=None)
        )
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        assert await reconcile_deleted_external_squads(api, factory) == []
        db.expire_all()
        assert (await db.get(Tariff, 1)).external_squad_uuid == 'old'
        api.get_external_squad_by_uuid.assert_not_awaited()


@pytest.mark.parametrize('language', ['ru', 'en', 'ua', 'zh', 'fa'])
async def test_deleted_squad_notice_is_localized_escaped_and_bounded(monkeypatch, language):
    from app.config import settings
    from tests.services.test_admin_notification_localization import _assert_language, _service

    monkeypatch.setattr(settings, 'DEFAULT_LANGUAGE', language)
    service = _service(monkeypatch)
    service._send_message = AsyncMock(return_value=True)
    assert await service.send_deleted_external_squad_notification([(n, '<b>&' * 100) for n in range(23)])
    assert service._send_message.await_count == 3
    for call in service._send_message.await_args_list:
        message = call.args[0]
        assert '<b>' not in message
        assert '&lt;b&gt;' in message
        assert len(message) < 4096
        assert 'ADMIN_DELETED_EXTERNAL_SQUAD' not in message
        _assert_language(message, language)


async def test_monitor_reconciliation_is_throttled_and_isolated(monkeypatch):
    from contextlib import asynccontextmanager

    from app.services import external_squad_reconciliation as module
    from app.services.monitoring_service import MonitoringService

    api = object()

    @asynccontextmanager
    async def client():
        yield api

    monitor = MonitoringService()
    monitor.subscription_service = SimpleNamespace(is_configured=True, get_api_client=client)
    repair = AsyncMock()
    monkeypatch.setattr(module, 'reconcile_deleted_external_squads', repair)
    await monitor._reconcile_external_squads()
    await monitor._reconcile_external_squads()
    assert repair.await_count == 1
    monitor._last_external_squad_check = float('-inf')
    repair.side_effect = TimeoutError()
    await monitor._reconcile_external_squads()
    await monitor._reconcile_external_squads()
    assert repair.await_count == 2


@pytest.mark.parametrize('stage', ['catalog', 'detail'])
async def test_slow_catalog_does_not_block_monitor_or_clear_mapping(monkeypatch, stage):
    import asyncio

    from app.services import external_squad_reconciliation as module

    monkeypatch.setattr(module, 'EXTERNAL_SQUAD_RECONCILIATION_TIMEOUT', 0.01, raising=False)
    async with database(monkeypatch) as db:
        db.add(Tariff(id=1, name='Example', external_squad_uuid='live', period_prices={}))
        await db.commit()

        async def hang(*args):
            await asyncio.Event().wait()

        api = SimpleNamespace(
            get_external_squads=hang if stage == 'catalog' else AsyncMock(return_value=[]),
            get_external_squad_by_uuid=hang,
        )
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        assert await asyncio.wait_for(module.reconcile_deleted_external_squads(api, factory), timeout=0.25) == []
        db.expire_all()
        assert (await db.get(Tariff, 1)).external_squad_uuid == 'live'
