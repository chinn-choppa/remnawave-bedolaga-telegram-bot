"""An old form must not overwrite a tariff changed since it was opened."""

import pytest

from app.database.models import Tariff
from tests.cabinet.test_admin_create_update_schema_parity import _tariff_payload
from tests.cabinet.test_tariff_edit_atomic import _app


pytestmark = pytest.mark.postgres


async def test_stale_form_is_rejected_without_overwriting_newer_settings(monkeypatch, postgres_database):
    async with _app(monkeypatch, postgres_database) as (http, db):
        first = (await http.post('/cabinet/admin/tariffs', json=_tariff_payload())).json()
        version = first['edit_version']
        assert isinstance(version, str) and len(version) == 64
        path = f'/cabinet/admin/tariffs/{first["id"]}'
        saved = await http.put(path, json={'name': 'First editor', 'expected_version': version})
        assert saved.status_code == 200, saved.text
        assert saved.json()['edit_version'] != version
        stale = await http.put(path, json={'name': 'Stale editor', 'expected_version': version})
        assert stale.status_code == 409, stale.text
        assert stale.json()['detail'] == 'tariff_changed'
        assert (await db.get(Tariff, first['id'])).name == 'First editor'


async def test_live_edit_outside_cabinet_also_invalidates_form_version(monkeypatch, postgres_database):
    async with _app(monkeypatch, postgres_database) as (http, db):
        first = (await http.post('/cabinet/admin/tariffs', json=_tariff_payload())).json()
        tariff = await db.get(Tariff, first['id'])
        tariff.allowed_squads = ['changed-outside-cabinet']
        await db.commit()
        response = await http.put(
            f'/cabinet/admin/tariffs/{first["id"]}',
            json={
                'allowed_squads': [],
                'expected_version': first['edit_version'],
            },
        )
        assert response.status_code == 409
        assert tariff.allowed_squads == ['changed-outside-cabinet']


async def test_legacy_clients_without_precondition_remain_compatible(monkeypatch, postgres_database):
    async with _app(monkeypatch, postgres_database) as (http, _db):
        first = (await http.post('/cabinet/admin/tariffs', json=_tariff_payload())).json()
        assert (await http.put(f'/cabinet/admin/tariffs/{first["id"]}', json={'name': 'Legacy'})).status_code == 200


async def test_version_changes_for_group_only_edit_but_not_plain_reads(monkeypatch, postgres_database):
    from app.database.models import PromoGroup

    async with _app(monkeypatch, postgres_database) as (http, db):
        db.add(PromoGroup(id=1, name='Restricted group'))
        await db.commit()
        first = (await http.post('/cabinet/admin/tariffs', json=_tariff_payload())).json()
        path = f'/cabinet/admin/tariffs/{first["id"]}'
        assert (await http.get(path)).json()['edit_version'] == first['edit_version']
        saved = await http.put(path, json={'promo_group_ids': [1], 'expected_version': first['edit_version']})
        assert saved.status_code == 200, saved.text
        assert saved.json()['edit_version'] != first['edit_version']
        stale = await http.put(path, json={'promo_group_ids': [], 'expected_version': first['edit_version']})
        assert stale.status_code == 409
        assert [group.id for group in (await db.get(Tariff, first['id'])).allowed_promo_groups] == [1]
