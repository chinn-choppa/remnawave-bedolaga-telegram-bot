import pytest
from sqlalchemy import select

from app.database.models import BsTrafficGraceIntent, GraceAccessSessionModel
from app.external.remnawave_api import RemnaWaveAPIError
from app.services.bs_traffic.grace_completion import complete_restore
from app.services.bs_traffic.grace_sender import send_restore
from tests.database.test_bs_traffic_grace_sender_postgres import TABLES, prepared
from tests.database.test_bs_traffic_policy_postgres import evidence
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
EXTERNAL = '88888888-8888-4888-8888-888888888888'


async def test_existing_external_is_restored_and_checked_again_before_completion(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, remote = await prepared(db, ordinary=True, external=EXTERNAL)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        assert remote['state'].external_squad_uuid == EXTERNAL
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'completed'
        assert api.split_external_squad_exists.await_count == 2
        api.split_external_squad_exists.assert_awaited_with(EXTERNAL)
        api.restore_split_user_once.assert_awaited_once()


async def test_deleted_external_cancels_unsent_without_detaching_silently(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)
        api.split_external_squad_exists.return_value = False
        assert await send_restore(reader, **keys, evidence=evidence()) == 'external_missing'
        api.restore_split_user_once.assert_not_awaited()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'cancelled'
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'


async def test_external_deleted_after_write_does_not_complete_grace(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        api.split_external_squad_exists.return_value = False
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'external_missing'
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'
        api.restore_split_user_once.assert_awaited_once()


async def test_catalog_error_does_not_write_or_cancel_unknown_attempt(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)
        api.split_external_squad_exists.side_effect = RemnaWaveAPIError('synthetic forbidden', 403)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        api.restore_split_user_once.assert_not_awaited()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_retired_binding_before_catalog_read_blocks_write(postgres_database):
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)
        # Retirement before the sender is admitted also demonstrates that the
        # reference is never checked through a different/reused panel epoch.
        async with reader._factory() as worker, worker.begin():
            await retire_binding(worker, panel_scope=reader.panel_scope)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'obsolete'
        api.split_external_squad_exists.assert_not_awaited()
        api.restore_split_user_once.assert_not_awaited()


async def test_null_external_needs_no_catalog_read(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'completed'
        api.split_external_squad_exists.assert_not_awaited()


async def test_retirement_during_completion_catalog_read_discards_result(postgres_database):
    from app.services.bs_traffic.panel_binding import retire_binding

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)
        assert await send_restore(reader, **keys, evidence=evidence()) == 'verified'

        async def retire(_):
            async with reader._factory() as worker, worker.begin():
                await retire_binding(worker, panel_scope=reader.panel_scope)
            return True

        api.split_external_squad_exists.side_effect = retire
        assert await complete_restore(reader, **keys, evidence=evidence()) == 'obsolete'
        assert (await db.scalar(select(GraceAccessSessionModel))).state == 'restoring'


async def test_deletion_between_lookup_and_write_never_retries_or_detaches(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)
        api.restore_split_user_once.side_effect = RemnaWaveAPIError('synthetic missing', 404, {'errorCode': 'A182'})
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        assert await send_restore(reader, **keys, evidence=evidence()) == 'unconfirmed'
        api.restore_split_user_once.assert_awaited_once()
        assert api.restore_split_user_once.call_args.args[0].external_squad_uuid == EXTERNAL
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'attempted'


async def test_tariff_external_change_during_lookup_blocks_stale_assignment(postgres_database):
    from sqlalchemy import update

    from app.database.models import Tariff

    async with postgres_session(postgres_database, TABLES) as db:
        keys, reader, api, _ = await prepared(db, ordinary=True, external=EXTERNAL)

        async def change(_):
            async with reader._factory() as worker, worker.begin():
                await worker.execute(
                    update(Tariff).where(Tariff.external_squad_uuid == EXTERNAL).values(external_squad_uuid=None)
                )
            return True

        api.split_external_squad_exists.side_effect = change
        assert await send_restore(reader, **keys, evidence=evidence()) in ('policy_changed', 'obsolete')
        api.restore_split_user_once.assert_not_awaited()
        assert (await db.get(BsTrafficGraceIntent, keys['operation_id'])).state == 'cancelled'
