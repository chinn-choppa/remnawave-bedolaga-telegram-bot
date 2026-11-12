"""A funded enrollment blocks the old traffic checkout before split activation."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from tests.database.test_bs_enrollment_postgres import TABLES, paid
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_pending_enrollment_blocks_both_old_traffic_surfaces(postgres_database, monkeypatch):
    from app.cabinet.routes.subscription_modules.traffic import _require_single_traffic_accounting
    from app.handlers.subscription import common, traffic
    from app.services.bs_traffic.enrollment import stage_paid_enrollment

    async with postgres_session(postgres_database, TABLES) as db:
        user, _, sub, tx = await paid(db, monkeypatch)
        await stage_paid_enrollment(db, sub, transaction=tx)
        await db.commit()
        assert sub.traffic_accounting_mode == 'legacy'
        with pytest.raises(HTTPException) as error:
            await _require_single_traffic_accounting(db, sub)
        assert error.value.status_code == 409
        monkeypatch.setattr(common, 'resolve_subscription_from_context', AsyncMock(return_value=(sub, sub.id)))
        callback = SimpleNamespace(
            data=f'st:{sub.id}', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await traffic.handle_add_traffic(callback, user, db)
        callback.message.edit_text.assert_not_awaited()
        assert callback.answer.call_args.kwargs['show_alert'] is True
        assert user.balance_kopeks == 8000
