"""Existing server authorization protects aggregate split diagnostics."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import SQLAlchemyError

from app.cabinet.dependencies import get_cabinet_db, get_current_cabinet_user
from app.cabinet.routes import admin_split_diagnostics as route
from app.services.permission_service import PermissionService


def test_route_registered_and_get_only(registered_paths):
    assert registered_paths['/cabinet/admin/remnawave/split-diagnostics'] == {'GET'}


@pytest.mark.parametrize('error', [TimeoutError(), SQLAlchemyError('masked-private-error')])
async def test_database_failure_returns_safe_retryable_error(monkeypatch, error):
    monkeypatch.setattr(route, 'read_diagnostics', AsyncMock(side_effect=error))
    with pytest.raises(HTTPException) as caught:
        await route.get_split_diagnostics(admin=SimpleNamespace())
    assert caught.value.status_code == 503
    assert caught.value.detail == {'code': 'split_diagnostics_unavailable'}


async def test_anonymous_request_cannot_reach_diagnostics(monkeypatch):
    reader = AsyncMock()
    monkeypatch.setattr(route, 'read_diagnostics', reader)
    app = FastAPI()
    app.include_router(route.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test.example') as client:
        response = await client.get('/admin/remnawave/split-diagnostics')
    assert response.status_code == 401
    reader.assert_not_awaited()


async def test_authenticated_user_without_remnawave_permission_cannot_read_snapshot(monkeypatch):
    reader = AsyncMock()
    check = AsyncMock(return_value=(False, 'denied'))
    monkeypatch.setattr(route, 'read_diagnostics', reader)
    monkeypatch.setattr(PermissionService, 'check_permission', check)
    monkeypatch.setattr(PermissionService, 'log_action', AsyncMock())
    app = FastAPI()
    app.include_router(route.router)
    app.dependency_overrides[get_current_cabinet_user] = lambda: SimpleNamespace(id=42)
    app.dependency_overrides[get_cabinet_db] = lambda: SimpleNamespace(commit=AsyncMock())
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test.example') as client:
        response = await client.get('/admin/remnawave/split-diagnostics')
    assert response.status_code == 403
    assert check.await_args.args[2] == 'remnawave:read'
    reader.assert_not_awaited()
