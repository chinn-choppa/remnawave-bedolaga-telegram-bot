"""Read-only split diagnostics under existing Remnawave operator RBAC."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from app.cabinet.dependencies import require_permission
from app.cabinet.schemas.split_diagnostics import SplitDiagnosticsResponse
from app.database.database import AsyncSessionLocal
from app.database.models import User
from app.services.bs_traffic.operator_diagnostics import read_diagnostics
from app.services.bs_traffic.runtime import split_traffic_runtime


router = APIRouter(prefix='/admin/remnawave', tags=['Cabinet Admin RemnaWave'])


@router.get('/split-diagnostics', response_model=SplitDiagnosticsResponse)
async def get_split_diagnostics(admin: User = Depends(require_permission('remnawave:read'))):
    try:
        return await read_diagnostics(AsyncSessionLocal, runtime=split_traffic_runtime)
    except (SQLAlchemyError, TimeoutError) as error:
        raise HTTPException(status_code=503, detail={'code': 'split_diagnostics_unavailable'}) from error
