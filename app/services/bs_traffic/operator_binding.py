"""First panel enrollment is part of an explicit operator tariff save.

A changed endpoint or a retired epoch is never adopted automatically. The
operator must reconcile existing ownership before replacing a panel.
"""

from uuid import uuid4

from sqlalchemy import exists, func, select

from app.config import settings as application_settings
from app.database.models import BsTrafficLedger, BsTrafficPanelBinding, Subscription
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import endpoint_fingerprint, register_binding
from app.services.bs_traffic.recovery_scan import _postgres
from app.services.bs_traffic.topology import TopologyConflict


class OperatorSettingsError(LedgerConflict):
    """Closed operator-facing taxonomy; underlying provider text stays private."""

    def __init__(self, code):
        allowed = {
            'bs_topology_invalid',
            'bs_usage_config_invalid',
            'bs_panel_unavailable',
            'bs_binding_mismatch',
            'bs_uncertain_policy_required',
        }
        if code not in allowed:
            raise ValueError('Unknown operator settings error')
        self.code = code
        self.status_code = 503 if code == 'bs_panel_unavailable' else 409 if code == 'bs_binding_mismatch' else 422
        super().__init__(code)


async def ensure_operator_binding(db, *, endpoint):
    _postgres(db)
    digest = endpoint_fingerprint(endpoint)
    # Serialize first registrations across simultaneous saves of different tariffs.
    await db.execute(select(func.pg_advisory_xact_lock(684732190163)))
    rows = list(await db.scalars(select(BsTrafficPanelBinding).with_for_update(read=True)))
    active = [row for row in rows if row.retired_at is None]
    if len(active) == 1 and active[0].endpoint_hash == digest:
        return active[0].panel_scope
    if rows:
        raise OperatorSettingsError('bs_binding_mismatch')
    if await db.scalar(select(exists().select_from(BsTrafficLedger))) or await db.scalar(
        select(exists().where(Subscription.traffic_accounting_mode.in_(('split', 'split_pending'))))
    ):
        raise OperatorSettingsError('bs_binding_mismatch')
    scope = str(uuid4())
    await register_binding(db, panel_scope=scope, endpoint=endpoint)
    return scope


async def prepare_operator_settings(db, *, settings, ordinary_squads):
    """Read-only panel preflight before saving an enabled split configuration."""
    if settings is None or not settings.get('enabled', True):
        return
    if application_settings.BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED is not True:
        raise OperatorSettingsError('bs_uncertain_policy_required')
    from app.services.remnawave_service import RemnaWaveService

    try:
        async with RemnaWaveService().get_api_client() as api:
            # No writes or epoch creation before the panel proves isolation.
            await collect_topology_evidence(
                api,
                panel_identity=endpoint_fingerprint(api.base_url),
                ordinary_squads=ordinary_squads,
                bs_squads=settings['squad_uuids'],
            )
            await ensure_operator_binding(db, endpoint=api.base_url)
    except OperatorSettingsError:
        raise
    except TopologyConflict as error:
        code = (
            'bs_usage_config_invalid'
            if error.code == 'accounting_configuration'
            else 'bs_panel_unavailable'
            if error.code == 'stale_evidence'
            else 'bs_topology_invalid'
        )
        raise OperatorSettingsError(code) from error
    except LedgerConflict as error:
        raise OperatorSettingsError('bs_binding_mismatch') from error
    except Exception as error:
        raise OperatorSettingsError('bs_panel_unavailable') from error
