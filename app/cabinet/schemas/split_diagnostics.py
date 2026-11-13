"""Aggregate operator diagnostics; no panel, account, or customer identifiers."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


DiagnosticWarning = Literal[
    'no_fresh_round',
    'round_errors',
    'panel_backoff',
    'collection_pending',
    'collection_attention',
    'settlement_pending',
    'settlement_errors',
    'delivery_pending',
    'delivery_attention',
    'renewal_pending',
    'enrollment_pending',
    'reverse_pending',
    'node_tail_pending',
    'node_tail_attention',
    'node_tail_delayed',
    'topology_attention',
    'connection_drop_pending',
    'connection_drop_unknown',
]


class SplitDiagnosticQueues(BaseModel):
    collection_pending: int = 0
    collection_attention: int = 0
    settlement_pending: int = 0
    settlement_errors: int = 0
    delivery_pending: int = 0
    delivery_attention: int = 0
    renewal_pending: int = 0
    enrollment_pending: int = 0
    reverse_pending: int = 0
    node_tail_pending: int = 0
    node_tail_attention: int = 0
    node_tail_delayed: int = 0
    topology_attention: int = 0
    connection_drop_pending: int = 0
    connection_drop_unknown: int = 0


class SplitDiagnosticScope(BaseModel):
    number: int
    collection_phase: Literal['building', 'collecting', 'settling', 'complete', 'attention'] | None
    last_success_at: datetime | None
    pause_until: datetime | None
    diagnostic_ready: bool
    local_runtime_ready: bool | None
    queues: SplitDiagnosticQueues
    oldest_pending_at: datetime | None
    warnings: list[DiagnosticWarning] = Field(default_factory=list)


class SplitDiagnosticsResponse(BaseModel):
    checked_at: datetime
    freshness_seconds: int
    local_runtime_running: bool | None
    force_disconnect_enabled: bool
    active_bindings: int
    bindings_truncated: bool
    unbound_enrollments: int
    scopes: list[SplitDiagnosticScope]
