"""Normalize raw port-ops JSON into a PyArrow table."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pyarrow as pa

from pipelines.ingestion.common.schemas import PORT_OPS_SCHEMA

logger = logging.getLogger(__name__)


def normalize(records: list[dict[str, Any]], target_date: date) -> pa.Table:
    """Convert raw port-ops dicts to a PyArrow Table matching PORT_OPS_SCHEMA."""
    rows: list[dict[str, Any]] = []
    for r in records:
        rows.append(
            {
                "date": target_date,
                "port_name": r.get("port_name", r.get("port", "")),
                "teu_total": _float(r.get("teu_total")),
                "vessel_calls": _int(r.get("vessel_calls")),
                "avg_vessel_wait_hours": _float(r.get("avg_vessel_wait_hours")),
                "terminal_dwell_days": _float(r.get("terminal_dwell_days")),
                "rail_dwell_days": _float(r.get("rail_dwell_days")),
                "truck_turn_time_min": _float(r.get("truck_turn_time_min")),
                "source_record_count": _int(r.get("source_record_count", 1)),
                "source_min_ts": _ts(r.get("source_min_ts")),
                "source_max_ts": _ts(r.get("source_max_ts")),
            }
        )

    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in PORT_OPS_SCHEMA})

    return pa.Table.from_pylist(rows, schema=PORT_OPS_SCHEMA)


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _ts(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except (TypeError, ValueError):
        return None
