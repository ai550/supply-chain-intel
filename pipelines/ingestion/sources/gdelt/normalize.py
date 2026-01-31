"""Normalize raw GDELT event records into a PyArrow table."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pyarrow as pa

from pipelines.ingestion.common.schemas import GDELT_SCHEMA

logger = logging.getLogger(__name__)


def normalize(records: list[dict[str, Any]]) -> pa.Table:
    """Convert GDELT dicts to a PyArrow Table matching GDELT_SCHEMA."""
    rows: list[dict[str, Any]] = []
    for r in records:
        d = r.get("date")
        if isinstance(d, str):
            d = datetime.fromisoformat(d).date()

        rows.append(
            {
                "date": d,
                "country_iso3": r.get("country_iso3"),
                "event_code": str(r.get("event_code", "")),
                "event_count": int(r.get("event_count", 0)),
                "avg_tone": _float(r.get("avg_tone")),
            }
        )

    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in GDELT_SCHEMA})

    return pa.Table.from_pylist(rows, schema=GDELT_SCHEMA)


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
