"""Normalize raw NOAA CDO records into a PyArrow table."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pyarrow as pa

from pipelines.ingestion.common.schemas import NOAA_SCHEMA

logger = logging.getLogger(__name__)


def normalize(records: list[dict[str, Any]], target_date: date) -> pa.Table:
    """Pivot NOAA CDO observations into one row per (date, station) and return PyArrow Table."""
    # Group by station
    by_station: dict[str, dict[str, Any]] = {}
    for r in records:
        station = r.get("_station_id", r.get("station", ""))
        if station not in by_station:
            by_station[station] = {
                "date": target_date,
                "station_id": station,
                "station_name": r.get("station_name", station),
                "temp_avg_c": None,
                "precipitation_mm": None,
                "wind_avg_mps": None,
                "storm_flag": False,
            }
        datatype = r.get("datatype", "")
        value = _float(r.get("value"))
        if datatype == "TAVG" and value is not None:
            by_station[station]["temp_avg_c"] = value
        elif datatype == "PRCP" and value is not None:
            by_station[station]["precipitation_mm"] = value
        elif datatype == "AWND" and value is not None:
            by_station[station]["wind_avg_mps"] = value

    # Mark storm flag heuristic: high wind or heavy precipitation
    for s in by_station.values():
        wind = s.get("wind_avg_mps") or 0
        precip = s.get("precipitation_mm") or 0
        s["storm_flag"] = wind > 15.0 or precip > 50.0

    rows = list(by_station.values())

    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in NOAA_SCHEMA})

    return pa.Table.from_pylist(rows, schema=NOAA_SCHEMA)


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
