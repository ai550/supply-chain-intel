"""Normalize raw Comtrade v1 API records into a PyArrow table."""

from __future__ import annotations

import logging
import math
from typing import Any

import pyarrow as pa

from pipelines.ingestion.common.schemas import COMTRADE_SCHEMA

logger = logging.getLogger(__name__)


def normalize(records: list[dict[str, Any]]) -> pa.Table:
    """Convert raw Comtrade v1 dicts to a PyArrow Table matching COMTRADE_SCHEMA.

    Expected v1 fields: refYear, refMonth, reporterISO, partnerISO, cmdCode,
    flowCode, primaryValue, netWgt, qty, qtyUnitAbbr.
    """
    rows: list[dict[str, Any]] = []
    for r in records:
        # Build YYYYMM period from refYear + refMonth, falling back to period string
        period = _build_period(r)
        if period is None:
            continue

        flow_code = str(r.get("flowCode", ""))
        flow = "import" if flow_code == "M" else "export"

        rows.append(
            {
                "period": period,
                "reporter_iso3": _str(r.get("reporterISO")),
                "partner_iso3": _str(r.get("partnerISO")),
                "hs_code": str(r.get("cmdCode", "")),
                "trade_flow": flow,
                "trade_value_usd": _float(r.get("primaryValue")),
                "net_weight_kg": _float(r.get("netWgt")),
                "quantity": _float(r.get("qty")),
                "quantity_unit": _str(r.get("qtyUnitAbbr")) or "",
            }
        )

    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in COMTRADE_SCHEMA})

    return pa.Table.from_pylist(rows, schema=COMTRADE_SCHEMA)


def _build_period(r: dict[str, Any]) -> int | None:
    """Build YYYYMM int from record fields."""
    ref_year = r.get("refYear")
    ref_month = r.get("refMonth")
    if ref_year is not None and ref_month is not None:
        try:
            return int(ref_year) * 100 + int(ref_month)
        except (TypeError, ValueError):
            pass
    # Fallback: period field may already be YYYYMM string/int
    period = r.get("period")
    if period is not None:
        try:
            return int(period)
        except (TypeError, ValueError):
            pass
    return None


def _str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return str(v)


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        val = float(v)
        if math.isnan(val):
            return None
        return val
    except (TypeError, ValueError):
        return None
