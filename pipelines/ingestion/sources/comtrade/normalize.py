"""Normalize raw Comtrade records into a PyArrow table."""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa

from pipelines.ingestion.common.schemas import COMTRADE_SCHEMA

logger = logging.getLogger(__name__)


def normalize(records: list[dict[str, Any]]) -> pa.Table:
    """Convert raw Comtrade dicts to a PyArrow Table matching COMTRADE_SCHEMA."""
    rows: list[dict[str, Any]] = []
    for r in records:
        flow_code = r.get("flowCode", r.get("rgCode", ""))
        flow = "import" if str(flow_code) in ("1", "M") else "export"

        rows.append(
            {
                "period": _int(
                    r.get("period", r.get("yr", 0)) if "period" in r else f"{r.get('yr', '')}{r.get('month', ''):>02}"
                ),
                "reporter_iso3": r.get("reporterISO", r.get("rtCode", "")),
                "partner_iso3": r.get("partnerISO", r.get("ptCode", "")),
                "hs_code": str(r.get("cmdCode", "")),
                "trade_flow": flow,
                "trade_value_usd": _float(r.get("primaryValue", r.get("TradeValue"))),
                "net_weight_kg": _float(r.get("netWgt", r.get("NetWeight"))),
                "quantity": _float(r.get("qty", r.get("Qty"))),
                "quantity_unit": r.get("qtyUnitAbbr", r.get("qtAltDesc", "")),
            }
        )

    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in COMTRADE_SCHEMA})

    return pa.Table.from_pylist(rows, schema=COMTRADE_SCHEMA)


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
