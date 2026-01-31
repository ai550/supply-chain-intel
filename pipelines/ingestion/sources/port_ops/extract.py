"""Extract port-operations data from a public port dataset.

Placeholder: adapt to whichever public port data API you choose
(e.g. Port of Los Angeles Signal data, MarineTraffic, etc.).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from pipelines.ingestion.common.http import get_json, rate_limit_wait

logger = logging.getLogger(__name__)

# Placeholder base URL -- replace with actual port data API
BASE_URL = "https://data.example.com/port-ops"


def fetch_daily(target_date: date, port_ids: list[str] | None = None) -> list[dict[str, Any]]:
    """Fetch raw port-ops records for a given date.

    Returns a list of dicts (one per port-day record).
    """
    port_ids = port_ids or ["POLA", "POLB"]
    records: list[dict[str, Any]] = []

    for port_id in port_ids:
        try:
            data = get_json(
                BASE_URL,
                params={"port": port_id, "date": target_date.isoformat()},
            )
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                records.append(data)
        except Exception:
            logger.exception("Failed to fetch port_ops for %s on %s", port_id, target_date)
        rate_limit_wait(0.5)

    logger.info("Extracted %d port_ops records for %s", len(records), target_date)
    return records
