"""Extract trade data from UN Comtrade API.

Uses the Comtrade bulk/preview API (v1 or v2).
Docs: https://comtradeapi.un.org/
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pipelines.ingestion.common.http import get_json, rate_limit_wait

logger = logging.getLogger(__name__)

BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/M"


def fetch_monthly(
    year: int,
    month: int,
    reporter_codes: list[str] | None = None,
    hs_codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch monthly trade records from Comtrade.

    Args:
        year: 4-digit year
        month: 1-12
        reporter_codes: ISO3 country codes (default: ['USA'])
        hs_codes: HS commodity codes to filter (default: broad set)

    Returns:
        List of raw record dicts.
    """
    api_key = os.environ.get("COMTRADE_KEY", "")
    reporter_codes = reporter_codes or ["USA"]
    period = f"{year}{month:02d}"

    records: list[dict[str, Any]] = []
    for reporter in reporter_codes:
        params: dict[str, Any] = {
            "reporterCode": reporter,
            "period": period,
            "flowCode": "M,X",  # imports + exports
        }
        if hs_codes:
            params["cmdCode"] = ",".join(hs_codes)

        headers = {}
        if api_key:
            headers["Ocp-Apim-Subscription-Key"] = api_key

        try:
            data = get_json(BASE_URL, params=params, headers=headers)
            dataset = data.get("data", data) if isinstance(data, dict) else data
            if isinstance(dataset, list):
                records.extend(dataset)
        except Exception:
            logger.exception("Comtrade fetch failed for reporter=%s period=%s", reporter, period)
        rate_limit_wait(1.0)

    logger.info("Extracted %d comtrade records for period=%s", len(records), period)
    return records
