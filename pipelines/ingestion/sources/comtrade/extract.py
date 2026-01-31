"""Extract trade data from UN Comtrade API (v1).

Public preview endpoint (no key required, rate-limited):
    https://comtradeapi.un.org/public/v1/preview/C/M/HS?flowCode=M&reporterCode=842&period=202301

Subscription endpoint (requires COMTRADE_KEY):
    https://comtradeapi.un.org/data/v1/get/C/M/HS?flowCode=M&reporterCode=842&period=202301

Docs: https://comtradeapi.un.org/
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pipelines.ingestion.common.http import get_json, rate_limit_wait

logger = logging.getLogger(__name__)

PUBLIC_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"
SUBSCRIPTION_URL = "https://comtradeapi.un.org/data/v1/get/C/M/HS"

# Numeric UN M49 reporter codes (extend as needed)
REPORTER_CODES: dict[str, int] = {
    "USA": 842,
    "CHN": 156,
    "DEU": 276,
    "JPN": 392,
    "KOR": 410,
    "GBR": 826,
    "NLD": 528,
    "SGP": 702,
}


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
        hs_codes: HS commodity codes to filter (default: all)

    Returns:
        List of raw record dicts.
    """
    api_key = os.environ.get("COMTRADE_KEY", "")
    reporter_codes = reporter_codes or ["USA"]
    period = f"{year}{month:02d}"

    # Use subscription endpoint when key is available, otherwise public preview
    if api_key:
        base_url = SUBSCRIPTION_URL
        headers = {"Ocp-Apim-Subscription-Key": api_key}
    else:
        base_url = PUBLIC_URL
        headers = {}

    records: list[dict[str, Any]] = []

    for reporter_iso3 in reporter_codes:
        reporter_num = REPORTER_CODES.get(reporter_iso3)
        if reporter_num is None:
            logger.warning("Unknown reporter code for %s, skipping", reporter_iso3)
            continue

        # Comtrade v1 requires separate calls per flowCode
        for flow_code in ("M", "X"):
            params: dict[str, Any] = {
                "reporterCode": reporter_num,
                "period": period,
                "flowCode": flow_code,
            }
            if hs_codes:
                params["cmdCode"] = ",".join(hs_codes)

            try:
                data = get_json(base_url, params=params, headers=headers)
                dataset = data.get("data", []) if isinstance(data, dict) else data
                if isinstance(dataset, list):
                    records.extend(dataset)
            except Exception:
                logger.exception(
                    "Comtrade fetch failed for reporter=%s flow=%s period=%s",
                    reporter_iso3,
                    flow_code,
                    period,
                )
            rate_limit_wait(1.0)

    logger.info("Extracted %d comtrade records for period=%s", len(records), period)
    return records
