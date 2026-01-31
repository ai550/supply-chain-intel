"""Extract weather data from NOAA Climate Data Online (CDO) API.

Docs: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from pipelines.ingestion.common.http import get_json, rate_limit_wait

logger = logging.getLogger(__name__)

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

# Major port-adjacent weather stations (add more as needed)
DEFAULT_STATION_IDS = [
    "GHCND:USW00023174",  # Los Angeles Intl Airport
    "GHCND:USW00023129",  # Long Beach
    "GHCND:USW00094728",  # New York Central Park
    "GHCND:USW00012839",  # Houston Hobby
]


def fetch_daily(
    target_date: date,
    station_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch daily weather observations from NOAA CDO for given stations.

    Returns raw observation dicts.
    """
    token = os.environ.get("NOAA_TOKEN", "")
    if not token:
        logger.warning("NOAA_TOKEN not set; skipping NOAA extraction")
        return []

    station_ids = station_ids or DEFAULT_STATION_IDS
    headers = {"token": token}
    records: list[dict[str, Any]] = []

    for station in station_ids:
        params = {
            "datasetid": "GHCND",
            "stationid": station,
            "startdate": target_date.isoformat(),
            "enddate": target_date.isoformat(),
            "datatypeid": "TAVG,PRCP,AWND",
            "units": "metric",
            "limit": 100,
        }
        try:
            data = get_json(BASE_URL, params=params, headers=headers)
            results = data.get("results", [])
            for r in results:
                r["_station_id"] = station
            records.extend(results)
        except Exception:
            logger.exception("NOAA fetch failed for station=%s date=%s", station, target_date)
        rate_limit_wait(0.3)

    logger.info("Extracted %d NOAA records for %s", len(records), target_date)
    return records
