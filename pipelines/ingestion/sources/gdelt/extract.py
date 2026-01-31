"""Extract GDELT event data.

Uses the GDELT Analysis Service (GKG / Events) or the raw GDELT daily export files.
Docs: https://www.gdeltproject.org/data.html
"""

from __future__ import annotations

import io
import logging
from datetime import date
from typing import Any

import pandas as pd

from pipelines.ingestion.common.http import get_csv_stream, rate_limit_wait

logger = logging.getLogger(__name__)

# GDELT v2 daily exports (events table)
GDELT_EXPORT_URL = "http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"

# Supply-chain relevant CAMEO root codes
SUPPLY_CHAIN_CODES = {
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
}


def fetch_daily(target_date: date) -> list[dict[str, Any]]:
    """Fetch and aggregate GDELT events for a given date.

    Returns aggregated records: one per (country_iso3, event_code) with counts and avg_tone.
    """
    date_str = target_date.strftime("%Y%m%d")
    url = GDELT_EXPORT_URL.format(date_str=date_str)

    try:
        resp = get_csv_stream(url, timeout=120)
        df = pd.read_csv(
            io.BytesIO(resp.content),
            sep="\t",
            header=None,
            compression="zip",
            low_memory=False,
        )
    except Exception:
        logger.exception("Failed to fetch GDELT for %s", target_date)
        return []

    rate_limit_wait(0.5)

    # GDELT columns: col 26 = EventRootCode, col 53 = ActionGeo_CountryCode, col 34 = AvgTone
    if df.shape[1] < 55:
        logger.warning("Unexpected GDELT column count: %d", df.shape[1])
        return []

    df = df.rename(columns={26: "event_root_code", 53: "country_code", 34: "avg_tone"})
    df = df[["event_root_code", "country_code", "avg_tone"]].copy()
    df["event_root_code"] = df["event_root_code"].astype(str).str.zfill(2)

    agg = (
        df.groupby(["country_code", "event_root_code"])
        .agg(event_count=("event_root_code", "size"), avg_tone=("avg_tone", "mean"))
        .reset_index()
    )

    records: list[dict[str, Any]] = []
    for _, row in agg.iterrows():
        records.append(
            {
                "date": target_date.isoformat(),
                "country_iso3": row["country_code"] if pd.notna(row["country_code"]) else None,
                "event_code": row["event_root_code"],
                "event_count": int(row["event_count"]),
                "avg_tone": float(row["avg_tone"]) if pd.notna(row["avg_tone"]) else None,
            }
        )

    logger.info("Extracted %d GDELT aggregated rows for %s", len(records), target_date)
    return records
