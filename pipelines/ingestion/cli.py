"""CLI entry-point for data ingestion.

Usage:
    python -m pipelines.ingestion.cli ingest --source port_ops --date 2025-01-15
    python -m pipelines.ingestion.cli ingest --source comtrade --date 2025-01-01
    python -m pipelines.ingestion.cli ingest --source gdelt --date 2025-01-15
    python -m pipelines.ingestion.cli ingest --source noaa --date 2025-01-15
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

from pipelines.ingestion.common.time import month_partition_path, parse_date, partition_path
from pipelines.ingestion.load.manifest import write_manifest
from pipelines.ingestion.load.to_minio_parquet import write_parquet

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def ingest_port_ops(date_str: str) -> None:
    started = datetime.utcnow()
    target = parse_date(date_str)

    from pipelines.ingestion.sources.port_ops.extract import fetch_daily
    from pipelines.ingestion.sources.port_ops.normalize import normalize

    raw = fetch_daily(target)
    table = normalize(raw, target)
    key = partition_path("port_ops", target) + "data.parquet"
    write_parquet(table, key=key)
    write_manifest("port_ops", "ok", date_str, table.num_rows, started_at=started)
    logger.info("port_ops ingestion complete for %s (%d rows)", date_str, table.num_rows)


def ingest_comtrade(date_str: str) -> None:
    started = datetime.utcnow()
    target = parse_date(date_str)

    from pipelines.ingestion.sources.comtrade.extract import fetch_monthly
    from pipelines.ingestion.sources.comtrade.normalize import normalize

    raw = fetch_monthly(target.year, target.month)
    table = normalize(raw)
    key = month_partition_path("comtrade", target) + "data.parquet"
    write_parquet(table, key=key)
    write_manifest("comtrade", "ok", date_str, table.num_rows, started_at=started)
    logger.info("comtrade ingestion complete for %s (%d rows)", date_str, table.num_rows)


def ingest_gdelt(date_str: str) -> None:
    started = datetime.utcnow()
    target = parse_date(date_str)

    from pipelines.ingestion.sources.gdelt.extract import fetch_daily
    from pipelines.ingestion.sources.gdelt.normalize import normalize

    raw = fetch_daily(target)
    table = normalize(raw)
    key = partition_path("gdelt", target) + "data.parquet"
    write_parquet(table, key=key)
    write_manifest("gdelt", "ok", date_str, table.num_rows, started_at=started)
    logger.info("gdelt ingestion complete for %s (%d rows)", date_str, table.num_rows)


def ingest_noaa(date_str: str) -> None:
    started = datetime.utcnow()
    target = parse_date(date_str)

    from pipelines.ingestion.sources.noaa.extract import fetch_daily
    from pipelines.ingestion.sources.noaa.normalize import normalize

    raw = fetch_daily(target)
    table = normalize(raw, target)
    key = partition_path("noaa", target) + "data.parquet"
    write_parquet(table, key=key)
    write_manifest("noaa", "ok", date_str, table.num_rows, started_at=started)
    logger.info("noaa ingestion complete for %s (%d rows)", date_str, table.num_rows)


SOURCE_MAP = {
    "port_ops": ingest_port_ops,
    "comtrade": ingest_comtrade,
    "gdelt": ingest_gdelt,
    "noaa": ingest_noaa,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Supply Chain Intel ingestion CLI")
    sub = parser.add_subparsers(dest="command")

    ingest_parser = sub.add_parser("ingest", help="Run an ingestion job")
    ingest_parser.add_argument("--source", required=True, choices=SOURCE_MAP.keys())
    ingest_parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")

    args = parser.parse_args()
    if args.command == "ingest":
        fn = SOURCE_MAP[args.source]
        fn(args.date)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
