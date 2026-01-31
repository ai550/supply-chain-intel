"""Write pipeline run manifests to MinIO for mart_pipeline_health."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

import pyarrow as pa

from pipelines.ingestion.common.schemas import MANIFEST_SCHEMA
from pipelines.ingestion.load.to_minio_parquet import write_parquet

logger = logging.getLogger(__name__)


def write_manifest(
    source_name: str,
    status: str,
    last_partition: str,
    rows_loaded: int,
    error_count: int = 0,
    started_at: datetime | None = None,
) -> str:
    """Write a manifest record for a pipeline run."""
    now = datetime.now(UTC)
    latency = 0.0
    if started_at:
        latency = (now - started_at).total_seconds() / 3600.0

    row = {
        "run_date": date.today(),
        "source_name": source_name,
        "status": status,
        "last_partition_loaded": last_partition,
        "rows_loaded": rows_loaded,
        "error_count": error_count,
        "latency_hours": round(latency, 4),
    }

    table = pa.Table.from_pylist([row], schema=MANIFEST_SCHEMA)
    key = f"raw/manifests/dt={date.today().isoformat()}/{source_name}.parquet"
    return write_parquet(table, key=key, skip_if_exists=False)
