"""Write PyArrow tables to MinIO as Parquet files."""

from __future__ import annotations

import io
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.ingestion.common.s3 import key_exists, upload_parquet_buffer

logger = logging.getLogger(__name__)


def write_parquet(
    table: pa.Table,
    bucket: str | None = None,
    key: str = "",
    skip_if_exists: bool = True,
) -> str:
    """Write a PyArrow table to MinIO as a Parquet file.

    Args:
        table: The data to write.
        bucket: S3 bucket (defaults to LAKE_BUCKET env var).
        key: Full S3 object key (e.g. raw/gdelt/dt=2025-01-15/data.parquet).
        skip_if_exists: If True, skip upload when key already exists.

    Returns:
        The S3 key written.
    """
    bucket = bucket or os.environ.get("LAKE_BUCKET", "lake")

    if skip_if_exists and key_exists(bucket, key):
        logger.info("Key already exists, skipping: s3://%s/%s", bucket, key)
        return key

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    upload_parquet_buffer(bucket, key, buf)

    logger.info("Wrote %d rows to s3://%s/%s", table.num_rows, bucket, key)
    return key
