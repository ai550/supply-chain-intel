"""S3 / MinIO helper utilities."""

from __future__ import annotations

import io
import logging
import os

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


def _client():
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minio")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minio12345")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_bytes(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    client = _client()
    logger.info("Uploading s3://%s/%s (%d bytes)", bucket, key, len(data))
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def upload_parquet_buffer(bucket: str, key: str, buf: io.BytesIO) -> None:
    buf.seek(0)
    upload_bytes(bucket, key, buf.read(), content_type="application/octet-stream")


def list_keys(bucket: str, prefix: str) -> list[str]:
    client = _client()
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def key_exists(bucket: str, key: str) -> bool:
    client = _client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False
