#!/usr/bin/env bash
set -euo pipefail

MINIO_ALIAS=${MINIO_ALIAS:-local}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-minio}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minio12345}

mc alias set "$MINIO_ALIAS" "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

mc mb -p "${MINIO_ALIAS}/lake" || true
mc mb -p "${MINIO_ALIAS}/logs" || true

mc anonymous set download "${MINIO_ALIAS}/lake" || true

echo "MinIO buckets initialised."
