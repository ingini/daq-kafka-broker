#!/bin/sh
# MinIO 버킷 초기화
set -e

mc alias set local "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"

if mc ls local/"$MINIO_BUCKET" >/dev/null 2>&1; then
    echo "[minio-init] bucket '$MINIO_BUCKET' already exists — skipping"
else
    mc mb local/"$MINIO_BUCKET"
    echo "[minio-init] bucket '$MINIO_BUCKET' created"
fi
