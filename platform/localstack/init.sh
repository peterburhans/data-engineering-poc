#!/bin/sh

set -eu

ensure_bucket() {
    bucket_name="$1"
    if awslocal s3api head-bucket --bucket "$bucket_name" >/dev/null 2>&1; then
        return
    fi
    awslocal s3api create-bucket --bucket "$bucket_name" >/dev/null
}

# These buckets are the minimum storage contract needed by the raw backfill CLI.
# Terraform remains authoritative for their configuration and for every streaming,
# security, secret, and bookmark resource.
ensure_bucket "${RAW_BUCKET:-local-smart-meter-raw-s3}"
ensure_bucket "${CURATED_BUCKET:-local-smart-meter-curated-s3}"
