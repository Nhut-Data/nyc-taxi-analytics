#!/usr/bin/env bash
# infra/gcs_setup.sh
# Setup ban đầu cho GCS bucket phục vụ pipeline NYC Taxi Analytics.
# Chạy 1 lần khi khởi tạo hạ tầng mới — không phải chạy lại mỗi lần deploy.
set -euo pipefail

PROJECT_ID="banking-data-platform-500108"
REGION="us-central1"
RAW_BUCKET="nyc-taxi-raw-banking-500108"
STAGING_BUCKET="nyc-taxi-dataproc-staging-500108"

echo "=== Tạo bucket raw data ==="
gcloud storage buckets create "gs://${RAW_BUCKET}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --uniform-bucket-level-access

echo "=== Tạo bucket staging cho Dataproc (temp write khi ghi BigQuery) ==="
gcloud storage buckets create "gs://${STAGING_BUCKET}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --uniform-bucket-level-access

echo "=== Cấp quyền ghi cho CI service account (least-privilege, scope đúng bucket) ==="
gcloud storage buckets add-iam-policy-binding "gs://${RAW_BUCKET}" \
  --member="serviceAccount:github-ci-dbt@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

echo "Done. Buckets sẵn sàng cho pipeline."
