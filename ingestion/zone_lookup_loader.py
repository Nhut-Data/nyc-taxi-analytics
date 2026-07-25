# ingestion/zone_lookup_loader.py
"""
Tải zone lookup CSV (static, 265 rows) lên GCS.
Chỉ cần chạy 1 lần — file này không thay đổi theo tháng.
"""

import logging
import requests
from google.cloud import storage
from ingestion.url_builder import ZONE_LOOKUP_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ZONE_LOOKUP_GCS_PATH = "reference/taxi_zone_lookup.csv"  # path cố định trong bucket


def upload_zone_lookup(bucket: str, skip_if_exists: bool = True) -> str:
    """
    Tải zone lookup CSV từ TLC CloudFront lên GCS.

    Args:
        bucket: tên GCS bucket
        skip_if_exists: nếu True, bỏ qua khi file đã có

    Returns:
        GCS path của file đã upload
    """
    gcs_path = f"gs://{bucket}/{ZONE_LOOKUP_GCS_PATH}"
    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(ZONE_LOOKUP_GCS_PATH)

    if skip_if_exists and blob.exists():
        logger.info(f"SKIP (already exists): {gcs_path}")
        return gcs_path

    logger.info(f"Downloading zone lookup from {ZONE_LOOKUP_URL}")
    response = requests.get(ZONE_LOOKUP_URL, timeout=30)
    response.raise_for_status()

    blob.upload_from_string(response.content, content_type="text/csv")
    logger.info(f"DONE: {gcs_path}")
    return gcs_path


if __name__ == "__main__":
    result = upload_zone_lookup(bucket="nyc-taxi-raw-banking-500108")
    print(f"Uploaded: {result}")