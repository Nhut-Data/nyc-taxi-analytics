# ingestion/download_to_gcs.py
"""
Tải file Parquet từ TLC CloudFront và upload lên GCS.

Flow:
1. HEAD request kiểm tra file tồn tại (tránh download 404)
2. Download stream trực tiếp lên GCS (không lưu local — tiết kiệm disk)
3. Skip nếu file đã tồn tại trên GCS (idempotent)
"""

import logging
import requests
from google.cloud import storage
from ingestion.url_builder import build_trip_url, build_gcs_path, ZONE_LOOKUP_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
UPLOAD_TIMEOUT = 600  # 10 phút — tăng từ 120s mặc định

def _parse_gcs_path(gcs_path: str) -> tuple[str, str]:
    """Parse 'gs://bucket/path/to/file' → (bucket, blob_path)"""
    assert gcs_path.startswith("gs://"), f"Invalid GCS path: {gcs_path}"
    parts = gcs_path[5:].split("/", 1)
    return parts[0], parts[1]


def blob_exists(client: storage.Client, gcs_path: str) -> bool:
    """Check file đã tồn tại trên GCS chưa."""
    bucket_name, blob_name = _parse_gcs_path(gcs_path)
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_name).exists()


def download_to_gcs(
    year: int,
    month: int,
    service_type: str,
    bucket: str,
    skip_if_exists: bool = True,
) -> str:
    """
    Download 1 file trip data từ TLC CloudFront lên GCS.

    Args:
        year, month, service_type: xác định file cần tải
        bucket: tên GCS bucket (không có gs://)
        skip_if_exists: nếu True, bỏ qua khi file đã có trên GCS

    Returns:
        GCS path của file đã upload

    Raises:
        FileNotFoundError: nếu URL trả về 404
        requests.HTTPError: nếu HTTP error khác
    """
    url = build_trip_url(year, month, service_type)
    gcs_path = build_gcs_path(year, month, service_type, bucket)
    bucket_name, blob_name = _parse_gcs_path(gcs_path)

    client = storage.Client()

    # Skip nếu đã tồn tại
    if skip_if_exists and blob_exists(client, gcs_path):
        logger.info(f"SKIP (already exists): {gcs_path}")
        return gcs_path

    # HEAD check — tránh download file không tồn tại
    logger.info(f"Checking URL: {url}")
    head = requests.head(url, timeout=30)
    if head.status_code == 404:
        raise FileNotFoundError(f"File not found on TLC CloudFront: {url}")
    head.raise_for_status()

    file_size_mb = int(head.headers.get("Content-Length", 0)) / 1024 / 1024
    logger.info(f"Downloading {file_size_mb:.1f} MB → {gcs_path}")

    # Stream download trực tiếp lên GCS
    bucket_obj = client.bucket(bucket_name)
    blob = bucket_obj.blob(blob_name)

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with blob.open("wb", timeout=UPLOAD_TIMEOUT) as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)

    logger.info(f"DONE: {gcs_path}")
    return gcs_path


if __name__ == "__main__":
    result = download_to_gcs(
        year=2024,
        month=1,
        service_type="yellow",
        bucket="nyc-taxi-raw-banking-500108",
    )
    print(f"Uploaded: {result}")