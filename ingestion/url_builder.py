# ingestion/url_builder.py
"""
Build download URLs cho TLC Trip Record Data."""


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

VALID_SERVICE_TYPES = {"yellow", "green", "fhv", "fhvhv"}


def build_trip_url(year: int, month: int, service_type: str) -> str:
    """
    Build URL để tải file Parquet từ TLC CloudFront.

    Args:
        year: năm (VD: 2024)
        month: tháng (1-12)
        service_type: "yellow" | "green" | "fhv" | "fhvhv"

    Returns:
        URL string hoàn chỉnh

    Raises:
        ValueError: nếu input không hợp lệ
    """
    if service_type not in VALID_SERVICE_TYPES:
        raise ValueError(
            f"Invalid service_type '{service_type}'. Must be one of {VALID_SERVICE_TYPES}"
        )

    if not (2009 <= year <= 2030):
        raise ValueError(f"Year {year} out of expected range (2009-2030)")

    if not (1 <= month <= 12):
        raise ValueError(f"Month {month} must be between 1 and 12")

    return f"{BASE_URL}/{service_type}_tripdata_{year}-{month:02d}.parquet"


def build_gcs_path(year: int, month: int, service_type: str, bucket: str) -> str:
    """
    Build GCS destination path tương ứng với URL.
    Pattern: gs://{bucket}/trip-data/{service_type}/{year}/{month:02d}/
    """
    filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
    return f"gs://{bucket}/trip-data/{service_type}/{year}/{month:02d}/{filename}"


if __name__ == "__main__":
    # Quick smoke test
    url = build_trip_url(2024, 1, "yellow")
