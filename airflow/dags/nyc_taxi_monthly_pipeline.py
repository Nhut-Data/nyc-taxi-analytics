from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import os
import sys
from airflow.models.param import Param

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from callbacks import notify_failure, notify_success  # noqa: E402

# ── Config ───────────────────────────────────────────────────
PROJECT_ID = "banking-data-platform-500108"
REGION = "us-central1"
RAW_BUCKET = "nyc-taxi-raw-banking-500108"
STAGING_BUCKET = "nyc-taxi-dataproc-staging-500108"
RUNTIME_VERSION = "2.3"

BQ_CONFORMED_TABLE = f"{PROJECT_ID}.nyc_taxi_conformed.trips"
BQ_QUARANTINE_TABLE = f"{PROJECT_ID}.nyc_taxi_quarantine.rejected_trips"
MIN_EXPECTED_ROWS = 100_000

DEFAULT_ARGS = {
    "owner": "nhutdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="nyc_taxi_monthly_pipeline",
    default_args=DEFAULT_ARGS,
    description="Monthly NYC Taxi ETL pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "spark", "dbt"],
    params={
        "year": Param(
            None,
            type=["null", "integer"],
            description=(
                "Override year (demo/backfill). Để trống = tự tính theo "
                "logical_date - 2 tháng."
            ),
        ),
        "month": Param(
            None,
            type=["null", "integer"],
            description="Override month (demo/backfill).",
        ),
    },
)
def nyc_taxi_monthly_pipeline():

    @task
    def get_target_period(logical_date=None, dag_run=None) -> dict:
        """
        Tính year/month cần process.
        Ưu tiên override từ dag_run.conf (dùng khi demo/backfill thủ công qua
        "Trigger DAG w/ config"); mặc định tự tính: Data TLC trễ ~2 tháng
        → target = logical_date - 2 tháng.
        """
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        conf_year, conf_month = conf.get("year"), conf.get("month")

        if conf_year and conf_month:
            year, month = conf_year, conf_month
        else:
            if logical_date is None:
                logical_date = pendulum.now("UTC")
            target = logical_date.subtract(months=2)
            year, month = target.year, target.month

        result = {
            "year": year,
            "month": month,
            "year_month": f"{year}-{month:02d}",
            "gcs_raw_path": (
                f"gs://{RAW_BUCKET}/trip-data/yellow"
                f"/{year}/{month:02d}"
                f"/yellow_tripdata_{year}-{month:02d}.parquet"
            ),
        }
        print(f"Target period: {result['year_month']}")
        return result

    @task
    def download_to_gcs(period: dict) -> str:
        """Tải Parquet từ TLC CloudFront lên GCS. Skip nếu đã tồn tại."""
        import sys

        sys.path.insert(0, "/opt/airflow")

        from ingestion.download_to_gcs import download_to_gcs as _download
        from ingestion.zone_lookup_loader import upload_zone_lookup

        gcs_path = _download(
            year=period["year"],
            month=period["month"],
            service_type="yellow",
            bucket=RAW_BUCKET,
        )
        upload_zone_lookup(bucket=RAW_BUCKET)
        print(f"GCS path: {gcs_path}")
        return gcs_path

    @task
    def row_count_sanity_check(period: dict) -> int:
        """Verify BigQuery có đủ rows sau khi Spark job xong."""
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        query = f"""
            SELECT COUNT(*) as cnt
            FROM `{BQ_CONFORMED_TABLE}`
            WHERE pickup_date >= '{period["year_month"]}-01'
              AND pickup_date < DATE_ADD(DATE '{period["year_month"]}-01', INTERVAL 1 MONTH)
        """
        result = hook.get_first(sql=query, parameters=None)
        count = result[0]
        print(f"Row count for {period['year_month']}: {count:,}")

        if count < MIN_EXPECTED_ROWS:
            raise ValueError(
                f"Sanity check FAILED: {count:,} rows < {MIN_EXPECTED_ROWS:,} "
                f"expected for {period['year_month']}"
            )
        print("Sanity check PASSED")
        return count

    # ── Tasks ─────────────────────────────────────────────────
    period = get_target_period()
    gcs_path = download_to_gcs(period)

    # DataprocCreateBatchOperator không phải @task nên dùng trực tiếp
    submit_dataproc = DataprocCreateBatchOperator(
        task_id="submit_dataproc_job",
        project_id=PROJECT_ID,
        region=REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{RAW_BUCKET}/jobs/conform_trips.py",
                "python_file_uris": [f"gs://{RAW_BUCKET}/jobs/spark_jobs.zip"],
                "args": [
                    "--gcs-raw-path",
                    "{{ ti.xcom_pull(task_ids='download_to_gcs') }}",
                    "--zone-lookup-path",
                    f"gs://{RAW_BUCKET}/reference/taxi_zone_lookup.csv",
                    "--bq-conformed-table",
                    BQ_CONFORMED_TABLE,
                    "--bq-quarantine-table",
                    BQ_QUARANTINE_TABLE,
                    "--service-type",
                    "yellow",
                    "--ingestion-date",
                    "{{ ti.xcom_pull(task_ids='get_target_period')['year_month'] }}-01",
                    "--gcs-temp-bucket",
                    STAGING_BUCKET,
                ],
            },
            "runtime_config": {"version": RUNTIME_VERSION},
        },
        batch_id="nyc-taxi-conform-{{ ts_nodash | lower }}",
        gcp_conn_id="google_cloud_default",
        on_failure_callback=notify_failure,
    )

    sanity = row_count_sanity_check(period)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt",
        on_failure_callback=notify_failure,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
        on_failure_callback=notify_failure,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt",
        on_success_callback=notify_success,
    )

    # ── Dependencies ──────────────────────────────────────────
    gcs_path >> submit_dataproc >> sanity >> dbt_run >> dbt_test >> dbt_docs


nyc_taxi_monthly_pipeline()
