.PHONY: help airflow-up airflow-down airflow-logs spark-test spark-run dbt-run dbt-test upload-jobs

help:
	@echo "Available commands:"
	@echo "  make airflow-up      — Start Airflow (docker compose)"
	@echo "  make airflow-down    — Stop Airflow"
	@echo "  make airflow-logs    — Tail Airflow scheduler logs"
	@echo "  make spark-test      — Run pytest trong Docker container"
	@echo "  make spark-run       — Run local integration test (local parquet)"
	@echo "  make dbt-run         — dbt run"
	@echo "  make dbt-test        — dbt test"
	@echo "  make upload-jobs     — Package + upload spark_jobs lên GCS"

airflow-up:
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"

airflow-down:
	docker compose down

airflow-logs:
	docker compose logs -f airflow-scheduler

spark-test:
	docker run --rm \
		-v $(PWD):/app \
		-w /app \
		nyc-taxi-spark-dev \
		python -m pytest tests/spark/ -v

spark-run:
	docker run --rm \
		-v $(PWD):/app \
		-w /app \
		nyc-taxi-spark-dev \
		python -m spark_jobs.jobs.run_local

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

upload-jobs:
	zip -r spark_jobs.zip spark_jobs/ \
		--exclude "spark_jobs/*.jar" \
		--exclude "spark_jobs/__pycache__/*" \
		--exclude "spark_jobs/**/__pycache__/*"
	gcloud storage cp spark_jobs/jobs/conform_trips.py \
		gs://nyc-taxi-raw-banking-500108/jobs/conform_trips.py
	gcloud storage cp spark_jobs.zip \
		gs://nyc-taxi-raw-banking-500108/jobs/spark_jobs.zip
	rm spark_jobs.zip
	@echo "Done: jobs uploaded to GCS"
