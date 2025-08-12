# dlh_run_db_ingestion

A single-file Python module that orchestrates database ingestion jobs into Apache Iceberg using Spark. It includes:
- Job tracking and status writes
- Structured logging and optional log persistence to S3
- Schema change checks and notification hooks
- Retry-on-write for Iceberg table updates
- Optional email notifications for batch start, per-job status, and consolidated reports

## Repository contents

- dlh_run_db_ingestion.py — The reconstructed module (single file as requested)
- python_dlh_ingestion_run_db_job.py — Thin wrapper to match unit test import/patch paths (re-exports SparkSession, CustomLogger, JobTracker)
- tests/test_logger_and_jobtracker.py — Recreated unit tests based on the shared screenshot
- README.md — This file

## Requirements

- Python 3.12+
- Apache Spark 3.4.4
- PySpark 3.4.x (to match Spark 3.4.4) — install locally to run unit tests
- A Spark runtime (3.4.4) if you intend to run the main ingestion flow
- If using S3 features in real runs: appropriate Hadoop/S3 configuration and credentials

Install PySpark 3.4.x (tests):
```
python3 -m pip install --upgrade pip
python3 -m pip install 'pyspark>=3.4,<3.5'
```

## Running unit tests

The included tests validate CustomLogger and JobTracker behavior and are designed to match your screenshot’s import and patch targets.

```
cd /home/ubuntu/repos/dlh_run_db_ingestion
python3 -m unittest discover -s tests -p "test_*.py" -v
```

What the tests cover:
- CustomLogger.log and get_log_content
- Patchable CustomLogger.save_to_s3
- JobTracker.insert_initial_status and update_status generate a single DataFrame via spark.createDataFrame

Note on imports/patching:
- Tests import from `python_dlh_ingestion_run_db_job` and patch `SparkSession` and `CustomLogger.save_to_s3`. The wrapper module re-exports these from `dlh_run_db_ingestion` specifically to satisfy those targets.

## Running the ingestion script

The module defines `main()` and can be executed directly after setting up Spark and any necessary configs. Expected args:

```
python3 dlh_run_db_ingestion.py <run_group> <app_pipeline> <env> [rerun]
```

Example:
```
python3 dlh_run_db_ingestion.py daily pipeline-a dev
python3 dlh_run_db_ingestion.py daily pipeline-a dev RERUN
```

High-level flow (from the reconstructed code):
- Build SparkSession
- Read alert/config from Iceberg
- Initialize logger and optional email service
- Read ingestion configuration table
- Create initial job statuses, then process tables concurrently
- Consolidate initial and updated statuses and write to Iceberg (with retry)
- Persist log stream to S3 (if configured)
- Stop Spark

## Notes and limitations

- Some configuration keys (catalog, schema names, table names, SMTP settings, S3 locations) are read from Iceberg-stored configs; ensure your environment provides these before executing main().
- Email functionality requires valid SMTP details; S3 logging requires valid cloud credentials and Hadoop/S3 setup.

## Running tests with coverage
## FAIR scheduling and per-job pools

This module enables Spark FAIR scheduling at runtime and assigns a per-job scheduler pool when processing each table. To ensure concurrent execution across jobs, run with FAIR enabled (you can also set it via spark-submit):

Recommended spark-submit flags:
- --conf spark.scheduler.mode=FAIR
- Optionally: --conf spark.scheduler.allocation.file=/path/to/fairscheduler.xml

The code assigns a distinct pool per job using the SparkContext local property "spark.scheduler.pool" so that jobs submitted via ThreadPoolExecutor can run concurrently under FAIR scheduling.

- Ensure Python 3.12 and coverage are available
- Commands:
  - pip install coverage
  - coverage run -m unittest discover -s tests -p "test_*.py" -v
  - coverage report -m dlh_run_db_ingestion.py

## Current test coverage

- All unit tests pass locally.
- Email tests are intentionally skipped per request.
- Current coverage for dlh_run_db_ingestion.py is approximately 51% with email tests skipped.

## License

Proprietary or as per your project’s policy. 
