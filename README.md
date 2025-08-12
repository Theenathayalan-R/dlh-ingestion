# dlh_run_db_ingestion

Reconstructed single-file Python module from the provided PDF. The file orchestrates database ingestion jobs into Iceberg via Spark, includes job tracking and logging to S3, schema change checks, retry-on-write, and optional email notifications.

## Repository contents

- dlh_run_db_ingestion.py — The reconstructed module (single file as requested)
- python_dlh_ingestion_run_db_job.py — Thin wrapper to match unit test import/patch paths (re-exports SparkSession, CustomLogger, JobTracker)
- tests/test_logger_and_jobtracker.py — Recreated unit tests based on the shared screenshot
- README.md — This file

## Requirements

- Python 3.12+
- pyspark (installed locally for running unit tests)
- A Spark runtime if you intend to run the main ingestion flow
- If using S3 features in real runs: appropriate Hadoop/S3 configuration and credentials

Install pyspark for tests:
```
python3 -m pip install --upgrade pip
python3 -m pip install pyspark
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

- This is a faithful reconstruction from the PDF; logic and structure were preserved as-is without refactoring.
- Some configuration keys (catalog, schema names, table names, SMTP settings, S3 locations) are read from the Iceberg-stored configs; ensure your environment provides these before executing main().
- Email functionality requires valid SMTP details; S3 logging requires valid cloud credentials and Hadoop/S3 setup.

## License

Proprietary or as per your project’s policy. 
