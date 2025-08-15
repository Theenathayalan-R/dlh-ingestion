import time
from pyspark.sql.functions import col


def write_with_retry(spark, df, catalog_name, tbl_schema, job_status_tbl, logger, retry_delay: int = 5, max_retries: int = 50):
    """Write DataFrame to Iceberg with retry (logic copied from legacy)."""
    try:
        table_name = f"{catalog_name}.{tbl_schema}.{job_status_tbl}"
        df = df.withColumn("start_time", col("start_time").cast("timestamp")).withColumn("end_time", col("end_time").cast("timestamp"))
        attempt = 0
        while attempt < max_retries:
            try:
                df.coalesce(10).write.format("iceberg").mode("append").saveAsTable(table_name)
                return
            except Exception as e:  # pragma: no cover (timing dependent)
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise
    except Exception as e:  # pragma: no cover
        logger.log(f"Error writing DataFrame to table {job_status_tbl}: {str(e)}", "ERROR")
        raise


def rerun_failed_jobs(spark, run_group, catalog_name, tbl_schema, job_status_tbl, logger):
    """Fetch failed jobs for rerun (logic copied from legacy)."""
    try:
        failed_jobs_df = spark.sql(f"""
SELECT job_id, MAX(batch_id) as batch_id, MAX(spark_app_id) as spark_app_id
FROM {catalog_name}.{tbl_schema}.{job_status_tbl}
WHERE run_group = '{run_group}' AND status = 'FAILED'
GROUP BY job_id
""")
        logger.log(f"Fetched failed jobs for rerun: {failed_jobs_df.count()} jobs found.", "INFO")
        return failed_jobs_df
    except Exception as e:  # pragma: no cover
        logger.log(f"Error fetching failed jobs for rerun: {str(e)}", "ERROR")
        raise
