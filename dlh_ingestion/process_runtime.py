import datetime
import threading
from functools import reduce
from typing import Any, Dict

# Removed global pyspark function imports to avoid lint errors when pyspark absent.

# ---------------- Internal Spark guards -----------------
try:  # set flag only
    import pyspark  # type: ignore
except Exception as _e:  # pragma: no cover
    pyspark = None  # type: ignore
    _PYSPARK_IMPORT_ERROR = _e  # type: ignore
else:  # pragma: no cover - executed only when pyspark present
    _PYSPARK_IMPORT_ERROR = None  # type: ignore


def _require_pyspark():  # pragma: no cover simple check
    if pyspark is None:
        raise ImportError(
            "PySpark is required for runtime ingestion functions. Install 'pyspark' before invoking these APIs."
        ) from _PYSPARK_IMPORT_ERROR


def _spark_funcs():  # import inside to keep top-level clean
    from pyspark.sql.functions import col, coalesce  # type: ignore
    import pyspark.sql.functions as F  # type: ignore
    return F, col, coalesce

# ---------------- Existing imports from sibling modules -----------------
from .logging_utils import CustomLogger
from .job_tracking import JobTracker
from .email_service import EmailService
from .jdbc_adaptive import (
    connect_to_oracle,
    connect_to_mssql,
    connect_to_mssql_columns,
    fetch_data_from_oracle,
)
from .loaders import (
    create_table,
    full_load_mssql,
    incremental_load_mssql,
)
from .orchestration import write_with_retry, rerun_failed_jobs

# ---------------- Config Model (Task C) -----------------
try:  # optional pydantic usage; degrade gracefully
    from pydantic import BaseModel
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore


class ConfigRow(BaseModel):  # type: ignore
    job_id: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    app_pipeline: str
    run_group: str
    is_enabled: str
    load_type: str | None = None
    target_partition_by: str | None = None
    target_truncate: str | None = None
    target_partition_overwrite: str | None = None
    target_table_options: str | None = None
    target_recreate: str | None = None
    source_fields: str | None = None
    source_where_clause: str | None = None
    cdc_type: str | None = None
    cdc_modified_date_column: str | None = None
    cdc_created_date_column: str | None = None
    cdc_append_key_column: str | None = None
    incremental_type: str | None = None
    incremental_column: str | None = None


# ---------------- Helper functions -----------------

def read_config_from_iceberg(spark, catalog_name, database, table, app_pipeline, run_group, logger=None):
    _require_pyspark()
    try:
        cfg_df = spark.sql(f"""
SELECT *
FROM {catalog_name}.{database}.{table}
WHERE app_pipeline = '{app_pipeline}' AND run_group = '{run_group}' AND is_active = 'Y'
""")
        if logger:
            logger.log(f"Successfully read configuration from {catalog_name}.{database}.{table}", "INFO")
        rows = cfg_df.collect()
        if not rows:
            raise ValueError("Configuration table is empty or no matching run_group found")
        return rows[0]
    except Exception as e:  # pragma: no cover
        if logger:
            logger.log(f"Error reading configuration from {catalog_name}.{database}.{table}: {str(e)}", "ERROR")
        raise


def get_run_id(spark, catalog_name, database, job_status_tbl, batch_id, logger=None):
    _require_pyspark()
    try:
        query = f"""
SELECT run_id
FROM {catalog_name}.{database}.{job_status_tbl}
WHERE batch_id = '{batch_id}'
ORDER BY start_time
"""
        run_id_df = spark.sql(query)
        if run_id_df.count() == 0 or run_id_df.rdd.isEmpty():  # legacy semantics
            return None
        return run_id_df.first()["run_id"]
    except Exception as e:  # pragma: no cover
        if logger:
            logger.log(f"Error fetching run id: {str(e)}", "ERROR")
        raise


def compare_and_notify_schema_changes(*args, **kwargs):  # placeholder retained
    current_schema = {}
    comments_text = "No schema changes detected."
    return current_schema, comments_text


def handle_job_completion_or_failure(
    spark,
    source_df,
    email_service,
    logger,
    job_id,
    source_schema,
    source_table,
    target_schema,
    target_table,
    batch_id,
    run_id,
    spark_app_id,
    catalog_name,
    tbl_schema,
    job_status_tbl,
    error_message,
    statusEmails,
    load_date,
    start_time,
    end_time,
):
    _require_pyspark()
    try:
        total_records = source_df.count() if source_df is not None else 0
        column_count = len(source_df.columns) if source_df is not None else 0
        status = "COMPLETED" if source_df is not None else "FAILED"
        elapsed_time = (
            datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            - datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        )
        elapsed_time_str = str(elapsed_time)
        job_details = {
            "job_id": job_id,
            "source_schema": source_schema,
            "source_table": source_table,
            "target_schema": target_schema,
            "target_table": target_table,
            "batch_id": batch_id,
            "run_id": run_id,
            "spark_app_id": spark_app_id,
            "records_processed": total_records,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "elapsed_time": elapsed_time_str,
            "error_message": error_message,
            "load_date": load_date,
        }
        if email_service and str(statusEmails).upper() == "Y":  # legacy flag
            email_service.send_job_run_email(job_details)  # dict accepted
        updated_status_df = spark.createDataFrame([
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "spark_app_id": spark_app_id,
                "run_group": None,
                "dlh_layer": None,
                "job_id": job_id,
                "table_name": f"{target_schema}.{target_table}",
                "schema": "",
                "column_count": column_count,
                "status": status,
                "start_time": start_time,
                "end_time": end_time,
                "records_processed": total_records,
                "records_rejected": 0,
                "error_message": error_message or "",
                "load_date": load_date or "",
                "comments": "",
            }
        ])
        return updated_status_df
    except Exception as e:  # pragma: no cover
        logger.log(f"Error during job completion or failure handling: {str(e)}", "ERROR")
        raise


def _build_incremental_predicate(
    spark,
    catalog_name: str,
    tbl_schema: str,
    cdc_tracker_tbl: str | None,
    job_id: str,
    column: str | None,
    predicate_type: str | None,
    logger,
) -> str:
    """Derive incremental predicate from tracker table, else return empty string (Task B)."""
    if not column or not predicate_type or not cdc_tracker_tbl:
        return ""
    try:
        tracker_table = f"{catalog_name}.{tbl_schema}.{cdc_tracker_tbl}"
        result_df = spark.sql(
            f"SELECT MAX({column}) as last_val FROM {tracker_table} WHERE job_id = '{job_id}'"
        )
        if result_df.count() == 0:
            return ""
        last_val = result_df.first()["last_val"]
        if last_val is None:
            return ""
        if predicate_type.upper() == "TIMESTAMP":
            return f"WHERE {column} > '{last_val}'"
        elif predicate_type.upper() in ("APPEND_KEY", "NUMERIC"):
            return f"WHERE {column} > {last_val}"
        return ""
    except Exception as e:  # pragma: no cover
        logger.log(f"Failed to derive incremental predicate: {str(e)}", "WARNING")
        return ""


def process_table(
    row: Dict[str, Any],
    spark,
    env,
    batch_id,
    spark_app_id,
    catalog_name,
    tbl_schema,
    job_status_tbl,
    cdc_tracker_tbl,
    s3_bucket,
    source_db_config,
    dbpass,
    email_service,
    logger,
    lock,
    formatted_date,
    statusEmails,
    job_tracker,
    run_id,
):
    _require_pyspark()
    from pyspark.sql import DataFrame  # type: ignore

    try:
        job_id = row["job_id"]
        source_schema = row["source_schema"]
        source_table = row["source_table"]
        target_schema = row["target_schema"]
        target_table = row["target_table"]
        table_name = f"{catalog_name}.{target_schema}.{target_table}"
        app_name = row["app_pipeline"]
        run_group = row["run_group"]
        is_enabled = row["is_enabled"]
        load_type = row.get("load_type")
        target_partition_by = row.get("target_partition_by")
        target_truncate = row.get("target_truncate")
        target_partition_overwrite = row.get("target_partition_overwrite")
        target_table_options = row.get("target_table_options")
        target_recreate = row.get("target_recreate")
        source_fields = row.get("source_fields")
        source_where_clause = row.get("source_where_clause") or ""
        cdc_type = row.get("cdc_type")
        cdc_modified_date_column = row.get("cdc_modified_date_column")
        cdc_created_date_column = row.get("cdc_created_date_column")
        cdc_append_key_column = row.get("cdc_append_key_column")
        incremental_type = row.get("incremental_type")
        incremental_column = row.get("incremental_column")
        source_db_type = source_db_config["db_type"]
        source_df: DataFrame | None = None
        load_date = None
        try:
            if hasattr(spark, "sparkContext"):
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"pool-{job_id}")
        except Exception:
            pass
        updated_status_df = job_tracker.insert_initial_status()
        # ---------- ORACLE ----------
        if source_db_type.upper() == "ORACLE":
            url = source_db_config["url"]
            user = source_db_config["db_user"]
            driver = source_db_config["driver"]
            dbtable = f"{source_schema}.{source_table}"
            meta_query = f"""
SELECT COLUMN_NAME, DATA_TYPE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = '{source_table.upper()}' AND OWNER = '{source_schema.upper()}'
"""
            data_df = connect_to_oracle(spark, url, meta_query, user, dbpass, driver, logger)
            predicate = ""
            if load_type and load_type.upper() == "INCREMENTAL":
                # Use cdc_modified_date_column or append key for predicate
                if cdc_modified_date_column:
                    predicate = _build_incremental_predicate(
                        spark,
                        catalog_name,
                        tbl_schema,
                        cdc_tracker_tbl,
                        job_id,
                        cdc_modified_date_column,
                        "TIMESTAMP",
                        logger,
                    )
                elif cdc_append_key_column:
                    predicate = _build_incremental_predicate(
                        spark,
                        catalog_name,
                        tbl_schema,
                        cdc_tracker_tbl,
                        job_id,
                        cdc_append_key_column,
                        "APPEND_KEY",
                        logger,
                    )
            where_clause_full = ""
            if source_where_clause and predicate:
                # merge user filter with incremental predicate
                where_clause_full = f"WHERE ({source_where_clause}) AND {predicate[6:]}"  # strip leading WHERE
            elif source_where_clause:
                where_clause_full = f"WHERE {source_where_clause}"
            else:
                where_clause_full = predicate
            # Fetch data
            source_df = fetch_data_from_oracle(
                spark,
                url,
                dbtable,
                user,
                dbpass,
                driver,
                data_df,
                cdc_modified_date_column,
                cdc_created_date_column,
                where_clause_full,
                source_fields,
                logger,
                source_table,
            )
            if load_type and load_type.upper() == "INCREMENTAL":
                load_date = create_table(
                    spark,
                    "ORACLE",
                    table_name,
                    source_df,
                    target_partition_by,
                    logger,
                    target_table_options,
                    target_recreate,
                    catalog_name,
                    target_truncate,
                )
            else:
                load_date = create_table(
                    spark,
                    "ORACLE",
                    table_name,
                    source_df,
                    target_partition_by,
                    logger,
                    target_table_options,
                    target_recreate,
                    catalog_name,
                    target_truncate,
                )
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_time = updated_status_df.select("start_time").first()["start_time"]
            return handle_job_completion_or_failure(
                spark=spark,
                source_df=source_df,
                email_service=email_service,
                logger=logger,
                job_id=job_id,
                source_schema=source_schema,
                source_table=source_table,
                target_schema=target_schema,
                target_table=target_table,
                batch_id=batch_id,
                run_id=run_id,
                spark_app_id=spark_app_id,
                catalog_name=catalog_name,
                tbl_schema=tbl_schema,
                job_status_tbl=job_status_tbl,
                error_message=None,
                statusEmails=statusEmails,
                load_date=load_date,
                start_time=start_time,
                end_time=end_time,
            )
        # ---------- MSSQL ----------
        elif source_db_type.upper() == "MSSQL":
            db_host = source_db_config["db_host"]
            db_port = source_db_config["db_port"]
            db_name = source_db_config["db_name"]
            db_user = source_db_config["db_user"]
            driver = source_db_config["driver"]
            encrypt = source_db_config.get("encrypt", "false")
            certificate = source_db_config.get("trust_server_certificate", "true")
            base_url = f"jdbc:sqlserver://{db_host}:{db_port};databaseName={db_name}"
            columns_query = f"""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{source_table}' AND TABLE_SCHEMA = '{source_schema}'
ORDER BY ORDINAL_POSITION
"""
            column_select = connect_to_mssql_columns(
                spark,
                base_url,
                columns_query,
                db_user,
                dbpass,
                driver,
                encrypt,
                certificate,
                logger,
            )
            predicate = ""
            if load_type and load_type.upper() == "INCREMENTAL":
                if incremental_type and incremental_column:
                    predicate = _build_incremental_predicate(
                        spark,
                        catalog_name,
                        tbl_schema,
                        cdc_tracker_tbl,
                        job_id,
                        incremental_column,
                        incremental_type,
                        logger,
                    )
            where_clause_full = predicate
            if source_where_clause and predicate:
                where_clause_full = f"WHERE ({source_where_clause}) AND {predicate[6:]}"
            elif source_where_clause and not predicate:
                where_clause_full = f"WHERE {source_where_clause}"
            query = f"SELECT {column_select or '*'} FROM {source_schema}.{source_table} {where_clause_full}".strip()
            if load_type and load_type.upper() == "INCREMENTAL":
                source_df = connect_to_mssql(
                    spark,
                    base_url,
                    query,
                    db_user,
                    dbpass,
                    driver,
                    encrypt,
                    certificate,
                    logger,
                )
                load_date = incremental_load_mssql(
                    spark,
                    source_df,
                    table_name,
                    target_partition_by,
                    logger,
                    lock,
                    job_id,
                    app_name,
                    run_group,
                    is_enabled,
                    source_schema,
                    source_table,
                    None,
                    None,
                    None,
                    None,
                    catalog_name,
                    tbl_schema,
                    cdc_tracker_tbl,
                    target_truncate,
                    target_partition_overwrite,
                    query,
                )
            else:
                source_df = connect_to_mssql(
                    spark,
                    base_url,
                    query,
                    db_user,
                    dbpass,
                    driver,
                    encrypt,
                    certificate,
                    logger,
                )
                load_date = full_load_mssql(
                    spark,
                    source_df,
                    table_name,
                    target_partition_by,
                    logger,
                    job_id,
                    app_name,
                    run_group,
                    is_enabled,
                    source_schema,
                    source_table,
                    catalog_name,
                    tbl_schema,
                    cdc_tracker_tbl,
                    target_truncate,
                    target_partition_overwrite,
                    query,
                )
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_time = updated_status_df.select("start_time").first()["start_time"]
            return handle_job_completion_or_failure(
                spark=spark,
                source_df=source_df,
                email_service=email_service,
                logger=logger,
                job_id=job_id,
                source_schema=source_schema,
                source_table=source_table,
                target_schema=target_schema,
                target_table=target_table,
                batch_id=batch_id,
                run_id=run_id,
                spark_app_id=spark_app_id,
                catalog_name=catalog_name,
                tbl_schema=tbl_schema,
                job_status_tbl=job_status_tbl,
                error_message=None,
                statusEmails=statusEmails,
                load_date=load_date,
                start_time=start_time,
                end_time=end_time,
            )
        else:
            raise ValueError(f"Unsupported source_db_type: {source_db_type}")
    except Exception as e:  # pragma: no cover
        logger.log(f"Error in process_table for {row.get('job_id')}: {str(e)}", "ERROR")
        raise
    finally:
        try:
            if hasattr(spark, "sparkContext"):
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
        except Exception:
            pass


def main():  # High-level orchestration retained; minimal modifications
    _require_pyspark()
    spark = None
    logger = None
    alert_config: Dict[str, Any] = {}
    try:
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.builder.getOrCreate()
        try:
            spark.conf.set("spark.scheduler.mode", "FAIR")
        except Exception:
            pass
        import sys, os
        from pyspark import SparkFiles  # type: ignore
        if len(sys.argv) < 4:
            print("Usage: script.py <run_group> <app_pipeline> <env> [rerun]")
            sys.exit(1)
        run_group = sys.argv[1]
        app_pipeline = sys.argv[2]
        app_name = f"{app_pipeline}-{run_group}"
        env = sys.argv[3]
        rerun = sys.argv[4] if len(sys.argv) > 4 else None
        files = os.listdir(SparkFiles.getRootDirectory())
        if files:
            print(f"File-1 added with --files: {files[0]}")
            if len(files) > 1:
                print(f"File-2 added with --files: {files[1]}")
        alert_config = read_config_from_iceberg(spark, "", "", "", run_group, app_pipeline, None)
        s3_bucket = alert_config.get("log_bucket", "s3://temp-bucket/")
        s3_prefix = alert_config.get("log_reference", "/temp-prefix/")
        app_name_cfg = alert_config.get("app_pipeline", app_name)
        smtp_server = alert_config.get("smtp_server", "")
        smtp_port = alert_config.get("smtp_port", 25)
        statusEmails = alert_config.get("statusEmails", "Y")
        retention_period = alert_config.get("retention_period", 7)
        sender_email = alert_config.get("alert_email_from", "")
        success_email = alert_config.get("success_email_to", "")
        failure_email = alert_config.get("failure_email_to", "")
        logger = CustomLogger(spark, s3_bucket, s3_prefix, app_name_cfg)
        logger.log(f"Logger is Intialized for app: {app_name_cfg}")
        logger.log("Log file initiated", "INFO")
        catalog_name = alert_config.get("catalog_name", "")
        schema_config = alert_config.get("schema_config", {})
        tbl_schema = schema_config.get("tbl_schema", "")
        ingest_tbl = alert_config.get("ingest_tbl", "")
        table_name = f"{catalog_name}.{tbl_schema}.{ingest_tbl}"
        if alert_config.get("alert_type", "EMAIL").upper() == "EMAIL":
            email_service = EmailService(
                spark=spark,
                batch_id=None,
                env=env,
                app_name=app_name_cfg,
                run_group=run_group,
                smtp_server=smtp_server,
                smtp_port=smtp_port,
                sender_email=sender_email,
                success_email=success_email,
                failure_email=failure_email,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                logger=logger,
            )
        else:
            email_service = None
        if rerun and rerun.upper() == "RERUN":
            failed_jobs_df = rerun_failed_jobs(spark, run_group, catalog_name, tbl_schema, job_status_tbl=alert_config.get("job_status_tbl", ""), logger=logger)
            if failed_jobs_df.count() == 0:
                print("No failed jobs found for rerun.")
                return 0
        try:
            if hasattr(spark, "sparkContext"):
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", "main")
        except Exception:
            pass
        spark_app_id = spark.sparkContext.applicationId
        batch_id = datetime.datetime.now().strftime("%Y%m%d%H")
        formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        from pyspark.sql.functions import col  # type: ignore
        df = spark.table(table_name).filter(
            (col("app_pipeline") == app_pipeline)
            & (col("run_group") == run_group)
            & (col("is_enabled") == "Y")
            & (col("is_active") == "Y")
        ).persist()
        source_db_config = alert_config.get("source_db_config", {})
        dbpass = alert_config.get("dbpass", "")
        rows = df.collect() or []
        if not rows:
            logger.log("No rows returned from configuration table – exiting.", "WARNING")
            return 0
        lock = threading.Lock()
        run_id_val = get_run_id(spark, catalog_name, tbl_schema, alert_config.get("job_status_tbl", ""), batch_id, logger)
        run_id = run_id_val or ""  # ensure string for JobTracker
        logger.log(f"Job initiated for batch id: {batch_id} run_id: {run_id}", "INFO")
        max_workers = alert_config.get("max_workers", 5)
        from concurrent.futures import ThreadPoolExecutor
        futures = []
        initial_status_dfs = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for r in rows:
                r_dict = r.asDict()
                cfg_row = ConfigRow(**r_dict) if isinstance(ConfigRow, type) else r_dict
                job_tracker = JobTracker(
                    spark=spark,
                    batch_id=batch_id,
                    run_id=run_id,
                    spark_app_id=spark_app_id,
                    catalog_name=catalog_name,
                    database=tbl_schema,
                    job_status_tbl=alert_config.get("job_status_tbl", ""),
                    job_id=r_dict["job_id"],
                    table_name=r_dict["target_table"],
                    run_group=run_group,
                    logger=logger,
                )
                initial_df = job_tracker.insert_initial_status()
                initial_status_dfs.append(initial_df)
                futures.append(
                    executor.submit(
                        process_table,
                        row=cfg_row.dict() if hasattr(cfg_row, "dict") else cfg_row,
                        spark=spark,
                        env=env,
                        batch_id=batch_id,
                        spark_app_id=spark_app_id,
                        catalog_name=catalog_name,
                        tbl_schema=tbl_schema,
                        job_status_tbl=alert_config.get("job_status_tbl", ""),
                        cdc_tracker_tbl=alert_config.get("cdc_tracker_tbl", ""),
                        s3_bucket=s3_bucket,
                        source_db_config=source_db_config,
                        dbpass=dbpass,
                        email_service=email_service,
                        logger=logger,
                        lock=lock,
                        formatted_date=formatted_date,
                        statusEmails=statusEmails,
                        job_tracker=job_tracker,
                        run_id=run_id,
                    )
                )
        updated_status_dfs = [f.result() for f in futures]
        from functools import reduce as _reduce
        if initial_status_dfs:
            consolidated_initial = _reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), initial_status_dfs)
        else:
            consolidated_initial = spark.createDataFrame([], spark.table(f"{catalog_name}.{tbl_schema}.{alert_config.get('job_status_tbl', '')}").schema)
        if updated_status_dfs:
            consolidated_updated = _reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), updated_status_dfs)
        else:
            consolidated_updated = spark.createDataFrame([], consolidated_initial.schema)
        from pyspark.sql.functions import col, coalesce  # type: ignore
        final_status_df = consolidated_initial.alias("initial").join(
            consolidated_updated.alias("updated"),
            on="job_id",
            how="outer",
        ).select(
            coalesce(col("updated.batch_id"), col("initial.batch_id")).alias("batch_id"),
            coalesce(col("updated.run_id"), col("initial.run_id")).alias("run_id"),
            coalesce(col("updated.spark_app_id"), col("initial.spark_app_id")).alias("spark_app_id"),
            coalesce(col("updated.run_group"), col("initial.run_group")).alias("run_group"),
            coalesce(col("updated.dlh_layer"), col("initial.dlh_layer")).alias("dlh_layer"),
            coalesce(col("updated.job_id"), col("initial.job_id")).alias("job_id"),
            coalesce(col("updated.table_name"), col("initial.table_name")).alias("table_name"),
            coalesce(col("updated.status"), col("initial.status")).alias("status"),
            coalesce(col("updated.start_time"), col("initial.start_time")).alias("start_time"),
            coalesce(col("updated.end_time"), col("initial.end_time")).alias("end_time"),
            coalesce(col("updated.column_count"), col("initial.column_count")).alias("column_count"),
            coalesce(col("updated.records_processed"), col("initial.records_processed")).alias("records_processed"),
            coalesce(col("updated.records_rejected"), col("initial.records_rejected")).alias("records_rejected"),
            coalesce(col("updated.error_message"), col("initial.error_message")).alias("error_message"),
            coalesce(col("updated.load_date"), col("initial.load_date")).alias("load_date"),
            coalesce(col("updated.comments"), col("initial.comments")).alias("comments"),
            coalesce(col("updated.schema"), col("initial.schema")).alias("schema"),
        )
        try:
            write_with_retry(spark, final_status_df, catalog_name, tbl_schema, alert_config.get("job_status_tbl", ""), logger)
        except Exception as e:
            logger.log(f"Write operation failed: {str(e)}", "ERROR")
        logger.save_to_s3(alert_config.get("log_reference", "/log_reference/"))
        return 0
    except Exception as e:  # pragma: no cover
        error_message = f"Fatal error in main: {e}"
        if logger:
            logger.log(error_message, "ERROR")
            try:
                logger.save_to_s3(alert_config.get("log_reference", "/log_reference/"))
            except Exception:
                pass
        else:
            print(error_message)
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
