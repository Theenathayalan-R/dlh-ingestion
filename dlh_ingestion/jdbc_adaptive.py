import datetime
import time
from typing import Any, Optional

# We reuse sanitize_column_name from utils
from .utils import sanitize_column_name


def connect_to_oracle(spark, url: str, query: str, user: str, password: str, driver: str, logger):
    try:
        data_df = (
            spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .load()
        )
        logger.log("Oracle connection is made successfully", "INFO")
        return data_df
    except Exception as e:  # pragma: no cover
        logger.log(f"Error connecting to Oracle: {str(e)}", "ERROR")
        raise


def connect_to_mssql(spark, url: str, query: str, user: str, password: str, driver: str, encrypt: str, certificate: str, logger):
    try:
        source_df = (
            spark.read
            .format("jdbc")
            .option("driver", driver)
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("Encrypt", encrypt)
            .option("TrustServerCertificate", certificate)
            .load()
        )
        logger.log("MSSQL connection is made successfully", "INFO")
        return source_df
    except Exception as e:  # pragma: no cover
        logger.log(f"Error connecting to MSSQL: {str(e)}", "ERROR")
        raise


def connect_to_mssql_columns(spark, url, query, user, password, driver, encrypt, certificate, logger):
    try:
        columns_df = (
            spark.read
            .format("jdbc")
            .option("driver", driver)
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("Encrypt", encrypt)
            .option("TrustServerCertificate", certificate)
            .load()
        )
        columns = columns_df.collect()
        logger.log("MSSQL connection is made successfully", "INFO")
        column_list = [f"[{row['COLUMN_NAME']}] as {sanitize_column_name(row['COLUMN_NAME'], logger)}" for row in columns]
        return ",".join(column_list)
    except Exception as e:  # pragma: no cover
        logger.log(f"Error connecting to MSSQL columns: {str(e)}", "ERROR")
        raise


def map_data_types(spark, data_df, cdc_modified_date_column, cdc_created_date_column, sourcedb_conn,
                   sourcedb_user, sourcedb_pass, sourcedb_driver, sourcedb_tblname, logger):
    try:
        mapped_columns = []
        for row in data_df.collect():
            col_name = row["COLUMN_NAME"]
            data_type = row["DATA_TYPE"]
            if data_type == "NUMBER":
                query = f"SELECT COUNT(*) as DECIMAL_COUNT FROM {sourcedb_tblname} WHERE {col_name} != TRUNC({col_name})"
                data_scale_df = connect_to_oracle(spark, sourcedb_conn, query, sourcedb_user, sourcedb_pass, sourcedb_driver, logger)
                data_scale = data_scale_df.first()["DECIMAL_COUNT"]
                if data_scale > 0:
                    query = f"SELECT CAST(MAX(LENGTH(SUBSTR({col_name}, INSTR({col_name}, '.') + 1))) AS INT) AS DATA_SCALE FROM {sourcedb_tblname} WHERE {col_name} != TRUNC({col_name})"
                    data_scale_df = connect_to_oracle(spark, sourcedb_conn, query, sourcedb_user, sourcedb_pass, sourcedb_driver, logger)
                    data_scale = data_scale_df.first()["DATA_SCALE"]
                    mapped_type = f"DECIMAL(38, {data_scale})"
                else:
                    mapped_type = "LONG"
            elif data_type in {"VARCHAR2"}:
                mapped_type = "STRING"
            elif data_type in {"DATE", "TIMESTAMP", "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6)"}:
                mapped_type = "TIMESTAMP"
            elif data_type == "BLOB":
                mapped_type = "BINARY"
            else:
                mapped_type = "STRING"
            mapped_columns.append(f"{col_name} {mapped_type}")
        return ", ".join(mapped_columns)
    except Exception as e:  # pragma: no cover
        logger.log(f"Error mapping data types: {str(e)}", "ERROR")
        raise


def probe_read(spark, url, dbtable, user, password, driver, logger, limit=500, fetchsize=300, customSchema=None,
               target_batch_bytes=None, max_fetchsize_override=10000):
    """Lightweight probe identical to legacy but modular."""
    try:
        start = time.time()
        probe_query = f"SELECT * FROM {dbtable} WHERE ROWNUM <= {int(limit)}"
        reader = (spark.read.format("jdbc")
                  .option("url", url)
                  .option("query", probe_query)
                  .option("user", user)
                  .option("password", password)
                  .option("driver", driver)
                  .option("fetchsize", int(fetchsize)))
        if customSchema:
            reader = reader.option("customSchema", customSchema)
        probe_df = reader.load()
        row_count = probe_df.count()
        sample_n = min(row_count, 50)
        est_row_bytes = None
        if sample_n > 0:
            sample_rows = probe_df.take(sample_n)
            total_bytes = 0
            for r in sample_rows:
                total_bytes += sum(len(str(v)) if v is not None else 1 for v in r)
            est_row_bytes = max(1, int(total_bytes / sample_n))
        elapsed_ms = int((time.time() - start) * 1000)
        throughput_rps = (row_count / (elapsed_ms / 1000.0)) if elapsed_ms > 0 else None
        recommended_fetch = None
        if est_row_bytes and target_batch_bytes:
            recommended_fetch = int(target_batch_bytes / est_row_bytes)
            recommended_fetch = max(50, min(recommended_fetch, max_fetchsize_override))
        metrics = {
            "probe_limit": int(limit),
            "rows_returned": row_count,
            "elapsed_ms": elapsed_ms,
            "throughput_rows_per_sec": int(throughput_rps) if throughput_rps else None,
            "est_row_bytes": est_row_bytes,
            "recommended_fetchsize": recommended_fetch,
        }
        logger.log(
            f"Probe metrics -> limit={limit}, rows={row_count}, elapsed_ms={elapsed_ms}, est_row_bytes={est_row_bytes}, recommended_fetchsize={recommended_fetch}",
            "INFO"
        )
        return metrics
    except Exception as e:  # pragma: no cover
        logger.log(f"Probe read failed (non-fatal): {e}", "WARNING")
        return {}


def read_from_jdbc(spark, url, query, user, password, driver, schema_string, source_table, dbtable, logger):
    try:
        owner = dbtable.split('.')[0]
        table_name_only = dbtable.split('.')[-1]
        object_type_q = f"""
SELECT OBJECT_TYPE FROM ALL_OBJECTS
WHERE OBJECT_NAME = '{table_name_only}' AND OWNER = '{owner}'
"""
        object_type_df = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("query", object_type_q)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .load()
        )
        OBJECT_TYPE = object_type_df.first()["OBJECT_TYPE"] if not object_type_df.rdd.isEmpty() else None
        if not OBJECT_TYPE:
            raise ValueError(f"Could not determine object type for {dbtable}")

        num_rows = 0
        avg_row_len = None
        col_count = 0
        if OBJECT_TYPE == "TABLE":
            meta_q = f"""
SELECT t.NUM_ROWS, t.AVG_ROW_LEN,
       (SELECT COUNT(*) FROM ALL_TAB_COLUMNS c WHERE c.OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME) AS COL_COUNT
FROM ALL_TABLES t
WHERE t.OWNER = '{owner}' AND t.TABLE_NAME = '{table_name_only}'
"""
            meta_df = (
                spark.read.format("jdbc")
                .option("url", url)
                .option("query", meta_q)
                .option("user", user)
                .option("password", password)
                .option("driver", driver)
                .load()
            )
            if not meta_df.rdd.isEmpty():
                r = meta_df.first()
                num_rows = int(r["NUM_ROWS"]) if r["NUM_ROWS"] is not None else 0
                avg_row_len = int(r["AVG_ROW_LEN"]) if r["AVG_ROW_LEN"] is not None else None
                col_count = int(r["COL_COUNT"]) if r["COL_COUNT"] is not None else 0
        else:
            rowcount_q = f"SELECT COUNT(*) AS NUM_ROWS FROM {dbtable}"
            rc_df = (
                spark.read.format("jdbc")
                .option("url", url)
                .option("query", rowcount_q)
                .option("user", user)
                .option("password", password)
                .option("driver", driver)
                .load()
            )
            num_rows = int(rc_df.first()["NUM_ROWS"]) if not rc_df.rdd.isEmpty() else 0
            col_len_q = f"""SELECT SUM(DATA_LENGTH) AS AVG_ROW_LEN, COUNT(*) AS COL_COUNT
FROM ALL_TAB_COLUMNS WHERE OWNER='{owner}' AND TABLE_NAME='{table_name_only}'"""
            len_df = (
                spark.read.format("jdbc")
                .option("url", url)
                .option("query", col_len_q)
                .option("user", user)
                .option("password", password)
                .option("driver", driver)
                .load()
            )
            if not len_df.rdd.isEmpty():
                r = len_df.first()
                avg_row_len = int(r["AVG_ROW_LEN"]) if r["AVG_ROW_LEN"] is not None else None
                col_count = int(r["COL_COUNT"]) if r["COL_COUNT"] is not None else 0

        if not avg_row_len or avg_row_len <= 0:
            avg_row_len = 500

        dataset_bytes = num_rows * avg_row_len
        thr_cfg = spark.conf.get("dlh.jdbc.sizeThresholdsMB", "100,1024")
        try:
            parts = [int(p.strip()) for p in thr_cfg.split(",") if p.strip()]
        except Exception:  # pragma: no cover
            parts = [100, 1024]
        while len(parts) < 2:
            parts.append(parts[-1] * 2)
        small_mb, medium_mb = parts[0], parts[1]
        small_bytes = small_mb * 1024 * 1024
        medium_bytes = medium_mb * 1024 * 1024
        if dataset_bytes < small_bytes:
            size_class = "small"
        elif dataset_bytes < medium_bytes:
            size_class = "medium"
        else:
            size_class = "large"

        target_batch_mb_cfg = spark.conf.get("dlh.jdbc.targetBatchMB", "8")
        try:
            target_batch_mb = float(target_batch_mb_cfg)
        except ValueError:
            target_batch_mb = 8.0
        target_batch_bytes = max(1.0, target_batch_mb) * 1024 * 1024

        max_fetchsize_override = spark.conf.get("dlh.jdbc.maxFetchSize", "10000")
        try:
            max_fetchsize_override = int(max_fetchsize_override)
        except ValueError:  # pragma: no cover
            max_fetchsize_override = 10000

        base_fetch_map = {"small": 2000, "medium": 4000, "large": 8000}
        base_fetch = base_fetch_map.get(size_class, 2000)
        cap_by_batch = int(target_batch_bytes / avg_row_len) if avg_row_len > 0 else base_fetch
        if cap_by_batch < 50:
            cap_by_batch = 50
        fetchsize = min(base_fetch, cap_by_batch, max_fetchsize_override)
        if fetchsize < 50:
            fetchsize = 50

        enable_probe = spark.conf.get("dlh.jdbc.enableProbe", "false").lower() in {"true", "1", "yes", "y"}
        if enable_probe:
            probe_limit_cfg = spark.conf.get("dlh.jdbc.probeLimit", "500")
            try:
                probe_limit = int(probe_limit_cfg)
            except ValueError:  # pragma: no cover
                probe_limit = 500
            probe_metrics = probe_read(
                spark=spark,
                url=url,
                dbtable=dbtable,
                user=user,
                password=password,
                driver=driver,
                logger=logger,
                limit=probe_limit,
                fetchsize=min(fetchsize, 500),
                customSchema=schema_string,
                target_batch_bytes=target_batch_bytes,
                max_fetchsize_override=max_fetchsize_override,
            )
            rec = probe_metrics.get("recommended_fetchsize")
            if rec and rec > 0:
                blended = min(rec, max_fetchsize_override)
                if blended < 50:
                    blended = 50
                logger.log(f"Fetchsize refinement: initial={fetchsize} -> blended={blended} (probe)", "INFO")
                fetchsize = blended

        try:
            pool_prefix = spark.conf.get("dlh.jdbc.poolPrefix", "")
            pool_name = f"{pool_prefix}{size_class}" if pool_prefix else size_class
            if hasattr(spark, "sparkContext"):
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
        except Exception as pool_ex:  # pragma: no cover
            logger.log(f"Pool assignment failed: {pool_ex}", "WARNING")

        logger.log(
            "JDBC adaptive summary -> "
            f"object_type={OBJECT_TYPE}, num_rows={num_rows}, avg_row_len={avg_row_len}, col_count={col_count}, "
            f"dataset_bytes={dataset_bytes}, size_class={size_class}, fetchsize={fetchsize}, target_batch_bytes={int(target_batch_bytes)}, max_override={max_fetchsize_override}",
            "INFO",
        )

        source_df = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .option("customSchema", schema_string)
            .option("fetchsize", fetchsize)
            .load()
        )
        return source_df
    except Exception as e:  # pragma: no cover
        logger.log(f"Error reading from JDBC: {str(e)}", "ERROR")
        raise


def fetch_data_from_oracle(spark, sourcedb_conn, sourcedb_tblname, sourcedb_user, sourcedb_pass, sourcedb_driver,
                           data_df, cdc_modified_date_column, cdc_created_date_column, source_where_clause,
                           source_fields, logger, source_table):
    try:
        schema_string = map_data_types(
            spark,
            data_df,
            cdc_modified_date_column,
            cdc_created_date_column,
            sourcedb_conn,
            sourcedb_user,
            sourcedb_pass,
            sourcedb_driver,
            sourcedb_tblname,
            logger
        )
        if (not source_where_clause or source_where_clause == "") and (not source_fields or source_fields == ""):
            query = f"SELECT /*+ PARALLEL(16) */ * FROM {sourcedb_tblname}"
        elif (not source_fields or source_fields == "") and (source_where_clause and source_where_clause != ""):
            query = f"SELECT * FROM {sourcedb_tblname} WHERE {source_where_clause}"
        elif (source_fields and source_fields != "") and (not source_where_clause or source_where_clause == ""):
            query = f"SELECT {source_fields} FROM {sourcedb_tblname}"
        else:
            query = f"SELECT {source_fields} FROM {sourcedb_tblname} WHERE {source_where_clause}"
        source_df = read_from_jdbc(
            spark=spark,
            url=sourcedb_conn,
            query=query,
            user=sourcedb_user,
            password=sourcedb_pass,
            driver=sourcedb_driver,
            schema_string=schema_string,
            source_table=source_table,
            dbtable=sourcedb_tblname,
            logger=logger
        )
        logger.log("Oracle connection is made successfully and data fetched", "INFO")
        return source_df
    except Exception as e:  # pragma: no cover
        logger.log(f"Error fetching data from Oracle: {str(e)}", "ERROR")
        raise
