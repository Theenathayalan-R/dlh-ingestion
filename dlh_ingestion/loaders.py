import datetime


def _require_pyspark_funcs():  # pragma: no cover
    try:
        from pyspark.sql.functions import lit, expr  # type: ignore
        return lit, expr
    except Exception as e:
        raise ImportError("PySpark required for loaders functionality") from e


def create_table(
    spark,
    source_db_type,
    table_name,
    source_df,
    target_partition_by,
    logger,
    target_table_options,
    target_recreate,
    catalog_name,
    target_truncate,
):
    lit, expr = _require_pyspark_funcs()
    try:
        load_date = None
        if target_partition_by and target_partition_by.strip().upper() == "SNAPSHOT_DATE":
            load_date = datetime.datetime.now().strftime("%Y%m%d")
            source_df = source_df.withColumn("SNAPSHOT_DATE", lit(load_date))

        if target_truncate and str(target_truncate).upper() == "Y":
            truncate_sql = f"TRUNCATE TABLE {table_name}"
            spark.sql(truncate_sql)
            logger.log(f"All partitions and files for table {table_name} have been deleted.", "INFO")
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")

        if target_table_options:
            pass

        writer = source_df.write.format("iceberg").option("mergeSchema", "true")
        mode = "append" if not target_recreate else "overwrite"
        writer = writer.mode(mode)

        partition_column = None
        partition_expression = None
        if target_partition_by and "=" in str(target_partition_by):
            parts = target_partition_by.split("=", 1)
            partition_column = parts[0].strip()
            partition_expression = parts[1].strip()
            if partition_expression and "SELECT" in partition_expression.upper():
                try:
                    cleaned = partition_expression.replace("(", "").replace(")", "")
                    df_expr = spark.sql(cleaned)
                    colname = df_expr.columns[0]
                    val = df_expr.first()[colname]
                    source_df = source_df.withColumn(partition_column, lit(val))
                except Exception:
                    raise ValueError("Invalid SELECT query format in partition_expression.")
            elif partition_expression:
                source_df = source_df.withColumn(partition_column, expr(partition_expression))
        elif target_partition_by and target_partition_by.strip():
            partition_column = target_partition_by.strip()

        if partition_column:
            writer = writer.partitionBy(partition_column)

        writer.saveAsTable(table_name)
        logger.log(f"DataFrame written to Iceberg table {table_name} successfully.", "INFO")
        return load_date
    except Exception as e:  # pragma: no cover
        logger.log(f"Error writing DataFrame to Iceberg table {table_name}: {e}", "ERROR")
        raise


def full_load_mssql(
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
):
    lit, expr = _require_pyspark_funcs()
    try:
        if target_partition_overwrite and str(target_partition_overwrite).upper() == "Y":
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
        load_date = create_table(
            spark,
            "MSSQL",
            table_name,
            source_df,
            target_partition_by,
            logger,
            None,
            False,
            catalog_name,
            target_truncate,
        )
        return load_date
    except Exception as e:  # pragma: no cover
        logger.log(f"Error during full load for table {table_name}: {str(e)}", "ERROR")
        raise


def incremental_load_mssql(
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
    updated_at,
    cdc_type,
    cdc_modified_date_column,
    cdc_append_key_column,
    catalog_name,
    tbl_schema,
    cdc_tracker_tbl,
    target_truncate,
    target_partition_overwrite,
    query,
):
    lit, expr = _require_pyspark_funcs()
    try:
        if target_partition_overwrite and str(target_partition_overwrite).upper() == "Y":
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
        load_date = create_table(
            spark,
            "MSSQL",
            table_name,
            source_df,
            target_partition_by,
            logger,
            None,
            False,
            catalog_name,
            target_truncate,
        )
        return load_date
    except Exception as e:  # pragma: no cover
        logger.log(f"Error during incremental load for table {table_name}: {str(e)}", "ERROR")
        raise
