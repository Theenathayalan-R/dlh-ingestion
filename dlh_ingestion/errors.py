try:  # avoid hard dependency at import time
    import pyspark  # type: ignore
except Exception:  # pragma: no cover
    pyspark = None  # type: ignore


def classify_error(spark, error_message, logger):
    """Replicated classification logic from legacy script.
    Lazily imports pyspark.sql.* only when invoked to avoid import errors in non-Spark environments.
    """
    try:
        if pyspark is None:
            raise ImportError("PySpark required for classify_error")
        from pyspark.sql.types import StructType, StructField, StringType  # type: ignore
        import pyspark.sql.functions as F  # type: ignore
        error_patterns_data = [
            ("ORA-01017|invalid username/password|Login failed", "CRITICAL", "Invalid Credentials"),
            ("ORA-12541|no listener|connection refused", "CRITICAL", "Connection refused"),
            ("timeout|connection reset|network error", "CRITICAL", "Network Connectivity issue"),
            ("ORA-01031|insufficient privileges", "CRITICAL", "Insufficient permissions"),
            ("ORA-00942|table or view does not exist|table not found", "NON_CRITICAL", "Table or view does not exist"),
            ("ORA-00904|invalid column", "NON_CRITICAL", "Invalid column"),
        ]
        pattern_schema = StructType([
            StructField("pattern", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("description", StringType(), False),
        ])
        patterns_df = spark.createDataFrame(error_patterns_data, pattern_schema)
        error_df = spark.createDataFrame([(error_message,)], StructType([StructField("error_message", StringType(), False)]))
        matched = (
            error_df.crossJoin(patterns_df)
            .withColumn("matches", F.col("error_message").rlike(F.col("pattern")))
            .filter(F.col("matches") == True)
            .select("error_type", "description")
            .first()
        )
        if matched:
            return {"error_type": matched["error_type"], "description": matched["description"]}
        return {"error_type": "NON_CRITICAL", "description": "Error occurred"}
    except Exception as e:  # pragma: no cover
        logger.log(f"Error classifying error message: {str(e)}", "ERROR")
        raise
