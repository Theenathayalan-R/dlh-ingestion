import sys
import os
import datetime
import logging
import json
import re
import string
import time
import threading
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, expr, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark import SparkFiles
from functools import reduce



class CustomLogger:
    def __init__(self, spark, s3_bucket, s3_prefix, app_name):
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.app_name = app_name
        self.logs = []
        self.log_stream = []

        self.today = datetime.datetime.now().strftime("%Y%m%d")

        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def log(self, message, level="INFO"):
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        log_level = level_map.get(level.upper(), logging.INFO)
        self.logger.log(log_level, message)
        self.log_stream.append(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {level} - {message}")

    def get_log_content(self):
        return "\n".join(self.log_stream)

    def save_to_s3(self, log_reference):
        try:
            s3_path = f"{self.s3_bucket}{log_reference}{self.today}"
            self.log(f"s3 path for log loading... {s3_path}", "INFO")
            log_rdd = self.spark.sparkContext.parallelize([self.get_log_content()])
            log_df = self.spark.createDataFrame(log_rdd, "string")
            (
                log_df.coalesce(1)
                .write.format("text")
                .mode("append")
                .option("header", "false")
                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .option("compression", "none")
                .save(s3_path)
            )
            self.log(f"Logs successfully saved to s3 path", "INFO")
            return True
        except Exception as e:
            self.log(f"Failed to save logs to S3 bucket: {str(e)}", "ERROR")
            return False


class JobTracker:
    def __init__(self, spark, batch_id, run_id, spark_app_id, catalog_name, database, job_status_tbl, job_id, table_name, run_group, logger):
        self.spark = spark
        self.batch_id = batch_id
        self.run_id = run_id
        self.spark_app_id = spark_app_id
        self.catalog_name = catalog_name
        self.database = database
        self.job_status_tbl = job_status_tbl
        self.job_id = job_id
        self.table_name = table_name
        self.logger = logger
        self.batch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_group = run_group
        self.start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def insert_initial_status(self):
        try:
            data = [{
                "run_id": self.run_id,
                "batch_id": self.batch_id,
                "spark_app_id": self.spark_app_id,
                "run_group": self.run_group,
                "dlh_layer": "staging",
                "job_id": self.job_id,
                "table_name": self.table_name,
                "schema": "",
                "column_count": 0,
                "status": "STARTED",
                "start_time": self.start_time,
                "end_time": None,
                "records_processed": 0,
                "records_rejected": 0,
                "error_message": "",
                "load_date": "",
                "comments": ""
            }]
            return self.spark.createDataFrame(data)
        except Exception as e:
            self.logger.log(f"Error creating initial job status DataFrame: {str(e)}", "ERROR")
            raise

    def update_status(self, status, column_count, records_processed=None, error_message=None, load_date=None):
        try:
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if status in ["COMPLETED", "FAILED"] else None
            end_time_str = f"TIMESTAMP '{end_time}'" if end_time else None
            records_processed_str = records_processed if records_processed is not None else 0
            error_message_str = error_message.replace("'", "") if error_message else ""
            load_date_str = f"{load_date}" if load_date is not None else ""

            data = [{
                "run_id": self.run_id,
                "batch_id": self.batch_id,
                "spark_app_id": self.spark_app_id,
                "run_group": self.run_group,
                "dlh_layer": "staging",
                "job_id": self.job_id,
                "table_name": self.table_name,
                "schema": "",
                "column_count": column_count,
                "status": status,
                "start_time": self.start_time,
                "end_time": end_time,
                "records_processed": records_processed_str,
                "records_rejected": 0,
                "error_message": error_message_str or "",
                "load_date": load_date_str or "",
                "comments": ""
            }]
            return self.spark.createDataFrame(data)
        except Exception as e:
            self.logger.log(f"Error creating job status update DataFrame: {str(e)}", "ERROR")
            raise


class EmailService:
    def __init__(self, spark, batch_id, env, app_name, run_group, smtp_server, smtp_port, sender_email, success_email, failure_email, s3_bucket, s3_prefix, logger):
        self.spark = spark
        self.batch_id = batch_id
        self.env = env
        self.run_group = run_group
        self.app_name = app_name
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.success_email = success_email
        self.failure_email = failure_email
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.logger = logger
        self.today = datetime.datetime.now().strftime("%Y%m%d")

    def send_email(self, recipient_email, subject, body, attachment_path=None):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = recipient_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            if attachment_path == f"/tmp/batch_{self.batch_id}_tables.xlsx":
                with open(attachment_path, "rb") as attachment:
                    part = MIMEApplication(attachment.read(), Name=os.path.basename(attachment_path))
                    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
                    msg.attach(part)
            else:
                if attachment_path:
                    s3_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/"
                    file_name = f"job_report_{self.batch_id}.csv"
                    fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem
                    path = self.spark._jvm.org.apache.hadoop.fs.Path
                    conf = self.spark._jsc.hadoopConfiguration()
                    s3_filesystem = fs.get(path(s3_path).toUri(), conf)
                    input_stream = s3_filesystem.open(path(f"{attachment_path}"))
                    buffer = self.spark._jvm.org.apache.commons.io.IOUtils.toByteArray(input_stream)
                    attachment = MIMEApplication(bytes(buffer))
                    attachment.add_header('Content-Disposition', f"attachment; filename={file_name}")
                    msg.attach(attachment)

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as smtp_server:
                recipients = recipient_email.split(",")
                smtp_server.sendmail(self.sender_email, recipients, msg.as_string())
                smtp_server.quit()
                self.logger.log(f"Email sent successfully to {recipient_email}", "INFO")
        except smtplib.SMTPException as e:
            self.logger.log(f"Failed to send email: {str(e)}", "ERROR")

    def send_job_run_email(self, job_details):
        s3_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/"
        temp_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/job_report_temp/"
        final_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/job_summary_report_{self.batch_id}.csv"

        subject = f"{self.env}: {self.app_name} - {self.run_group} {job_details['source_schema']}.{job_details['source_table']} Job Run Report - {job_details['status']}"
        body = f"""Hello Team,

You are receiving this mail because you are the listed primary or secondary contact for this process.

Details of the job run are as follows:
Source Schema: {job_details['source_schema']}
Source Table: {job_details['source_table']}
Target Schema: {job_details['target_schema']}
Target Table: {job_details['target_table']}
Batch ID: {job_details['batch_id']}
Run ID: {job_details['run_id']}
Job ID: {job_details['job_id']}
Spark App ID: {job_details['spark_app_id']}
Records Processed: {job_details['records_processed']}
Status: {job_details['status']}
Start Time: {job_details['start_time']}
End Time: {job_details['end_time']}
Elapsed Time: {job_details['elapsed_time']}
Failure Reason: {job_details['error_message'] or 'N/A'}

Thank you.
"""

        try:
            (
                job_details.coalesce(1)
                .write.format("csv")
                .option("header", "true")
                .mode("overwrite")
                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .save(temp_path)
            )

            max_retries = 5
            retry_delay = 5
            attempt = 0

            uri = self.spark._jvm.java.net.URI
            path = self.spark._jvm.org.apache.hadoop.fs.Path
            filesystem = self.spark._jvm.org.apache.hadoop.fs.FileSystem
            s3_filesystem = filesystem.get(uri.create(s3_path), self.spark._jsc.hadoopConfiguration())

            while attempt < max_retries:
                try:
                    status = s3_filesystem.listStatus(path(temp_path))
                    part_file = None
                    for filestatus in status:
                        name = filestatus.getPath().getName()
                        if str(name).startswith("part-"):
                            part_file = filestatus.getPath()
                            break

                    if part_file:
                        source = part_file
                        dest = path(final_path)
                        if s3_filesystem.exists(dest):
                            s3_filesystem.delete(dest, True)
                        s3_filesystem.rename(source, dest)
                        if s3_filesystem.exists(path(temp_path)):
                            s3_filesystem.delete(path(temp_path), True)
                        break
                except Exception as e:
                    attempt += 1
                    if attempt < max_retries:
                        print(f"Rename attempt {attempt} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        print(f"All {max_retries} rename attempts failed.")
                        raise e

            recipient_email = self.success_email if job_details["status"] == "COMPLETED" else self.failure_email
            self.send_email(recipient_email, subject, body)
        except Exception as e:
            self.logger.log(f"Error sending job run email: {str(e)}", "ERROR")

    def send_consolidated_report(self, job_details):
        s3_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/"
        temp_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/job_report_temp/"

        try:
            success_jobs = job_details.filter(job_details.status == "COMPLETED").select("job_id").distinct().count()
            fail_jobs = job_details.filter(job_details.status == "FAILED").select("job_id").distinct().count()
            total_jobs = job_details.select("job_id").distinct().count()
            jobs_status = f"Successful: {success_jobs} Fail: {fail_jobs} Total: {total_jobs}"

            subject = f"{self.env}: {self.app_name} - {self.run_group} Data Ingestion Process {jobs_status} - Summary Report"
            body = f"""Hello Team,

You are receiving this mail because you are the listed primary or secondary contact for this process.

Data Ingestion result for the batch {self.batch_id} listed below:
Success: {success_jobs} table(s),
Fail: {fail_jobs} table(s),
Total: {total_jobs} table(s).

Please check attached summary report.

Thank you.
"""

            self.send_email(recipient_email=self.success_email, subject=subject, body=body, attachment_path=None)
        except Exception as e:
            self.logger.log(f"Error sending consolidated report: {str(e)}", "ERROR")

    def send_batch_start_email(self, batch_id, df):
        try:
            temp_excel_path = f"/tmp/batch_{batch_id}_tables.xlsx"
            (
                df.select("job_id", "source_schema", "source_table", "target_schema", "target_table")
                .write.format("com.crealytics.spark.excel")
                .option("dataAddress", "'Sheet1'!A1")
                .option("useHeader", "true")
                .option("header", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .mode("overwrite")
                .save(temp_excel_path)
            )

            subject = f"{self.env}: {self.app_name} - {self.run_group} Batch {batch_id} Ingestion started - List of Tables"
            body = f"""Hello Team,

The {self.app_name} - {self.run_group} batch ingestion process for batch {batch_id} has started. Please find the attached Excel file containing the list of tables included in this batch.

Thank you.
"""

            self.send_email(
                recipient_email=self.success_email,
                subject=subject,
                body=body,
                attachment_path=temp_excel_path
            )
        except Exception as e:
            self.logger.log(f"Error sending batch start email: {str(e)}", "ERROR")
def read_config_from_iceberg(spark, catalog_name, database, table, app_pipeline, run_group, logger):
    try:
        config_df = spark.sql(f"""
SELECT *
FROM {catalog_name}.{database}.{table}
WHERE app_pipeline = '{app_pipeline}' AND run_group = '{run_group}' AND is_active = 'Y'
""")
        logger.log(f"Successfully read configuration from {catalog_name}.{database}.{table}", "INFO")
        config_list = config_df.collect()
        if not config_list:
            raise ValueError("Configuration table is empty or no matching run_group found")
        return config_list[0]
    except Exception as e:
        logger.log(f"Error reading configuration from {catalog_name}.{database}.{table}: {str(e)}", "ERROR")
        raise


def connect_to_oracle(spark, url, query, user, password, driver, logger):
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
    except Exception as e:
        logger.log(f"Error connecting to Oracle: {str(e)}", "ERROR")
        error_message = str(e)
        raise


def connect_to_mssql(spark, url, query, user, password, driver, encrypt, certificate, logger):
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
    except Exception as e:
        logger.log(f"Error connecting to MSSQL: {str(e)}", "ERROR")
        error_message = str(e)
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
    except Exception as e:
        logger.log(f"Error connecting to MSSQL columns: {str(e)}", "ERROR")
        raise


def sanitize_column_name(col_name, logger):
    try:
        for char in string.punctuation:
            if char != "_":
                col_name = col_name.replace(char, " ")
        col_name = re.sub(r"\s+", " ", col_name).strip()
        col_name = re.sub(r"[^A-Za-z0-9_]", "_", col_name)
        col_name = col_name.replace(" ", "_")
        col_name = re.sub(r"_+", "_", col_name).strip("_")
        return col_name
    except Exception as e:
        logger.log(f"Error sanitizing column name: {str(e)}", "ERROR")
        raise


def map_data_types(spark, data_df, cdc_modified_date_column, cdc_created_date_column, sourcedb_conn, sourcedb_user, sourcedb_pass, sourcedb_driver, sourcedb_tblname, logger):
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
            elif data_type == "VARCHAR2":
                mapped_type = "STRING"
            elif data_type == "DATE":
                mapped_type = "TIMESTAMP"
            elif data_type == "TIMESTAMP":
                mapped_type = "TIMESTAMP"
            elif data_type == "BLOB":
                mapped_type = "BINARY"
            elif data_type == "TIMESTAMP(6) WITH TIME ZONE":
                mapped_type = "TIMESTAMP"
            elif data_type == "TIMESTAMP(6)":
                mapped_type = "TIMESTAMP"
            else:
                mapped_type = "STRING"
            mapped_columns.append(f"{col_name} {mapped_type}")
        return ", ".join(mapped_columns)
    except Exception as e:
        logger.log(f"Error mapping data types: {str(e)}", "ERROR")
        raise


def fetch_data_from_oracle(spark, sourcedb_conn, sourcedb_tblname, sourcedb_user, sourcedb_pass, sourcedb_driver, data_df, cdc_modified_date_column, cdc_created_date_column, source_where_clause, source_fields, logger, source_table):
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
    except Exception as e:
        logger.log(f"Error fetching data from Oracle: {str(e)}", "ERROR")
        raise


def read_from_jdbc(spark, url, query, user, password, driver, schema_string, source_table, dbtable, logger):
    try:
        object_name_query = f"""
SELECT OBJECT_TYPE FROM ALL_OBJECTS
WHERE OBJECT_NAME = '{dbtable.split('.')[-1]}' AND OWNER = '{dbtable.split('.')[0]}'
"""
        object_name_df = (
            spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", object_name_query)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .load()
        )
        OBJECT_TYPE = object_name_df.first()["OBJECT_TYPE"] if not object_name_df.rdd.isEmpty() else None
        if OBJECT_TYPE is None:
            raise ValueError(f"Object type for table '{source_table}' in schema '{dbtable.split('.')[0]}' could not be determined. Please check the table or view existence.")

        if OBJECT_TYPE == "TABLE":
            metadata_query = f"SELECT CAST(NUM_ROWS AS NUMBER) AS NUM_ROWS FROM ALL_TABLES WHERE TABLE_NAME = '{dbtable.split('.')[-1]}' and OWNER = '{dbtable.split('.')[0]}'"
        elif OBJECT_TYPE == "VIEW":
            metadata_query = f"SELECT count(*) as NUM_ROWS FROM {dbtable}"
        else:
            metadata_query = f"SELECT 0 as NUM_ROWS"

        metadata_df = (
            spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", metadata_query)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .load()
        )
        row_count = metadata_df.first()["NUM_ROWS"] or 0

        if row_count == 0:
            fetchsize = 20
        elif row_count <= 100:
            fetchsize = 50
        elif row_count <= 500:
            fetchsize = 100
        elif row_count <= 5000:
            fetchsize = 200
        elif row_count <= 20000:
            fetchsize = 500
        elif row_count <= 50000:
            fetchsize = 1000
        elif row_count <= 100000:
            fetchsize = 5000
        else:
            fetchsize = 10000

        source_df = (
            spark.read
            .format("jdbc")
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
    except Exception as e:
        logger.log(f"Error reading from JDBC: {str(e)}", "ERROR")
        raise
def get_run_id(spark, catalog_name, database, job_status_tbl, batch_id, logger):
    try:
        query = f"""
SELECT run_id
FROM {catalog_name}.{database}.{job_status_tbl}
WHERE batch_id = '{batch_id}'
ORDER BY start_time
"""
        run_id_df = spark.sql(query)
        if run_id_df.count() == 0 or run_id_df.rdd.isEmpty():
            return None
        return run_id_df.first()["run_id"]
    except Exception as e:
        logger.log(f"Error fetching run id: {str(e)}", "ERROR")
        raise


def classify_error(spark, error_message, logger):
    """
    Classify an error message and return error metadata.

    Args:
        spark: SparkSession instance
        error_message: error message to classify

    Returns:
        dict: { 'error_type': ..., 'description': ... }
    """
    try:
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
    except Exception as e:
        logger.log(f"Error classifying error message: {str(e)}", "ERROR")
        raise
def compare_and_notify_schema_changes(
    spark,
    env,
    app_name,
    run_group,
    source_df,
    catalog_name,
    tbl_schema,
    job_status_tbl,
    main_job_status_tbl,
    batch_id,
    job_id,
    source_table,
    email_service,
    logger,
):
    try:
        current_schema = {}
        comments_text = "No schema changes detected."
        return current_schema, comments_text
    except Exception as e:
        logger.log(f"Error comparing and notifying schema changes for job_id: {job_id}: {str(e)}", "ERROR")
        raise


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

        if email_service and str(statusEmails).upper() == "Y":
            email_service.send_job_run_email(job_details)

        updated_status_df = spark.createDataFrame(
            [
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
            ]
        )

        return updated_status_df
    except Exception as e:
        logger.log(f"Error during job completion or failure handling: {str(e)}", "ERROR")
        raise
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
                except Exception as e:
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
    except Exception as e:
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
    except Exception as e:
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
    except Exception as e:
        logger.log(f"Error during incremental load for table {table_name}: {str(e)}", "ERROR")
        raise


def process_table(
    row,
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
        source_where_clause = row.get("source_where_clause")
        cdc_type = row.get("cdc_type")
        cdc_modified_date_column = row.get("cdc_modified_date_column")
        cdc_created_date_column = row.get("cdc_created_date_column")
        cdc_append_key_column = row.get("cdc_append_key_column")
        source_db_type = source_db_config["db_type"]

        updated_status_df = job_tracker.insert_initial_status()

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

            if load_type and load_type.upper() == "INCREMENTAL":
                updated_at_df = spark.sql(
                    f"select max(updated_at) as max_updated_at from {catalog_name}.{tbl_schema}.{cdc_tracker_tbl} "
                    f"where source_table='{source_table}' and job_id='{job_id}'"
                )
                updated_at = None
                if not updated_at_df.rdd.isEmpty():
                    row_upd = updated_at_df.select("max_updated_at").distinct().collect()[0]
                    updated_at = row_upd["max_updated_at"]

                if cdc_type and cdc_type.upper() == "TIMESTAMP":
                    CURR_TS = datetime.datetime.now().strftime("%Y%m%d%H")
                    if updated_at:
                        conds = []
                        if cdc_modified_date_column:
                            conds.append(
                                f"to_char({cdc_modified_date_column}, 'YYYYMMDDHH24') >= {updated_at} AND "
                                f"to_char({cdc_modified_date_column}, 'YYYYMMDDHH24') < {CURR_TS}"
                            )
                        if cdc_created_date_column:
                            conds.append(
                                f"to_char({cdc_created_date_column}, 'YYYYMMDDHH24') >= {updated_at} AND "
                                f"to_char({cdc_created_date_column}, 'YYYYMMDDHH24') < {CURR_TS}"
                            )
                        date_clause = " OR ".join(conds) if conds else None
                    else:
                        CURR_TS = datetime.datetime.now().strftime("%Y%m%d%H")
                        conds = []
                        if cdc_modified_date_column:
                            conds.append(f"to_char({cdc_modified_date_column}, 'YYYYMMDDHH24') < {CURR_TS}")
                        if cdc_created_date_column:
                            conds.append(f"to_char({cdc_created_date_column}, 'YYYYMMDDHH24') < {CURR_TS}")
                        date_clause = " OR ".join(conds) if conds else None

                    if source_fields and source_fields.strip():
                        select_cols = source_fields
                    else:
                        select_cols = "*"

                    where_parts = []
                    if date_clause:
                        where_parts.append(f"({date_clause})")
                    if source_where_clause and source_where_clause.strip():
                        where_parts.append(source_where_clause)
                    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                    query = f"SELECT {select_cols} FROM {dbtable}{where_sql}"

                elif cdc_type and cdc_type.upper() == "APPEND_KEY" and cdc_append_key_column:
                    max_df = spark.read.format("jdbc").option("url", url).option(
                        "query", f"SELECT MAX({cdc_append_key_column}) as MAX_VALUE FROM {dbtable}"
                    ).option("user", user).option("password", dbpass).option("driver", driver).load()
                    max_value = None if max_df.rdd.isEmpty() else max_df.first()["MAX_VALUE"]
                    if updated_at is None:
                        updated_at = max_value

                    if source_fields and source_fields.strip():
                        select_cols = source_fields
                    else:
                        select_cols = "*"

                    where_parts = []
                    if updated_at is not None and max_value is not None:
                        where_parts.append(f"{cdc_append_key_column} > {updated_at} AND {cdc_append_key_column} <= {max_value}")
                    if source_where_clause and source_where_clause.strip():
                        where_parts.append(source_where_clause)
                    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                    query = f"SELECT {select_cols} FROM {dbtable}{where_sql}"
                else:
                    if source_fields and source_fields.strip():
                        select_cols = source_fields
                    else:
                        select_cols = "*"
                    where_sql = f" WHERE {source_where_clause}" if source_where_clause and source_where_clause.strip() else ""
                    query = f"SELECT {select_cols} FROM {dbtable}{where_sql}"

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
                    source_where_clause,
                    source_fields,
                    logger,
                    source_table,
                )
                column_count = len(source_df.columns)
                load_date = create_table(
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
                )
            else:
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
                    source_where_clause,
                    source_fields,
                    logger,
                    source_table,
                )
                column_count = len(source_df.columns)
                load_date = create_table(
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
                )

            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_time = updated_status_df.select("start_time").first()["start_time"]
            updated_status_df = handle_job_completion_or_failure(
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
            return updated_status_df

        elif source_db_type.upper() == "MSSQL":
            url = f"jdbc:sqlserver://{source_db_config['db_host']}.database.windows.net:{source_db_config['db_port']};database={source_db_config['db_name']}"
            user = source_db_config["db_user"]
            driver = source_db_config["driver"]
            encrypt = source_db_config.get("encrypt", "true")
            certificate = source_db_config.get("trustservercertificate", "false")
            dbtable = f"{source_schema}.{source_table}"

            column_query = f"SELECT TOP 100 PERCENT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{dbtable.split('.')[-1]}' AND TABLE_SCHEMA = '{dbtable.split('.')[0]}' ORDER BY ORDINAL_POSITION"
            safe_columns = connect_to_mssql_columns(spark, url, column_query, user, dbpass, driver, encrypt, certificate, logger)
            if not safe_columns:
                logger.log(f"No columns found for the table {dbtable}.", "ERROR")
                raise ValueError(f"No columns found for the table {dbtable}.")

            if load_type and load_type.upper() == "FULL":
                if (not source_where_clause) and (not source_fields):
                    query = f"SELECT {safe_columns} FROM {dbtable}"
                elif (not source_fields) and source_where_clause:
                    query = f"SELECT {safe_columns} FROM {dbtable} WHERE {source_where_clause}"
                elif source_fields and (not source_where_clause):
                    query = f"SELECT {source_fields} FROM {dbtable}"
                else:
                    query = f"SELECT {source_fields} FROM {dbtable} WHERE {source_where_clause}"

                source_df = connect_to_mssql(spark, url, query, user, dbpass, driver, encrypt, certificate, logger)
                column_count = len(source_df.columns)
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
            else:
                updated_at_df = spark.sql(
                    f"select max(updated_at) as max_updated_at from {catalog_name}.{tbl_schema}.{cdc_tracker_tbl} "
                    f"where source_table='{source_table}' and job_id='{job_id}'"
                )
                updated_at = None
                if not updated_at_df.rdd.isEmpty():
                    row_upd = updated_at_df.select("max_updated_at").distinct().collect()[0]
                    updated_at = row_upd["max_updated_at"]

                if cdc_type and cdc_type.upper() == "TIMESTAMP":
                    CURR_TS = datetime.datetime.now().strftime("%Y%m%d%H")
                    where_parts = []
                    if updated_at:
                        conds = []
                        if cdc_modified_date_column:
                            conds.append(f"FORMAT({cdc_modified_date_column}, 'yyyyMMddHH') >= {updated_at} AND FORMAT({cdc_modified_date_column}, 'yyyyMMddHH') < {CURR_TS}")
                        if cdc_created_date_column:
                            conds.append(f"FORMAT({cdc_created_date_column}, 'yyyyMMddHH') >= {updated_at} AND FORMAT({cdc_created_date_column}, 'yyyyMMddHH') < {CURR_TS}")
                        if conds:
                            where_parts.append(f"({' OR '.join(conds)})")
                    else:
                        conds = []
                        if cdc_modified_date_column:
                            conds.append(f"FORMAT({cdc_modified_date_column}, 'yyyyMMddHH') < {CURR_TS}")
                        if cdc_created_date_column:
                            conds.append(f"FORMAT({cdc_created_date_column}, 'yyyyMMddHH') < {CURR_TS}")
                        if conds:
                            where_parts.append(f"({' OR '.join(conds)})")

                    if source_where_clause and source_where_clause.strip():
                        where_parts.append(source_where_clause)
                    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                    select_cols = source_fields if source_fields and source_fields.strip() else safe_columns
                    query = f"SELECT {select_cols} FROM {dbtable}{where_sql}"

                elif cdc_type and cdc_type.upper() == "APPEND_KEY" and cdc_append_key_column:
                    max_df = spark.read.format("jdbc").option("driver", driver).option("url", url).option(
                        "query", f"SELECT MAX({cdc_append_key_column}) as MAX_VALUE FROM {dbtable}"
                    ).option("user", user).option("password", dbpass).option("Encrypt", encrypt).option("TrustServerCertificate", certificate).load()
                    max_value = None if max_df.rdd.isEmpty() else max_df.first()["MAX_VALUE"]
                    if updated_at is None:
                        updated_at = max_value

                    where_parts = []
                    if updated_at is not None and max_value is not None:
                        where_parts.append(f"{cdc_append_key_column} > {updated_at} AND {cdc_append_key_column} <= {max_value}")
                    if source_where_clause and source_where_clause.strip():
                        where_parts.append(source_where_clause)
                    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                    select_cols = source_fields if source_fields and source_fields.strip() else safe_columns
                    query = f"SELECT {select_cols} FROM {dbtable}{where_sql}"
                else:
                    select_cols = source_fields if source_fields and source_fields.strip() else safe_columns
                    where_sql = f" WHERE {source_where_clause}" if source_where_clause and source_where_clause.strip() else ""
                    query = f"SELECT {select_cols} FROM {dbtable}{where_sql}"

                source_df = connect_to_mssql(spark, url, query, user, dbpass, driver, encrypt, certificate, logger)
                column_count = len(source_df.columns)
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
                )

            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_time = updated_status_df.select("start_time").first()["start_time"]
            updated_status_df = handle_job_completion_or_failure(
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
            return updated_status_df
        else:
            raise ValueError(f"Unsupported source_db_type: {source_db_type}")
    except Exception as e:
        logger.log(f"Error in process_table for {row.get('job_id')}: {str(e)}", "ERROR")
        raise
def write_with_retry(spark, df, catalog_name, tbl_schema, job_status_tbl, logger, retry_delay: int = 5, max_retries: int = 50):
    try:
        table_name = f"{catalog_name}.{tbl_schema}.{job_status_tbl}"
        df = df.withColumn("start_time", col("start_time").cast("timestamp")).withColumn("end_time", col("end_time").cast("timestamp"))
        attempt = 0
        while attempt < max_retries:
            try:
                df.coalesce(10).write.format("iceberg").mode("append").saveAsTable(table_name)
                print(f"Data successfully written to {table_name}")
                return
            except Exception as e:
                attempt += 1
                print(f"Attempt {attempt} failed: {str(e)}")
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"All {max_retries} attempts failed. Raising exception.")
                    raise
    except Exception as e:
        logger.log(f"Error writing DataFrame to table {job_status_tbl}: {str(e)}", "ERROR")
        raise


def rerun_failed_jobs(spark, run_group, catalog_name, tbl_schema, job_status_tbl, logger):
    try:
        failed_jobs_df = spark.sql(f"""
SELECT job_id, MAX(batch_id) as batch_id, MAX(spark_app_id) as spark_app_id
FROM {catalog_name}.{tbl_schema}.{job_status_tbl}
WHERE run_group = '{run_group}' AND status = 'FAILED'
GROUP BY job_id
""")
        logger.log(f"Fetched failed jobs for rerun: {failed_jobs_df.count()} jobs found.", "INFO")
        return failed_jobs_df
    except Exception as e:
        logger.log(f"Error fetching failed jobs for rerun: {str(e)}", "ERROR")
        raise


def main():
    try:
        spark = SparkSession.builder.getOrCreate()
        if len(sys.argv) < 4:
            print("Usage: script.py <run_group> <app_pipeline> <env> [rerun]")
            sys.exit(1)

        run_group = sys.argv[1]
        app_pipeline = sys.argv[2]
        app_name = f"{app_pipeline}-{run_group}"
        env = sys.argv[3]
        print(f"run_group: {run_group}")
        print(f"app_pipeline: {app_pipeline}")
        print(f"env: {env}")
        rerun = sys.argv[4] if len(sys.argv) > 4 else None
        print(f"rerun: {rerun}")

        files = os.listdir(SparkFiles.getRootDirectory())
        if files:
            print(f"File-1 added with --files: {files[0]}")
            if len(files) > 1:
                print(f"File-2 added with --files: {files[1]}")

        alert_config = read_config_from_iceberg(spark, "", "", "", run_group, app_pipeline)

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
        alert_tbl = alert_config.get("alert_tbl", "")

        table_name = f"{catalog_name}.{tbl_schema}.{ingest_tbl}"
        print(f"table name: {table_name}")

        if alert_config.get("alert_type", "EMAIL").upper() == "EMAIL":
            email_service = EmailService(
                batch_id=None,
                env=env,
                app_name=app_name_cfg,
                run_group=run_group,
                smtp_server=smtp_server,
                smtp_port=smtp_port,
                sender_email=sender_email,
                success_email=success_email,
                failure_email=failure_email,
                retention_period=retention_period,
                logger=logger,
            )
        else:
            email_service = None

        if rerun and rerun.upper() == "RERUN":
            failed_jobs_df = rerun_failed_jobs(spark, run_group, catalog_name, tbl_schema, job_status_tbl=alert_config.get("job_status_tbl", ""), logger=logger)
            if failed_jobs_df.count() == 0:
                print("No failed jobs found for rerun.")
                return 0

        spark_app_id = spark.sparkContext.applicationId
        print(f"Spark Application id: {spark_app_id}")
        batch_id = datetime.datetime.now().strftime("%Y%m%d%H")
        formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = spark.table(table_name).filter(
            (col("app_pipeline") == app_pipeline)
            & (col("run_group") == run_group)
            & (col("is_enabled") == "Y")
            & (col("is_active") == "Y")
        ).persist()

        source_db_config = alert_config.get("source_db_config", {})
        dbpass = alert_config.get("dbpass", "")

        rows = df.collect()
        if rows is None:
            return 1

        lock = threading.Lock()
        run_id = get_run_id(spark, catalog_name, tbl_schema, alert_config.get("job_status_tbl", ""), batch_id, None, logger)
        logger.log(f"Job initiated for batch id: {batch_id} run_id: {run_id}", "INFO")

        max_workers = alert_config.get("max_workers", 5)
        futures = []
        job_trackers = []
        initial_status_dfs = []

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for row in rows:
                job_tracker = JobTracker(
                    spark=spark,
                    batch_id=batch_id,
                    run_id=run_id,
                    spark_app_id=spark_app_id,
                    catalog_name=catalog_name,
                    database=tbl_schema,
                    job_status_tbl=alert_config.get("job_status_tbl", ""),
                    job_id=row["job_id"],
                    table_name=row["target_table"],
                    run_group=run_group,
                    logger=logger,
                )
                initial_df = job_tracker.insert_initial_status()
                initial_status_dfs.append(initial_df)
                job_trackers.append(job_tracker)
                futures.append(
                    executor.submit(
                        process_table,
                        row=row,
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

        updated_status_dfs = []
        for future in futures:
            updated_status_dfs.append(future.result())

        if initial_status_dfs:
            consolidated_initial = reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), initial_status_dfs)
        else:
            consolidated_initial = spark.createDataFrame([], spark.table(f"{catalog_name}.{tbl_schema}.{alert_config.get('job_status_tbl', '')}").schema)

        if updated_status_dfs:
            consolidated_updated = reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), updated_status_dfs)
        else:
            consolidated_updated = spark.createDataFrame([], consolidated_initial.schema)

        final_status_df = consolidated_initial.alias("initial").join(
            consolidated_updated.alias("updated"),
            on="job_id",
            how="outer",
        ).select(
            F.coalesce(col("updated.batch_id"), col("initial.batch_id")).alias("batch_id"),
            F.coalesce(col("updated.run_id"), col("initial.run_id")).alias("run_id"),
            F.coalesce(col("updated.spark_app_id"), col("initial.spark_app_id")).alias("spark_app_id"),
            F.coalesce(col("updated.run_group"), col("initial.run_group")).alias("run_group"),
            F.coalesce(col("updated.dlh_layer"), col("initial.dlh_layer")).alias("dlh_layer"),
            F.coalesce(col("updated.job_id"), col("initial.job_id")).alias("job_id"),
            F.coalesce(col("updated.table_name"), col("initial.table_name")).alias("table_name"),
            F.coalesce(col("updated.status"), col("initial.status")).alias("status"),
            F.coalesce(col("updated.start_time"), col("initial.start_time")).alias("start_time"),
            F.coalesce(col("updated.end_time"), col("initial.end_time")).alias("end_time"),
            F.coalesce(col("updated.column_count"), col("initial.column_count")).alias("column_count"),
            F.coalesce(col("updated.records_processed"), col("initial.records_processed")).alias("records_processed"),
            F.coalesce(col("updated.records_rejected"), col("initial.records_rejected")).alias("records_rejected"),
            F.coalesce(col("updated.error_message"), col("initial.error_message")).alias("error_message"),
            F.coalesce(col("updated.load_date"), col("initial.load_date")).alias("load_date"),
            F.coalesce(col("updated.comments"), col("initial.comments")).alias("comments"),
            F.coalesce(col("updated.schema"), col("initial.schema")).alias("schema"),
        )

        try:
            write_with_retry(spark, final_status_df, catalog_name, tbl_schema, alert_config.get("job_status_tbl", ""), logger)
        except Exception as e:
            logger.log(f"Write operation failed: {str(e)}", "ERROR")

        logger.save_to_s3(alert_config.get("log_reference", "/log_reference/"))

        return 0
    except Exception as e:
        error_message = f"Connection failed: {str(e).replace('\\n', ' ')}"
        print(f"Connection failed: {str(e)}")
        if "logger" in locals():
            logger.log(error_message, "ERROR")
            logger.save_to_s3(alert_config.get("log_reference", "/log_reference/"))
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    exit_code = main()
    print(f"exit_code in main: {exit_code}")
    sys.exit(exit_code)
