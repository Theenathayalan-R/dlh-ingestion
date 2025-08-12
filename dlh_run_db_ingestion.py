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
