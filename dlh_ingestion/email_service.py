import os
import time
import datetime
import smtplib
from typing import Optional, List, cast
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# Re-establish proper JobRunReport definition with fallback when pydantic absent
try:
    from pydantic import BaseModel  # type: ignore
    class JobRunReport(BaseModel):  # noqa: D401
        job_id: str
        source_schema: str
        source_table: str
        target_schema: str
        target_table: str
        batch_id: str
        run_id: str
        spark_app_id: str
        records_processed: int
        status: str
        start_time: str
        end_time: str
        elapsed_time: str
        error_message: Optional[str] = None
        load_date: Optional[str] = None
except Exception:  # pragma: no cover - fallback path
    class JobRunReport:  # type: ignore
        def __init__(self, **data):
            for k, v in data.items():
                setattr(self, k, v)
        def model_dump(self):
            return self.__dict__


class EmailService:
    """Modular Email Service (logic mirrors legacy implementation)."""

    def __init__(self, spark, batch_id, env, app_name, run_group, smtp_server, smtp_port,
                 sender_email, success_email, failure_email, s3_bucket, s3_prefix, logger):
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

    # ---------------- Internal helpers -----------------
    def _build_message(self, recipient_email: str, subject: str, body: str):
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        return msg

    def _attach_local_file(self, msg, path: str):
        with open(path, "rb") as attachment:
            part = MIMEApplication(attachment.read(), Name=os.path.basename(path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(path)}"'
            msg.attach(part)

    def _attach_remote_file(self, msg, remote_path: str, file_name: str):
        # remote_path is full S3 path of actual object (legacy code passes part path)
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem
        path = self.spark._jvm.org.apache.hadoop.fs.Path
        conf = self.spark._jsc.hadoopConfiguration()
        s3_filesystem = fs.get(path(remote_path).toUri(), conf)
        input_stream = s3_filesystem.open(path(remote_path))
        buffer = self.spark._jvm.org.apache.commons.io.IOUtils.toByteArray(input_stream)
        attachment = MIMEApplication(bytes(buffer))
        attachment.add_header('Content-Disposition', f"attachment; filename={file_name}")
        msg.attach(attachment)

    # ---------------- Public API -----------------
    def send_email(self, recipient_email: str, subject: str, body: str, attachment_path: Optional[str] = None):
        try:
            msg = self._build_message(recipient_email, subject, body)
            if attachment_path == f"/tmp/batch_{self.batch_id}_tables.xlsx":
                self._attach_local_file(msg, cast(str, attachment_path))
            elif attachment_path:
                file_name = f"job_report_{self.batch_id}.csv"
                self._attach_remote_file(msg, attachment_path, file_name)

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as smtp_server:
                recipients: List[str] = recipient_email.split(",")
                smtp_server.sendmail(self.sender_email, recipients, msg.as_string())
                smtp_server.quit()
                self.logger.log(f"Email sent successfully to {recipient_email}", "INFO")
        except smtplib.SMTPException as e:
            self.logger.log(f"Failed to send email: {str(e)}", "ERROR")

    def send_job_run_email(self, job_details: JobRunReport):
        # Convert to dict-compatible for old template
        jd = job_details.model_dump()
        subject = f"{self.env}: {self.app_name} - {self.run_group} {jd['source_schema']}.{jd['source_table']} Job Run Report - {jd['status']}"
        body = f"""Hello Team,

You are receiving this mail because you are the listed primary or secondary contact for this process.

Details of the job run are as follows:
Source Schema: {jd['source_schema']}
Source Table: {jd['source_table']}
Target Schema: {jd['target_schema']}
Target Table: {jd['target_table']}
Batch ID: {jd['batch_id']}
Run ID: {jd['run_id']}
Job ID: {jd['job_id']}
Spark App ID: {jd['spark_app_id']}
Records Processed: {jd['records_processed']}
Status: {jd['status']}
Start Time: {jd['start_time']}
End Time: {jd['end_time']}
Elapsed Time: {jd['elapsed_time']}
Failure Reason: {jd['error_message'] or 'N/A'}

Thank you.
"""
        try:
            temp_path = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/job_report_temp/"
            s3_root = f"{self.s3_bucket}{self.s3_prefix}{self.app_name}/{self.today}/SummaryReport/{self.batch_id}/"
            final_path = f"{s3_root}job_summary_report_{self.batch_id}.csv"

            df = self.spark.createDataFrame([jd])
            (df.coalesce(1)
               .write.format("csv")
               .option("header", "true")
               .mode("overwrite")
               .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
               .save(temp_path))

            uri = self.spark._jvm.java.net.URI
            path = self.spark._jvm.org.apache.hadoop.fs.Path
            filesystem = self.spark._jvm.org.apache.hadoop.fs.FileSystem
            s3_fs = filesystem.get(uri.create(s3_root), self.spark._jsc.hadoopConfiguration())
            max_retries = 5
            delay = 5
            for attempt in range(max_retries):
                try:
                    status = s3_fs.listStatus(path(temp_path))
                    part_file = None
                    for stat in status:
                        name = stat.getPath().getName()
                        if str(name).startswith("part-"):
                            part_file = stat.getPath()
                            break
                    if part_file:
                        dest = path(final_path)
                        if s3_fs.exists(dest):
                            s3_fs.delete(dest, True)
                        s3_fs.rename(part_file, dest)
                        if s3_fs.exists(path(temp_path)):
                            s3_fs.delete(path(temp_path), True)
                        break
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                    else:
                        raise e

            recipient = self.success_email if jd["status"] == "COMPLETED" else self.failure_email
            self.send_email(recipient, subject, body)
        except Exception as e:
            self.logger.log(f"Error sending job run email: {str(e)}", "ERROR")

    def send_consolidated_report(self, job_details_df):
        try:
            success_jobs = job_details_df.filter(job_details_df.status == "COMPLETED").select("job_id").distinct().count()
            fail_jobs = job_details_df.filter(job_details_df.status == "FAILED").select("job_id").distinct().count()
            total_jobs = job_details_df.select("job_id").distinct().count()
            jobs_status = f"Successful: {success_jobs} Fail: {fail_jobs} Total: {total_jobs}"
            subject = f"{self.env}: {self.app_name} - {self.run_group} Data Ingestion Process {jobs_status} - Summary Report"
            body = f"""Hello Team,
\nYou are receiving this mail because you are the listed primary or secondary contact for this process.\n\nData Ingestion result for the batch {self.batch_id} listed below:\nSuccess: {success_jobs} table(s),\nFail: {fail_jobs} table(s),\nTotal: {total_jobs} table(s).\n\nPlease check attached summary report.\n\nThank you.\n"""
            self.send_email(self.success_email, subject, body, attachment_path=None)
        except Exception as e:
            self.logger.log(f"Error sending consolidated report: {str(e)}", "ERROR")

    def send_batch_start_email(self, batch_id, df):
        try:
            temp_excel_path = f"/tmp/batch_{batch_id}_tables.xlsx"
            (df.select("job_id", "source_schema", "source_table", "target_schema", "target_table")
               .write.format("com.crealytics.spark.excel")
               .option("dataAddress", "'Sheet1'!A1")
               .option("useHeader", "true")
               .option("header", "true")
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
               .mode("overwrite")
               .save(temp_excel_path))
            subject = f"{self.env}: {self.app_name} - {self.run_group} Batch {batch_id} Ingestion started - List of Tables"
            body = ("Hello Team,\n\n" +
                    f"The {self.app_name} - {self.run_group} batch ingestion process for batch {batch_id} has started. "
                    "Please find the attached Excel file containing the list of tables included in this batch.\n\nThank you.\n")
            self.send_email(self.success_email, subject, body, attachment_path=temp_excel_path)
        except Exception as e:
            self.logger.log(f"Error sending batch start email: {str(e)}", "ERROR")
