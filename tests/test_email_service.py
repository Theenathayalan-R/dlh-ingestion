import unittest
from unittest.mock import MagicMock, patch, mock_open
from dlh_ingestion import EmailService, CustomLogger, JobRunReport


class TestEmailService(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")
        self.email_service = EmailService(
            spark=self.spark,
            batch_id="b1",
            env="dev",
            app_name="app",
            run_group="daily",
            smtp_server="smtp.example.com",
            smtp_port=25,
            sender_email="sender@example.com",
            success_email="ok@example.com",
            failure_email="ko@example.com",
            s3_bucket="s3a://bucket/",
            s3_prefix="prefix/",
            logger=self.logger
        )

    @patch("smtplib.SMTP")
    def test_send_email_simple(self, MockSMTP):
        smtp = MockSMTP.return_value.__enter__.return_value
        self.email_service.send_email("a@b.com,c@d.com", "Sub", "Body")
        smtp.sendmail.assert_called_once()
        args, kwargs = smtp.sendmail.call_args
        self.assertEqual(args[0], "sender@example.com")
        self.assertEqual(args[1], ["a@b.com", "c@d.com"])
        self.assertIn("Sub", args[2])

    @patch("builtins.open", new_callable=mock_open, read_data=b"DATA")
    @patch("smtplib.SMTP")
    def test_send_email_with_local_attachment(self, MockSMTP, mopen):
        path = "/tmp/batch_b1_tables.xlsx"
        self.email_service.send_email("a@b.com", "Sub", "Body", attachment_path=path)
        mopen.assert_called_once_with(path, "rb")
        smtp = MockSMTP.return_value.__enter__.return_value
        smtp.sendmail.assert_called_once()

    def test_send_job_run_email_completed(self):
        # Build spark stubs for createDataFrame chain and Hadoop FS interaction
        df = MagicMock(); df.coalesce.return_value = df; write = df.write; write.format.return_value = write; write.option.return_value = write; write.mode.return_value = write
        self.spark.createDataFrame.return_value = df
        # _jvm + filesystem stubs
        class StatObj:
            def __init__(self):
                self._path = MagicMock()
                self._path.getName.return_value = "part-0000"
            def getPath(self):
                return self._path
        s3_fs = MagicMock()
        s3_fs.listStatus.return_value = [StatObj()]
        s3_fs.exists.return_value = False
        s3_fs.rename.return_value = True
        class URICls:
            @staticmethod
            def create(x):
                return x
        class FSNS:
            Path = lambda p: p  # noqa: E731
            FileSystem = MagicMock(get=MagicMock(return_value=s3_fs))
        class ORG:
            class apache:
                class hadoop:
                    fs = FSNS
        self.spark._jvm.java.net.URI = URICls
        self.spark._jvm.org.apache.hadoop.fs = FSNS
        self.spark._jsc.hadoopConfiguration.return_value = {}
        # Patch send_email to avoid SMTP
        with patch.object(self.email_service, "send_email") as send_email_mock:
            jr = JobRunReport(
                job_id="J1", source_schema="S", source_table="T", target_schema="TS", target_table="TT",
                batch_id="b1", run_id="r1", spark_app_id="sa1", records_processed=10, status="COMPLETED",
                start_time="2025-01-01 00:00:00", end_time="2025-01-01 00:05:00", elapsed_time="0:05:00",
                error_message=None, load_date="20250101"
            )
            self.email_service.send_job_run_email(jr)
            send_email_mock.assert_called_once()
            args, _ = send_email_mock.call_args
            self.assertIn("Job Run Report", args[1])
