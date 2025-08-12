import unittest
from unittest.mock import MagicMock, patch, mock_open
from dlh_run_db_ingestion import EmailService, CustomLogger


@unittest.skip("Skipping email tests per user request")
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
