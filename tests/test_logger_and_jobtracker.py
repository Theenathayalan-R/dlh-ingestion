import unittest
from unittest.mock import MagicMock, patch
from dlh_ingestion import CustomLogger, JobTracker  # switched imports


class TestCustomLogger(unittest.TestCase):
    @patch('dlh_ingestion.logging_utils.CustomLogger.save_to_s3')
    def setUp(self, mock_save):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://test-bucket", "test-prefix", "test-app")

    def test_log(self):
        self.logger.log("Test message", "INFO")
        self.assertIn("Test message", self.logger.get_log_content())

    @patch('dlh_ingestion.logging_utils.CustomLogger.save_to_s3')
    def test_save_to_s3(self, mock_save_to_s3):
        mock_save_to_s3.return_value = True
        result = self.logger.save_to_s3("test-log-reference")
        self.assertTrue(result)


class TestJobTracker(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://test-bucket", "test-prefix", "test-app")
        self.lock = MagicMock()
        self.job_tracker = JobTracker(
            spark=self.spark,
            batch_id="test_batch_id",
            run_id="test_run_id",
            spark_app_id="test_spark_app_id",
            catalog_name="test_catalog",
            database="test_db",
            job_status_tbl="test_job_status_tbl",
            job_id="test_job_id",
            table_name="test_table_name",
            run_group="test_run_group",
            logger=self.logger
        )

    def test_insert_initial_status(self):
        with patch.object(self.spark, "createDataFrame") as mock_create_df:
            mock_create_df.return_value = MagicMock()
            self.job_tracker.insert_initial_status()
            mock_create_df.assert_called_once()

    def test_update_status(self):
        with patch.object(self.spark, "createDataFrame") as mock_create_df:
            mock_create_df.return_value = MagicMock()
            self.job_tracker.update_status("COMPLETED", 100, "No errors")
            mock_create_df.assert_called_once()


if __name__ == "__main__":
    unittest.main()
