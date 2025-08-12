import unittest
from unittest.mock import MagicMock, patch

from python_dlh_ingestion_run_db_job import CustomLogger, SparkSession


class TestCustomLoggerExtended(unittest.TestCase):
    @patch('python_dlh_ingestion_run_db_job.SparkSession')
    def setUp(self, MockSparkSession):
        self.spark = MockSparkSession.builder.getOrCreate()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    def test_log_levels_and_content(self):
        self.logger.log("info msg", "INFO")
        self.logger.log("warn msg", "WARNING")
        self.logger.log("error msg", "ERROR")
        content = self.logger.get_log_content()
        self.assertIn("info msg", content)
        self.assertIn("warn msg", content)
        self.assertIn("error msg", content)

    def test_save_to_s3_success(self):
        log_rdd = MagicMock()
        log_df = MagicMock()
        self.spark.sparkContext.parallelize.return_value = log_rdd
        self.spark.createDataFrame.return_value = log_df
        writer = MagicMock()
        log_df.coalesce.return_value = log_df
        log_df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.save.return_value = None

        ok = self.logger.save_to_s3("/logref/")
        self.assertTrue(ok)
        writer.save.assert_called_once()

    def test_save_to_s3_failure(self):
        self.spark.sparkContext.parallelize.side_effect = Exception("Boom")
        ok = self.logger.save_to_s3("/logref/")
        self.assertFalse(ok)
