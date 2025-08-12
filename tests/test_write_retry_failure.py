import unittest
from unittest.mock import MagicMock
from dlh_run_db_ingestion import write_with_retry, CustomLogger


class TestWriteRetryFailure(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    def test_write_with_retry_exhausts(self):
        df = MagicMock()
        df.withColumn.return_value = df
        df.coalesce.return_value = df
        writer = MagicMock()
        df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.saveAsTable.side_effect = Exception("always fails")
        with self.assertRaises(Exception):
            write_with_retry(self.spark, df, "cat", "sch", "jt", self.logger, retry_delay=0, max_retries=2)
        self.assertEqual(writer.saveAsTable.call_count, 2)
