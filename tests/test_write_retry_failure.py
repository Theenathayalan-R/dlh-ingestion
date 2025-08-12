import unittest
from unittest.mock import MagicMock, patch
from dlh_run_db_ingestion import write_with_retry, CustomLogger


class TestWriteRetryFailure(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    @patch("dlh_run_db_ingestion.col")
    def test_write_with_retry_exhausts(self, mock_col):
        m = MagicMock()
        m.cast.return_value = m
        mock_col.return_value = m
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
