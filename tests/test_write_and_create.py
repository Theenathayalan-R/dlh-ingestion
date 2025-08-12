import unittest
from unittest.mock import MagicMock, patch
from dlh_run_db_ingestion import write_with_retry, create_table, CustomLogger


class TestWriteAndCreate(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    @patch("dlh_run_db_ingestion.col")
    def test_write_with_retry_success_after_failure(self, mock_col):
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
        writer.saveAsTable.side_effect = [Exception("first"), None]
        out = write_with_retry(self.spark, df, "cat", "sch", "jt", self.logger, retry_delay=0, max_retries=2)
        self.assertIsNone(out)
        self.assertEqual(writer.saveAsTable.call_count, 2)

    def test_create_table_truncate_and_partition(self):
        df = MagicMock()
        df.columns = ["c1", "c2"]
        writer = MagicMock()
        df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.partitionBy.return_value = writer
        self.spark.sql.return_value = MagicMock()

        load_date = create_table(
            spark=self.spark,
            source_db_type="MSSQL",
            table_name="cat.sch.tbl",
            source_df=df,
            target_partition_by="c1",
            logger=self.logger,
            target_table_options=None,
            target_recreate=False,
            catalog_name="cat",
            target_truncate="Y",
        )
        self.assertTrue(isinstance(load_date, (str, type(None))))
        self.assertTrue(self.spark.sql.called)

    def test_create_table_no_partition_no_truncate(self):
        df = MagicMock()
        df.columns = ["c1", "c2"]
        writer = MagicMock()
        df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer

        self.spark.sql.return_value = MagicMock()

        load_date = create_table(
            spark=self.spark,
            source_db_type="MSSQL",
            table_name="cat.sch.tbl2",
            source_df=df,
            target_partition_by=None,
            logger=self.logger,
            target_table_options=None,
            target_recreate=False,
            catalog_name="cat",
            target_truncate="N",
        )
        self.assertTrue(isinstance(load_date, (str, type(None))))
