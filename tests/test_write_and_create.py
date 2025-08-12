import unittest
from unittest.mock import MagicMock, patch
from dlh_run_db_ingestion import create_table, write_with_retry, CustomLogger


class TestCreateAndWrite(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    def test_create_table_basic_append(self):
        df = MagicMock()
        df.columns = ["a", "b"]
        writer = MagicMock()
        df.write = writer
        df.withColumn.return_value = df
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        writer.saveAsTable.return_value = None

        load_date = create_table(
            spark=self.spark,
            source_db_type="MSSQL",
            table_name="cat.schema.table",
            source_df=df,
            target_partition_by=None,
            logger=self.logger,
            target_table_options=None,
            target_recreate=False,
            catalog_name="cat",
            target_truncate=False,
        )
        self.assertIsNone(load_date)
        writer.saveAsTable.assert_called_once()

    def test_create_table_with_partition_literal(self):
        df = MagicMock()
        df.columns = ["a"]
        writer = MagicMock()
        df.write = writer
        df.withColumn.return_value = df
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        writer.saveAsTable.return_value = None

        load_date = create_table(
            spark=self.spark,
            source_db_type="MSSQL",
            table_name="cat.schema.table",
            source_df=df,
            target_partition_by="SNAPSHOT_DATE",
            logger=self.logger,
            target_table_options=None,
            target_recreate=False,
            catalog_name="cat",
            target_truncate=False,
        )
        self.assertTrue(isinstance(load_date, str) and len(load_date) == 8)
        df.withColumn.assert_called()

    def test_write_with_retry_success_then_failures(self):
        df = MagicMock()
        df.withColumn.return_value = df
        df.coalesce.return_value = df
        writer = MagicMock()
        df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer

        calls = {"i": 0}

        def side_effect(name):
            if calls["i"] == 0:
                calls["i"] += 1
                raise Exception("temp")
            return None

        writer.saveAsTable.side_effect = side_effect

        write_with_retry(self.spark, df, "cat", "schema", "job_tbl", self.logger, retry_delay=0, max_retries=3)
        self.assertEqual(writer.saveAsTable.call_count, 2)
