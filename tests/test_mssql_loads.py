import unittest
from unittest.mock import MagicMock, patch
from dlh_ingestion import full_load_mssql, incremental_load_mssql, CustomLogger  # switched to modular package


class TestMSSQLLoads(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    @patch("dlh_ingestion.loaders.create_table")
    def test_full_load_mssql_success(self, create_table_fn):
        create_table_fn.return_value = "20250101"
        df = MagicMock()
        out = full_load_mssql(
            spark=self.spark,
            source_df=df,
            table_name="cat.sch.tbl",
            target_partition_by="p1",
            logger=self.logger,
            job_id="J1",
            app_name="app",
            run_group="rg",
            is_enabled="Y",
            source_schema="dbo",
            source_table="T",
            catalog_name="cat",
            tbl_schema="sch",
            cdc_tracker_tbl="cdc_tbl",
            target_truncate="N",
            target_partition_overwrite="N",
            query="SELECT * FROM dbo.T",
        )
        self.assertEqual(out, "20250101")
        self.assertFalse(self.spark.sql.called)

    @patch("dlh_ingestion.loaders.create_table")
    def test_full_load_mssql_with_partition_overwrite(self, create_table_fn):
        create_table_fn.return_value = "20250101"
        df = MagicMock()
        out = full_load_mssql(
            spark=self.spark,
            source_df=df,
            table_name="cat.sch.tbl",
            target_partition_by=None,
            logger=self.logger,
            job_id="J1",
            app_name="app",
            run_group="rg",
            is_enabled="Y",
            source_schema="dbo",
            source_table="T",
            catalog_name="cat",
            tbl_schema="sch",
            cdc_tracker_tbl="cdc_tbl",
            target_truncate="Y",
            target_partition_overwrite="Y",
            query="SELECT * FROM dbo.T",
        )
        self.assertEqual(out, "20250101")
        self.spark.sql.assert_called_once()

    @patch("dlh_ingestion.loaders.create_table")
    def test_incremental_load_mssql_success(self, create_table_fn):
        create_table_fn.return_value = "20250102"
        df = MagicMock()
        out = incremental_load_mssql(
            spark=self.spark,
            source_df=df,
            table_name="cat.sch.tbl",
            target_partition_by=None,
            logger=self.logger,
            lock=MagicMock(),
            job_id="J1",
            app_name="app",
            run_group="rg",
            is_enabled="Y",
            source_schema="dbo",
            source_table="T",
            updated_at="20250101",
            cdc_type="TIMESTAMP",
            cdc_modified_date_column="mod",
            cdc_append_key_column="id",
            catalog_name="cat",
            tbl_schema="sch",
            cdc_tracker_tbl="cdc_tbl",
            target_truncate="N",
            target_partition_overwrite="N",
            query="SELECT * FROM dbo.T WHERE 1=1",
        )
        self.assertEqual(out, "20250102")

    @patch("dlh_ingestion.loaders.create_table", side_effect=Exception("boom"))
    def test_full_load_mssql_error_path(self, create_table_fn):
        df = MagicMock()
        with self.assertRaises(Exception):
            full_load_mssql(
                spark=self.spark,
                source_df=df,
                table_name="cat.sch.tbl",
                target_partition_by=None,
                logger=self.logger,
                job_id="J1",
                app_name="app",
                run_group="rg",
                is_enabled="Y",
                source_schema="dbo",
                source_table="T",
                catalog_name="cat",
                tbl_schema="sch",
                cdc_tracker_tbl="cdc_tbl",
                target_truncate="N",
                target_partition_overwrite="N",
                query="SELECT 1",
            )

    @patch("dlh_ingestion.loaders.create_table", side_effect=Exception("boom"))
    def test_incremental_load_mssql_error_path(self, create_table_fn):
        df = MagicMock()
        with self.assertRaises(Exception):
            incremental_load_mssql(
                spark=self.spark,
                source_df=df,
                table_name="cat.sch.tbl",
                target_partition_by=None,
                logger=self.logger,
                lock=MagicMock(),
                job_id="J1",
                app_name="app",
                run_group="rg",
                is_enabled="Y",
                source_schema="dbo",
                source_table="T",
                updated_at=None,
                cdc_type="APPEND_KEY",
                cdc_modified_date_column=None,
                cdc_append_key_column="id",
                catalog_name="cat",
                tbl_schema="sch",
                cdc_tracker_tbl="cdc_tbl",
                target_truncate="N",
                target_partition_overwrite="N",
                query="SELECT * FROM dbo.T",
            )
