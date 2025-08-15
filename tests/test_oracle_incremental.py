import unittest
from unittest.mock import MagicMock, patch
from dlh_ingestion import process_table, JobTracker


class TestOracleIncremental(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = MagicMock()
        self.email = MagicMock()
        self.job_tracker = JobTracker(
            spark=self.spark,
            batch_id="b1",
            run_id="r1",
            spark_app_id="sa1",
            catalog_name="cat",
            database="db",
            job_status_tbl="job_tbl",
            job_id="J1",
            table_name="t",
            run_group="rg",
            logger=self.logger
        )

    @patch("dlh_ingestion.connect_to_oracle")
    @patch("dlh_ingestion.fetch_data_from_oracle")
    @patch("dlh_ingestion.create_table")
    @patch.object(JobTracker, "insert_initial_status")
    @patch("dlh_ingestion.jdbc_adaptive.read_from_jdbc")
    def test_oracle_incremental_timestamp_with_prev_updated_at(self, read_jdbc, ins_init, create_table_fn, fetch_oracle, conn_oracle):
        mock_status = MagicMock()
        select_mock = MagicMock()
        first_mock = MagicMock()
        first_mock.__getitem__.return_value = "2025-08-15 13:00:00"
        select_mock.first.return_value = first_mock
        mock_status.select.return_value = select_mock
        ins_init.return_value = mock_status
        df_meta = MagicMock()
        conn_oracle.return_value = df_meta
        df_data = MagicMock()
        df_data.columns = ["A"]
        fetch_oracle.return_value = df_data
        create_table_fn.return_value = "20250101"
        read_jdbc.return_value = df_data
        rdd_mock = MagicMock()
        rdd_mock.isEmpty.return_value = False
        sel_df = MagicMock()
        sel_df.distinct.return_value = sel_df
        sel_df.collect.return_value = [{"max_updated_at": "2024123112"}]
        upd_df = MagicMock()
        upd_df.rdd = rdd_mock
        upd_df.select.return_value = sel_df
        self.spark.sql.return_value = upd_df
        row = {
            "job_id": "J1",
            "source_schema": "S",
            "source_table": "T",
            "target_schema": "ts",
            "target_table": "tt",
            "app_pipeline": "app",
            "run_group": "rg",
            "is_enabled": "Y",
            "load_type": "INCREMENTAL",
            "cdc_type": "TIMESTAMP",
            "cdc_modified_date_column": "MOD_TS",
            "cdc_created_date_column": "CRT_TS",
            "source_where_clause": "X=1",
            "source_fields": "A,B",
            "target_partition_by": None,
            "target_truncate": "N",
        }
        with patch("dlh_ingestion.handle_job_completion_or_failure") as mock_handle:
            mock_handle.return_value = MagicMock()
            out = process_table(
                row=row,
                spark=self.spark,
                env="dev",
                batch_id="b1",
                spark_app_id="sa1",
                catalog_name="cat",
                tbl_schema="sch",
                job_status_tbl="job_tbl",
                cdc_tracker_tbl="cdc_tbl",
                s3_bucket="s3a://bucket/",
                source_db_config={"db_type": "ORACLE", "url": "u", "db_user": "du", "driver": "dd"},
                dbpass="p",
                email_service=self.email,
                logger=self.logger,
                lock=MagicMock(),
                formatted_date="20250101",
                statusEmails="N",
                job_tracker=self.job_tracker,
                run_id="r1",
            )
        self.assertTrue(out)

    @patch("dlh_ingestion.connect_to_oracle")
    @patch("dlh_ingestion.fetch_data_from_oracle")
    @patch("dlh_ingestion.create_table")
    @patch.object(JobTracker, "insert_initial_status")
    @patch("dlh_ingestion.jdbc_adaptive.read_from_jdbc")
    def test_oracle_incremental_append_key(self, read_jdbc, ins_init, create_table_fn, fetch_oracle, conn_oracle):
        # configure start_time chain to return a concrete timestamp string
        mock_status = MagicMock()
        select_mock = MagicMock()
        first_mock = MagicMock()
        first_mock.__getitem__.return_value = "2025-08-15 14:00:00"
        select_mock.first.return_value = first_mock
        mock_status.select.return_value = select_mock
        ins_init.return_value = mock_status
        df_meta = MagicMock()
        conn_oracle.return_value = df_meta
        df_data = MagicMock()
        df_data.columns = ["A"]
        fetch_oracle.return_value = df_data
        create_table_fn.return_value = "20250102"
        read_jdbc.return_value = df_data  # bypass internal object type logic
        upd_df = MagicMock()
        upd_df.rdd.isEmpty.return_value = True
        self.spark.sql.return_value = upd_df
        row = {
            "job_id": "J2",
            "source_schema": "S",
            "source_table": "T",
            "target_schema": "ts",
            "target_table": "tt",
            "app_pipeline": "app",
            "run_group": "rg",
            "is_enabled": "Y",
            "load_type": "INCREMENTAL",
            "cdc_type": "APPEND_KEY",
            "cdc_append_key_column": "ID",
            "target_partition_by": None,
            "target_truncate": "N",
        }
        with patch("dlh_ingestion.handle_job_completion_or_failure") as mock_handle:
            mock_handle.return_value = MagicMock()
            out = process_table(
                row=row,
                spark=self.spark,
                env="dev",
                batch_id="b2",
                spark_app_id="sa1",
                catalog_name="cat",
                tbl_schema="sch",
                job_status_tbl="job_tbl",
                cdc_tracker_tbl="cdc_tbl",
                s3_bucket="s3a://bucket/",
                source_db_config={"db_type": "ORACLE", "url": "u", "db_user": "du", "driver": "dd"},
                dbpass="p",
                email_service=self.email,
                logger=self.logger,
                lock=MagicMock(),
                formatted_date="20250101",
                statusEmails="N",
                job_tracker=self.job_tracker,
                run_id="r2",
            )
        self.assertTrue(out)
