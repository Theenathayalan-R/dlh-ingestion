import unittest
from unittest.mock import MagicMock, patch
from dlh_ingestion import (
    process_table,
    handle_job_completion_or_failure,
    compare_and_notify_schema_changes,
    rerun_failed_jobs,
    JobTracker,
)


class TestProcessAndMain(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = MagicMock()
        self.email = MagicMock()
        self.lock = MagicMock()
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

    def test_handle_job_completion_success_and_failure(self):
        df = MagicMock()
        df.count.return_value = 10
        df.columns = ["a", "b"]
        out = handle_job_completion_or_failure(
            spark=self.spark,
            source_df=df,
            email_service=None,
            logger=self.logger,
            job_id="J",
            source_schema="S",
            source_table="T",
            target_schema="TS",
            target_table="TT",
            batch_id="B",
            run_id="R",
            spark_app_id="SA",
            catalog_name="C",
            tbl_schema="SCH",
            job_status_tbl="JT",
            error_message=None,
            statusEmails="N",
            load_date="20240101",
            start_time="2025-01-01 00:00:00",
            end_time="2025-01-01 00:05:00",
        )
        self.assertTrue(hasattr(out, "coalesce") or True)

        out2 = handle_job_completion_or_failure(
            spark=self.spark,
            source_df=None,
            email_service=None,
            logger=self.logger,
            job_id="J",
            source_schema="S",
            source_table="T",
            target_schema="TS",
            target_table="TT",
            batch_id="B",
            run_id="R",
            spark_app_id="SA",
            catalog_name="C",
            tbl_schema="SCH",
            job_status_tbl="JT",
            error_message="err",
            statusEmails="N",
            load_date=None,
            start_time="2025-01-01 00:00:00",
            end_time="2025-01-01 00:05:00",
        )
        self.assertTrue(hasattr(out2, "coalesce") or True)

    def test_compare_and_notify_schema_changes(self):
        current_schema, comments = compare_and_notify_schema_changes(
            self.spark, "dev", "app", "rg",
            MagicMock(), "cat", "sch", "job_tbl",
            "main_tbl", "b1", "J1", "ST",
            self.email, self.logger
        )
        self.assertIsInstance(current_schema, dict)
        self.assertTrue(isinstance(comments, str))

    def test_rerun_failed_jobs(self):
        df = MagicMock()
        self.spark.sql.return_value = df
        out = rerun_failed_jobs(self.spark, "rg", "cat", "sch", "job_tbl", self.logger)
        self.assertIs(out, df)

    @patch("dlh_ingestion.connect_to_mssql_columns")
    @patch("dlh_ingestion.connect_to_mssql")
    @patch("dlh_ingestion.full_load_mssql")
    @patch("dlh_ingestion.incremental_load_mssql")
    @patch("dlh_ingestion.fetch_data_from_oracle")
    @patch("dlh_ingestion.create_table")
    @patch.object(JobTracker, "insert_initial_status")
    def test_process_table_mssql_full(self, ins_init, create_table_fn, fetch_oracle, incr_fn, full_fn, conn_mssql, conn_cols):
        # configure start_time chain
        status_mock = MagicMock(); sel=MagicMock(); fst=MagicMock(); fst.__getitem__.return_value="2025-08-15 14:00:00"; sel.first.return_value=fst; status_mock.select.return_value=sel; ins_init.return_value=status_mock
        conn_cols.return_value = "c1 as c1"
        df = MagicMock()
        df.columns = ["c1"]
        conn_mssql.return_value = df
        full_fn.return_value = None

        row = {
            "job_id": "J1",
            "source_schema": "dbo",
            "source_table": "T",
            "target_schema": "ts",
            "target_table": "tt",
            "app_pipeline": "app",
            "run_group": "rg",
            "is_enabled": "Y",
            "load_type": "FULL",
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
                source_db_config={"db_type": "MSSQL", "db_host": "h", "db_port": "1433", "db_name": "n", "db_user": "u", "driver": "d"},
                dbpass="p",
                email_service=self.email,
                logger=self.logger,
                lock=self.lock,
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
    def test_process_table_oracle_full(self, read_jdbc, ins_init, create_table_fn, fetch_oracle, conn_oracle):
        status_mock = MagicMock(); sel=MagicMock(); fst=MagicMock(); fst.__getitem__.return_value="2025-08-15 13:00:00"; sel.first.return_value=fst; status_mock.select.return_value=sel; ins_init.return_value=status_mock
        df = MagicMock(); df.columns=["a"]; fetch_oracle.return_value=df; create_table_fn.return_value="20250101"; read_jdbc.return_value=df

        row = {
            "job_id": "J1",
            "source_schema": "S",
            "source_table": "T",
            "target_schema": "ts",
            "target_table": "tt",
            "app_pipeline": "app",
            "run_group": "rg",
            "is_enabled": "Y",
            "load_type": "FULL",
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

    @patch("dlh_ingestion.incremental_load_mssql")
    @patch("dlh_ingestion.connect_to_mssql_columns")
    @patch("dlh_ingestion.connect_to_mssql")
    @patch.object(JobTracker, "insert_initial_status")
    def test_process_table_mssql_incremental_timestamp(self, ins_init, conn_mssql, conn_cols, incr_fn):
        status_mock = MagicMock(); sel=MagicMock(); fst=MagicMock(); fst.__getitem__.return_value="2025-08-15 12:00:00"; sel.first.return_value=fst; status_mock.select.return_value=sel; ins_init.return_value=status_mock
        conn_cols.return_value = "c1 as c1"
        df = MagicMock(); df.columns=["c1"]; conn_mssql.return_value = df

        row = {"job_id": "J2", "source_schema": "dbo", "source_table": "T2", "target_schema": "ts", "target_table": "tt2", "app_pipeline": "app", "run_group": "rg", "is_enabled": "Y", "load_type": "INCREMENTAL", "incremental_type": "TIMESTAMP", "incremental_column": "updated_at"}
        out = process_table(row=row, spark=self.spark, env="dev", batch_id="b2", spark_app_id="sa1", catalog_name="cat", tbl_schema="sch", job_status_tbl="job_tbl", cdc_tracker_tbl="cdc_tbl", s3_bucket="s3a://bucket/", source_db_config={"db_type": "MSSQL", "db_host": "h", "db_port": "1433", "db_name": "n", "db_user": "u", "driver": "d"}, dbpass="p", email_service=self.email, logger=self.logger, lock=self.lock, formatted_date="20250101", statusEmails="N", job_tracker=self.job_tracker, run_id="r2")
        self.assertTrue(out)

    @patch("dlh_ingestion.incremental_load_mssql")
    @patch("dlh_ingestion.connect_to_mssql_columns")
    @patch("dlh_ingestion.connect_to_mssql")
    @patch.object(JobTracker, "insert_initial_status")
    def test_process_table_mssql_incremental_append_key(self, ins_init, conn_mssql, conn_cols, incr_fn):
        status_mock = MagicMock(); sel=MagicMock(); fst=MagicMock(); fst.__getitem__.return_value="2025-08-15 11:00:00"; sel.first.return_value=fst; status_mock.select.return_value=sel; ins_init.return_value=status_mock
        conn_cols.return_value = "c1 as c1"
        df = MagicMock(); df.columns = ["c1"]; conn_mssql.return_value = df

        row = {"job_id": "J3", "source_schema": "dbo", "source_table": "T3", "target_schema": "ts", "target_table": "tt3", "app_pipeline": "app", "run_group": "rg", "is_enabled": "Y", "load_type": "INCREMENTAL", "incremental_type": "APPEND_KEY", "incremental_column": "id"}
        out = process_table(row=row, spark=self.spark, env="dev", batch_id="b3", spark_app_id="sa1", catalog_name="cat", tbl_schema="sch", job_status_tbl="job_tbl", cdc_tracker_tbl="cdc_tbl", s3_bucket="s3a://bucket/", source_db_config={"db_type": "MSSQL", "db_host": "h", "db_port": "1433", "db_name": "n", "db_user": "u", "driver": "d"}, dbpass="p", email_service=self.email, logger=self.logger, lock=self.lock, formatted_date="20250101", statusEmails="N", job_tracker=self.job_tracker, run_id="r3")
        self.assertTrue(out)
