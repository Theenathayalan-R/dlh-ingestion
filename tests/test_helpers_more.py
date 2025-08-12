import unittest
from unittest.mock import MagicMock, patch
from dlh_run_db_ingestion import (
    read_config_from_iceberg,
    connect_to_mssql_columns,
    read_from_jdbc,
    get_run_id,
    classify_error,
    CustomLogger,
)


class TestHelpersMore(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    def test_read_config_from_iceberg_success(self):
        df = MagicMock()
        df.collect.return_value = [{"k": "v"}]
        self.spark.sql.return_value = df
        out = read_config_from_iceberg(self.spark, "cat", "sch", "tbl", "app", "rg", self.logger)
        self.assertEqual(out, {"k": "v"})
        self.spark.sql.assert_called_once()

    def test_read_config_from_iceberg_empty_raises(self):
        df = MagicMock()
        df.collect.return_value = []
        self.spark.sql.return_value = df
        with self.assertRaises(ValueError):
            read_config_from_iceberg(self.spark, "cat", "sch", "tbl", "app", "rg", self.logger)

    def test_connect_to_mssql_columns_success(self):
        df = MagicMock()
        row = {"COLUMN_NAME": "C1"}
        df.collect.return_value = [row]
        chain = self.spark.read.format.return_value
        chain.option.return_value = chain
        chain.load.return_value = df
        cols = connect_to_mssql_columns(self.spark, "url", "q", "u", "p", "d", "true", "false", self.logger)
        self.assertEqual(cols, "[C1] as C1")

    def test_connect_to_mssql_columns_empty(self):
        df = MagicMock()
        df.collect.return_value = []
        chain = self.spark.read.format.return_value
        chain.option.return_value = chain
        chain.load.return_value = df
        cols = connect_to_mssql_columns(self.spark, "url", "q", "u", "p", "d", "true", "false", self.logger)
        self.assertEqual(cols, "")

    def test_read_from_jdbc_oracle(self):
        obj_df = MagicMock()
        obj_df.rdd.isEmpty.return_value = False
        obj_df.first.return_value = {"OBJECT_TYPE": "TABLE"}
        meta_df = MagicMock()
        meta_df.first.return_value = {"NUM_ROWS": 10}
        final_df = MagicMock()
        chain = self.spark.read.format.return_value
        chain.option.return_value = chain
        chain.load.side_effect = [obj_df, meta_df, final_df]
        out = read_from_jdbc(
            self.spark, "url", "SELECT * FROM S.T", "u", "p", "d",
            "C1 STRING", "T", "S.T", self.logger
        )
        self.assertIs(out, final_df)

    def test_read_from_jdbc_mssql(self):
        obj_df = MagicMock()
        obj_df.rdd.isEmpty.return_value = False
        obj_df.first.return_value = {"OBJECT_TYPE": "VIEW"}
        meta_df = MagicMock()
        meta_df.first.return_value = {"NUM_ROWS": 0}
        final_df = MagicMock()
        chain = self.spark.read.format.return_value
        chain.option.return_value = chain
        chain.load.side_effect = [obj_df, meta_df, final_df]
        out = read_from_jdbc(
            self.spark, "url", "SELECT * FROM dbo.T", "u", "p", "d",
            "C1 STRING", "T", "dbo.T", self.logger
        )
        self.assertIs(out, final_df)

    def test_get_run_id_present_and_empty(self):
        df = MagicMock()
        df.count.return_value = 1
        rdd = MagicMock()
        rdd.isEmpty.return_value = False
        df.rdd = rdd
        df.first.return_value = {"run_id": "RID"}
        self.spark.sql.return_value = df
        rid = get_run_id(self.spark, "cat", "sch", "tbl", "B1", self.logger)
        self.assertEqual(rid, "RID")

        df2 = MagicMock()
        df2.count.return_value = 0
        rdd2 = MagicMock()
        rdd2.isEmpty.return_value = True
        df2.rdd = rdd2
        self.spark.sql.return_value = df2
        rid2 = get_run_id(self.spark, "cat", "sch", "tbl", "B1", self.logger)
        self.assertIsNone(rid2)

    @patch("dlh_run_db_ingestion.F")
    def test_classify_error_paths(self, mock_F):
        spark_mock = MagicMock()

        patterns_df = MagicMock()
        error_df = MagicMock()
        df2 = MagicMock()
        df3 = MagicMock()
        df4 = MagicMock()
        df5 = MagicMock()
        df5.first.return_value = {"error_type": "CRITICAL", "description": "Invalid Credentials"}

        error_df.crossJoin.return_value = df2
        df2.withColumn.return_value = df3
        df3.filter.return_value = df4
        df4.select.return_value = df5

        spark_mock.createDataFrame.side_effect = [patterns_df, error_df]

        mcol = MagicMock()
        mcol.rlike.return_value = MagicMock()
        mock_F.col.return_value = mcol

        out = classify_error(spark_mock, "ORA-01017: invalid username/password", self.logger)
        self.assertEqual(out, {"error_type": "CRITICAL", "description": "Invalid Credentials"})

        spark_mock2 = MagicMock()
        patterns_df2 = MagicMock()
        error_df2 = MagicMock()
        df20 = MagicMock()
        df21 = MagicMock()
        df22 = MagicMock()
        df23 = MagicMock()
        df23.first.return_value = None

        error_df2.crossJoin.return_value = df20
        df20.withColumn.return_value = df21
        df21.filter.return_value = df22
        df22.select.return_value = df23
        spark_mock2.createDataFrame.side_effect = [patterns_df2, error_df2]
        mock_F.col.return_value = mcol

        out2 = classify_error(spark_mock2, "random error", self.logger)
        self.assertEqual(out2, {"error_type": "NON_CRITICAL", "description": "Error occurred"})
