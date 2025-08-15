import unittest
from unittest.mock import MagicMock, patch
from dlh_ingestion import sanitize_column_name, map_data_types, fetch_data_from_oracle, CustomLogger


class TestHelpersAndMapping(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    def test_sanitize_various(self):
        self.assertEqual(sanitize_column_name("a  b", self.logger), "a_b")
        self.assertEqual(sanitize_column_name("A-B/C", self.logger), "A_B_C")
        self.assertEqual(sanitize_column_name("__x__ y__", self.logger), "x_y")

    @patch("dlh_ingestion.jdbc_adaptive.connect_to_oracle")
    def test_map_data_types_various(self, conn_oracle):
        df_count = MagicMock()
        df_count.first.return_value = {"DECIMAL_COUNT": 1}
        df_scale = MagicMock()
        df_scale.first.return_value = {"DATA_SCALE": 3}
        conn_oracle.side_effect = [df_count, df_scale]

        data_df = MagicMock()
        rows = [
            {"COLUMN_NAME": "C1", "DATA_TYPE": "NUMBER"},
            {"COLUMN_NAME": "C2", "DATA_TYPE": "VARCHAR2"},
            {"COLUMN_NAME": "C3", "DATA_TYPE": "DATE"},
            {"COLUMN_NAME": "C4", "DATA_TYPE": "TIMESTAMP"},
            {"COLUMN_NAME": "C5", "DATA_TYPE": "BLOB"},
            {"COLUMN_NAME": "C6", "DATA_TYPE": "TIMESTAMP(6) WITH TIME ZONE"},
            {"COLUMN_NAME": "C7", "DATA_TYPE": "TIMESTAMP(6)"},
            {"COLUMN_NAME": "C8", "DATA_TYPE": "UNKNOWN"},
        ]
        data_df.collect.return_value = rows

        out = map_data_types(
            self.spark,
            data_df,
            "mod_col",
            "crt_col",
            "conn",
            "user",
            "pass",
            "driver",
            "SCHEMA.TABLE",
            self.logger,
        )
        self.assertIn("C1 DECIMAL(38, 3)", out)
        self.assertIn("C2 STRING", out)
        self.assertIn("C3 TIMESTAMP", out)
        self.assertIn("C4 TIMESTAMP", out)
        self.assertIn("C5 BINARY", out)
        self.assertIn("C6 TIMESTAMP", out)
        self.assertIn("C7 TIMESTAMP", out)
        self.assertIn("C8 STRING", out)

    @patch("dlh_ingestion.jdbc_adaptive.read_from_jdbc")
    def test_fetch_data_from_oracle_query_variants(self, read_jdbc):
        read_jdbc.return_value = MagicMock()
        data_df = MagicMock()
        data_df.collect.return_value = [{"COLUMN_NAME": "C1", "DATA_TYPE": "VARCHAR2"}]

        fetch_data_from_oracle(
            self.spark, "url", "S.T", "u", "p", "d", data_df,
            "mod", "crt", None, None, self.logger, "T"
        )
        fetch_data_from_oracle(
            self.spark, "url", "S.T", "u", "p", "d", data_df,
            "mod", "crt", "X=1", None, self.logger, "T"
        )
        fetch_data_from_oracle(
            self.spark, "url", "S.T", "u", "p", "d", data_df,
            "mod", "crt", None, "C1,C2", self.logger, "T"
        )
        fetch_data_from_oracle(
            self.spark, "url", "S.T", "u", "p", "d", data_df,
            "mod", "crt", "X=1", "C1,C2", self.logger, "T"
        )
        self.assertTrue(read_jdbc.called)
