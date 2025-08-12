import unittest
from unittest.mock import MagicMock, patch

from dlh_run_db_ingestion import (
    read_config_from_iceberg,
    connect_to_oracle,
    connect_to_mssql,
    read_from_jdbc,
    get_run_id,
    classify_error,
)


class TestConfigAndConnectors(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = MagicMock()

    def test_read_config_from_iceberg_success(self):
        df = MagicMock()
        row = {"k": "v"}
        df.collect.return_value = [row]
        self.spark.sql.return_value = df
        out = read_config_from_iceberg(self.spark, "cat", "db", "tbl", "pipe", "rg", self.logger)
        self.assertEqual(out, row)
        self.logger.log.assert_any_call("Successfully read configuration from cat.db.tbl", "INFO")

    def test_read_config_from_iceberg_empty_raises(self):
        df = MagicMock()
        df.collect.return_value = []
        self.spark.sql.return_value = df
        with self.assertRaises(ValueError):
            read_config_from_iceberg(self.spark, "cat", "db", "tbl", "pipe", "rg", self.logger)
        self.assertTrue(self.logger.log.called)

    def test_connect_to_oracle_success(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        reader.load.return_value = df
        out = connect_to_oracle(self.spark, "url", "select 1", "u", "p", "d", self.logger)
        self.assertIs(out, df)
        self.logger.log.assert_any_call("Oracle connection is made successfully", "INFO")

    def test_connect_to_oracle_error(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.side_effect = Exception("bad")
        with self.assertRaises(Exception):
            connect_to_oracle(self.spark, "url", "q", "u", "p", "d", self.logger)
        self.logger.log.assert_any_call("Error connecting to Oracle: bad", "ERROR")

    def test_connect_to_mssql_success(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        reader.load.return_value = df
        out = connect_to_mssql(self.spark, "url", "q", "u", "p", "d", "true", "false", self.logger)
        self.assertIs(out, df)
        self.logger.log.assert_any_call("MSSQL connection is made successfully", "INFO")

    def test_connect_to_mssql_error(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.side_effect = Exception("badm")
        with self.assertRaises(Exception):
            connect_to_mssql(self.spark, "url", "q", "u", "p", "d", "true", "false", self.logger)
        self.logger.log.assert_any_call("Error connecting to MSSQL: badm", "ERROR")

    def test_read_from_jdbc_branches_table(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader

        object_df = MagicMock()
        object_df.rdd.isEmpty.return_value = False
        object_df.first.return_value = {"OBJECT_TYPE": "TABLE"}

        meta_df = MagicMock()
        meta_df.first.return_value = {"NUM_ROWS": 10}

        data_df = MagicMock()

        reader.load.side_effect = [object_df, meta_df, data_df]

        out = read_from_jdbc(
            spark=self.spark,
            url="url",
            query="SELECT * FROM T",
            user="u",
            password="p",
            driver="d",
            schema_string="C1 STRING",
            source_table="T",
            dbtable="S.T",
            logger=self.logger,
        )
        self.assertIs(out, data_df)

    def test_read_from_jdbc_branches_view(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader

        object_df = MagicMock()
        object_df.rdd.isEmpty.return_value = False
        object_df.first.return_value = {"OBJECT_TYPE": "VIEW"}

        meta_df = MagicMock()
        meta_df.first.return_value = {"NUM_ROWS": 0}

        data_df = MagicMock()

        reader.load.side_effect = [object_df, meta_df, data_df]
        out = read_from_jdbc(
            spark=self.spark,
            url="url",
            query="SELECT * FROM V",
            user="u",
            password="p",
            driver="d",
            schema_string="C1 STRING",
            source_table="V",
            dbtable="S.V",
            logger=self.logger,
        )
        self.assertIs(out, data_df)

    def test_read_from_jdbc_object_not_found_raises(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader

        object_df = MagicMock()
        object_df.rdd.isEmpty.return_value = True
        reader.load.return_value = object_df

        with self.assertRaises(ValueError):
            read_from_jdbc(
                spark=self.spark,
                url="url",
                query="Q",
                user="u",
                password="p",
                driver="d",
                schema_string="C1 STRING",
                source_table="T",
                dbtable="S.T",
                logger=self.logger,
            )

    def test_get_run_id_empty_and_present(self):
        df_empty = MagicMock()
        df_empty.count.return_value = 0
        df_empty.rdd.isEmpty.return_value = True
        self.spark.sql.return_value = df_empty
        out_none = get_run_id(self.spark, "c", "d", "t", "b", self.logger)
        self.assertIsNone(out_none)

        df = MagicMock()
        df.count.return_value = 1
        df.rdd.isEmpty.return_value = False
        df.first.return_value = {"run_id": "RID"}
        self.spark.sql.return_value = df
        out = get_run_id(self.spark, "c", "d", "t", "b", self.logger)
        self.assertEqual(out, "RID")

    def test_classify_error_handles_exception_path(self):
        bad_spark = MagicMock()
        bad_spark.createDataFrame.side_effect = Exception("boom")
        with self.assertRaises(Exception):
            classify_error(bad_spark, "anything", self.logger)
        self.logger.log.assert_called()
import unittest
from unittest.mock import MagicMock, patch

from dlh_run_db_ingestion import (
    read_config_from_iceberg,
    connect_to_oracle,
    connect_to_mssql,
    read_from_jdbc,
    get_run_id,
    classify_error,
)


class TestConfigAndConnectors(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = MagicMock()

    def test_read_config_from_iceberg_success(self):
        df = MagicMock()
        row = {"k": "v"}
        df.collect.return_value = [row]
        self.spark.sql.return_value = df
        out = read_config_from_iceberg(self.spark, "cat", "db", "tbl", "pipe", "rg", self.logger)
        self.assertEqual(out, row)
        self.logger.log.assert_any_call("Successfully read configuration from cat.db.tbl", "INFO")

    def test_read_config_from_iceberg_empty_raises(self):
        df = MagicMock()
        df.collect.return_value = []
        self.spark.sql.return_value = df
        with self.assertRaises(ValueError):
            read_config_from_iceberg(self.spark, "cat", "db", "tbl", "pipe", "rg", self.logger)
        self.assertTrue(self.logger.log.called)

    def test_connect_to_oracle_success(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        reader.load.return_value = df
        out = connect_to_oracle(self.spark, "url", "select 1", "u", "p", "d", self.logger)
        self.assertIs(out, df)
        self.logger.log.assert_any_call("Oracle connection is made successfully", "INFO")

    def test_connect_to_oracle_error(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.side_effect = Exception("bad")
        with self.assertRaises(Exception):
            connect_to_oracle(self.spark, "url", "q", "u", "p", "d", self.logger)
        self.logger.log.assert_any_call("Error connecting to Oracle: bad", "ERROR")

    def test_connect_to_mssql_success(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        reader.load.return_value = df
        out = connect_to_mssql(self.spark, "url", "q", "u", "p", "d", "true", "false", self.logger)
        self.assertIs(out, df)
        self.logger.log.assert_any_call("MSSQL connection is made successfully", "INFO")

    def test_connect_to_mssql_error(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.side_effect = Exception("badm")
        with self.assertRaises(Exception):
            connect_to_mssql(self.spark, "url", "q", "u", "p", "d", "true", "false", self.logger)
        self.logger.log.assert_any_call("Error connecting to MSSQL: badm", "ERROR")

    def test_read_from_jdbc_branches_table(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader

        object_df = MagicMock()
        object_df.rdd.isEmpty.return_value = False
        object_df.first.return_value = {"OBJECT_TYPE": "TABLE"}

        meta_df = MagicMock()
        meta_df.first.return_value = {"NUM_ROWS": 10}

        data_df = MagicMock()

        reader.load.side_effect = [object_df, meta_df, data_df]

        out = read_from_jdbc(
            spark=self.spark,
            url="url",
            query="SELECT * FROM T",
            user="u",
            password="p",
            driver="d",
            schema_string="C1 STRING",
            source_table="T",
            dbtable="S.T",
            logger=self.logger,
        )
        self.assertIs(out, data_df)

    def test_read_from_jdbc_branches_view(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader

        object_df = MagicMock()
        object_df.rdd.isEmpty.return_value = False
        object_df.first.return_value = {"OBJECT_TYPE": "VIEW"}

        meta_df = MagicMock()
        meta_df.first.return_value = {"NUM_ROWS": 0}

        data_df = MagicMock()

        reader.load.side_effect = [object_df, meta_df, data_df]
        out = read_from_jdbc(
            spark=self.spark,
            url="url",
            query="SELECT * FROM V",
            user="u",
            password="p",
            driver="d",
            schema_string="C1 STRING",
            source_table="V",
            dbtable="S.V",
            logger=self.logger,
        )
        self.assertIs(out, data_df)

    def test_read_from_jdbc_object_not_found_raises(self):
        reader = MagicMock()
        self.spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader

        object_df = MagicMock()
        object_df.rdd.isEmpty.return_value = True  # causes None path
        reader.load.return_value = object_df

        with self.assertRaises(ValueError):
            read_from_jdbc(
                spark=self.spark,
                url="url",
                query="Q",
                user="u",
                password="p",
                driver="d",
                schema_string="C1 STRING",
                source_table="T",
                dbtable="S.T",
                logger=self.logger,
            )

    def test_get_run_id_empty_and_present(self):
        df_empty = MagicMock()
        df_empty.count.return_value = 0
        df_empty.rdd.isEmpty.return_value = True
        self.spark.sql.return_value = df_empty
        out_none = get_run_id(self.spark, "c", "d", "t", "b", self.logger)
        self.assertIsNone(out_none)

        df = MagicMock()
        df.count.return_value = 1
        df.rdd.isEmpty.return_value = False
        df.first.return_value = {"run_id": "RID"}
        self.spark.sql.return_value = df
        out = get_run_id(self.spark, "c", "d", "t", "b", self.logger)
        self.assertEqual(out, "RID")

    def test_classify_error_handles_exception_path(self):
        bad_spark = MagicMock()
        bad_spark.createDataFrame.side_effect = Exception("boom")
        with self.assertRaises(Exception):
            classify_error(bad_spark, "anything", self.logger)
        self.logger.log.assert_called()
