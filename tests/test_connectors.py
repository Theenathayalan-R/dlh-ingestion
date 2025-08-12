import unittest
from unittest.mock import MagicMock
from dlh_run_db_ingestion import connect_to_mssql_columns


class TestConnectors(unittest.TestCase):
    def test_connect_to_mssql_columns(self):
        spark = MagicMock()
        reader = MagicMock()
        spark.read = reader
        reader.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        row1 = {"COLUMN_NAME": "First Name"}
        row2 = {"COLUMN_NAME": "id"}
        df.collect.return_value = [row1, row2]
        reader.load.return_value = df

        result = connect_to_mssql_columns(
            spark=spark,
            url="jdbc:sqlserver://example",
            query="SELECT ...",
            user="u",
            password="p",
            driver="d",
            encrypt="true",
            certificate="false",
            logger=MagicMock(),
        )
        self.assertIn("[First Name] as First_Name", result)
        self.assertIn("[id] as id", result)
