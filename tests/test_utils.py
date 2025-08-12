import unittest
from unittest.mock import MagicMock
from dlh_run_db_ingestion import sanitize_column_name, CustomLogger


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.logger = CustomLogger(self.spark, "s3a://bucket/", "prefix/", "test-app")

    def test_sanitize_column_name_basic(self):
        self.assertEqual(sanitize_column_name("First Name", self.logger), "First_Name")
        self.assertEqual(sanitize_column_name("col-1@x", self.logger), "col_1_x")
        self.assertEqual(sanitize_column_name("  a   b  ", self.logger), "a_b")

    def test_sanitize_preserve_underscore(self):
        self.assertEqual(sanitize_column_name("a_b_c", self.logger), "a_b_c")
