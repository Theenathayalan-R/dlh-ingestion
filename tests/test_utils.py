import unittest
from dlh_run_db_ingestion import sanitize_column_name


class TestUtils(unittest.TestCase):
    def test_sanitize_column_name_basic(self):
        self.assertEqual(sanitize_column_name("First Name"), "First_Name")
        self.assertEqual(sanitize_column_name("col-1@x"), "col_1_x")
        self.assertEqual(sanitize_column_name("  a   b  "), "a_b")

    def test_sanitize_preserve_underscore(self):
        self.assertEqual(sanitize_column_name("a_b_c"), "a_b_c")
