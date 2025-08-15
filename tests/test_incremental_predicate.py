import unittest
from unittest.mock import MagicMock


class TestIncrementalPredicate(unittest.TestCase):
    def setUp(self):
        import dlh_ingestion.process_runtime as pr
        self.pr = pr
        # Build spark mock
        self.spark = MagicMock()
        self.logger = MagicMock()

    def test_predicate_timestamp(self):
        pr = self.pr
        df_mock = MagicMock()
        df_mock.count.return_value = 1
        df_mock.first.return_value = {"last_val": "2025-01-01 00:00:00"}
        self.spark.sql.return_value = df_mock
        pred = pr._build_incremental_predicate(self.spark, 'cat', 'sch', 'tracker', 'J1', 'updated_at', 'TIMESTAMP', self.logger)
        self.assertIn("updated_at", pred)
        self.assertTrue(pred.startswith("WHERE updated_at > '"))

    def test_predicate_append_key(self):
        pr = self.pr
        df_mock = MagicMock()
        df_mock.count.return_value = 1
        df_mock.first.return_value = {"last_val": 100}
        self.spark.sql.return_value = df_mock
        pred = pr._build_incremental_predicate(self.spark, 'cat', 'sch', 'tracker', 'J2', 'id', 'APPEND_KEY', self.logger)
        self.assertEqual(pred, 'WHERE id > 100')

    def test_predicate_no_tracker(self):
        pr = self.pr
        pred = pr._build_incremental_predicate(self.spark, 'cat', 'sch', None, 'J3', 'id', 'APPEND_KEY', self.logger)
        self.assertEqual(pred, '')

    def test_predicate_no_data(self):
        pr = self.pr
        df_mock = MagicMock()
        df_mock.count.return_value = 0
        self.spark.sql.return_value = df_mock
        pred = pr._build_incremental_predicate(self.spark, 'cat', 'sch', 'tracker', 'J4', 'id', 'APPEND_KEY', self.logger)
        self.assertEqual(pred, '')

    def test_predicate_exception(self):
        pr = self.pr
        self.spark.sql.side_effect = Exception('boom')
        pred = pr._build_incremental_predicate(self.spark, 'cat', 'sch', 'tracker', 'J5', 'id', 'APPEND_KEY', self.logger)
        self.assertEqual(pred, '')
        self.assertTrue(self.logger.log.called)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
