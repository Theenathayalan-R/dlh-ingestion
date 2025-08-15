import sys
import types
import unittest
from unittest.mock import MagicMock, patch
import importlib.util


class TestLazyLoaderAndRuntimeGuards(unittest.TestCase):
    def test_lazy_loader_access(self):
        # Ensure process_runtime not yet imported
        sys.modules.pop('dlh_ingestion.process_runtime', None)
        import dlh_ingestion  # noqa: F401
        try:
            attr = dlh_ingestion.process_table  # triggers __getattr__
        except ImportError as e:  # pyspark missing scenario is acceptable
            self.assertIn('PySpark is required', str(e))
            return
        # If pyspark present, we at least have a callable
        self.assertTrue(callable(attr))

    def test_require_pyspark_guard(self):
        import dlh_ingestion.process_runtime as pr
        # Patch pyspark to None to force guard failure
        with patch.object(pr, 'pyspark', None), patch.object(pr, '_PYSPARK_IMPORT_ERROR', ImportError('no pyspark')):
            with self.assertRaises(ImportError):
                pr._require_pyspark()

    def test_config_row_validation_or_fallback(self):
        import dlh_ingestion.process_runtime as pr
        # If pydantic available, invalid instantiation should raise
        ConfigRow = pr.ConfigRow
        spec = importlib.util.find_spec('pydantic')
        is_pydantic = hasattr(ConfigRow, 'model_fields') or hasattr(ConfigRow, 'schema_json')
        if spec is None or not is_pydantic:
            # skip if pydantic not installed
            self.skipTest('pydantic not installed')
        else:
            # dynamic import to avoid top-level lint error
            pydantic_mod = importlib.import_module('pydantic')
            ValidationError = getattr(pydantic_mod, 'ValidationError')
            with self.assertRaises(ValidationError):
                ConfigRow()  # missing required fields

    def test_custom_logger_context_manager_flush(self):
        from dlh_ingestion import CustomLogger
        spark_mock = MagicMock()
        logger = CustomLogger(spark_mock, 's3://bucket/', '/logs/', 'app')
        with patch.object(CustomLogger, 'save_to_s3', return_value=True) as save_mock:
            with logger:
                logger.log('inside context')
            save_mock.assert_called_once()

    def test_handle_job_completion_or_failure_without_pyspark(self):
        # Validate graceful ImportError when pyspark absent
        import dlh_ingestion.process_runtime as pr
        with patch.object(pr, 'pyspark', None), patch.object(pr, '_PYSPARK_IMPORT_ERROR', ImportError('no pyspark')):
            with self.assertRaises(ImportError):
                pr.handle_job_completion_or_failure(
                    spark=None,
                    source_df=None,
                    email_service=None,
                    logger=MagicMock(),
                    job_id='J',
                    source_schema='S',
                    source_table='T',
                    target_schema='TS',
                    target_table='TT',
                    batch_id='B',
                    run_id='R',
                    spark_app_id='SA',
                    catalog_name='C',
                    tbl_schema='SCH',
                    job_status_tbl='JT',
                    error_message=None,
                    statusEmails='N',
                    load_date=None,
                    start_time='2025-01-01 00:00:00',
                    end_time='2025-01-01 00:05:00',
                )


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
