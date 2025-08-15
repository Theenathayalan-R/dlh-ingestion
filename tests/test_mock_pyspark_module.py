import sys
import types
import unittest


class TestMockPySparkModule(unittest.TestCase):
    def test_import_with_mock_pyspark(self):
        # Provide a lightweight mock pyspark to satisfy import during tests without real installation
        if 'pyspark' in sys.modules:
            self.skipTest('Real pyspark present; mock not needed')
        mock_pyspark = types.ModuleType('pyspark')
        sql_mod = types.ModuleType('pyspark.sql')
        class SparkSession:
            class Builder:
                def getOrCreate(self):
                    return types.SimpleNamespace(
                        sparkContext=types.SimpleNamespace(applicationId='app123', setLocalProperty=lambda *a, **k: None),
                        conf=types.SimpleNamespace(set=lambda *a, **k: None),
                        sql=lambda q: types.SimpleNamespace(count=lambda:0, rdd=types.SimpleNamespace(isEmpty=lambda: True), first=lambda: {'run_id': '1'}),
                        table=lambda name: types.SimpleNamespace(filter=lambda *a, **k: types.SimpleNamespace(persist=lambda : types.SimpleNamespace(collect=lambda: [])))
                    )
            builder = Builder()
        sql_mod.SparkSession = SparkSession
        functions_mod = types.ModuleType('pyspark.sql.functions')
        def col(name):
            return name
        def coalesce(a, b):
            return a or b
        functions_mod.col = col
        functions_mod.coalesce = coalesce
        sys.modules['pyspark'] = mock_pyspark
        sys.modules['pyspark.sql'] = sql_mod
        sys.modules['pyspark.sql.functions'] = functions_mod
        import dlh_ingestion
        # Access lazy attr (should now import process_runtime without raising ImportError)
        try:
            _ = dlh_ingestion.process_table
        except ImportError:
            self.fail('process_table should import with mock pyspark present')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
