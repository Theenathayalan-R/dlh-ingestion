# Test package initializer: inject lightweight pyspark mock if real pyspark not present.
import sys, types

if 'pyspark' not in sys.modules:
    pyspark_mod = types.ModuleType('pyspark')
    sql_mod = types.ModuleType('pyspark.sql')
    functions_mod = types.ModuleType('pyspark.sql.functions')
    types_mod = types.ModuleType('pyspark.sql.types')
    window_mod = types.ModuleType('pyspark.sql.window')

    class StructType(list):
        def __init__(self, *a, **k):
            pass
    class StructField:
        def __init__(self, name, _type, nullable):
            self.name = name
    class StringType: ...
    types_mod.StructType = StructType
    types_mod.StructField = StructField
    types_mod.StringType = StringType

    class _Writer:
        def __init__(self, df): self._df = df
        def format(self, *a, **k): return self
        def mode(self, *a, **k): return self
        def option(self, *a, **k): return self
        def saveAsTable(self, *a, **k): return None
        def save(self, *a, **k): return None
        def coalesce(self, *a, **k): return self

    class MockDF:
        def __init__(self, rows=None):
            self._rows = rows or []
            if self._rows and isinstance(self._rows[0], dict):
                self.columns = list(self._rows[0].keys())
            else:
                self.columns = []
            self.write = _Writer(self)
        def count(self): return len(self._rows)
        def first(self): return self._rows[0] if self._rows else {}
        def collect(self): return self._rows
        def filter(self, *a, **k): return self
        def persist(self, *a, **k): return self
        def select(self, *cols): return self
        def distinct(self): return self
        def withColumn(self, *a, **k): return self
        def unionByName(self, other, allowMissingColumns=False): return self
        def crossJoin(self, other): return self
        def withColumnRenamed(self, *a, **k): return self
        def coalesce(self, *a, **k): return self
        def join(self, *a, **k): return self
        def alias(self, *a, **k): return self

    # Expose DataFrame alias
    sql_mod.DataFrame = MockDF

    class _SparkContext:
        applicationId = 'app-mock'
        def setLocalProperty(self, *a, **k): pass
    class _Conf:
        def set(self, *a, **k): pass
    class SparkSession:
        def __init__(self):
            self.sparkContext = _SparkContext()
            self.conf = _Conf()
        def sql(self, *a, **k): return MockDF([])
        def table(self, *a, **k): return MockDF([])
        def createDataFrame(self, data, schema=None): return MockDF(data)
        class Builder:
            def getOrCreate(self_inner):
                return SparkSession()
        builder = Builder()
    sql_mod.SparkSession = SparkSession

    def lit(x): return x
    def expr(s): return s
    def col(c): return c
    def coalesce(a, b): return a if a is not None else b
    functions_mod.lit = lit
    functions_mod.expr = expr
    functions_mod.col = col
    functions_mod.coalesce = coalesce

    class Window: ...
    window_mod.Window = Window

    sys.modules['pyspark'] = pyspark_mod
    sys.modules['pyspark.sql'] = sql_mod
    sys.modules['pyspark.sql.functions'] = functions_mod
    sys.modules['pyspark.sql.types'] = types_mod
    sys.modules['pyspark.sql.window'] = window_mod
