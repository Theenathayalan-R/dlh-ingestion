from importlib import import_module
from typing import Any

__version__ = "0.1.0"

# Public API – grouped for clarity (safe, no immediate pyspark dependency)
from .logging_utils import CustomLogger  # noqa: E402
from .job_tracking import JobTracker  # noqa: E402
from .email_service import EmailService, JobRunReport  # noqa: E402
from .utils import sanitize_column_name  # noqa: E402
from .errors import classify_error  # noqa: E402

# Lazy export groups (pyspark heavy)
_RUNTIME_EXPORTS = [
    "read_config_from_iceberg",
    "get_run_id",
    "compare_and_notify_schema_changes",
    "handle_job_completion_or_failure",
    "process_table",
    "main",
]
_JDBC_EXPORTS = [
    "connect_to_oracle",
    "connect_to_mssql",
    "connect_to_mssql_columns",
    "map_data_types",
    "probe_read",
    "read_from_jdbc",
    "fetch_data_from_oracle",
]
_LOADER_EXPORTS = [
    "create_table",
    "full_load_mssql",
    "incremental_load_mssql",
]
_ORCH_EXPORTS = [
    "write_with_retry",
    "rerun_failed_jobs",
]

# Build mapping attr -> module
_LAZY_ATTR_MODULE = {name: "dlh_ingestion.process_runtime" for name in _RUNTIME_EXPORTS}
_LAZY_ATTR_MODULE.update({name: "dlh_ingestion.jdbc_adaptive" for name in _JDBC_EXPORTS})
_LAZY_ATTR_MODULE.update({name: "dlh_ingestion.loaders" for name in _LOADER_EXPORTS})
_LAZY_ATTR_MODULE.update({name: "dlh_ingestion.orchestration" for name in _ORCH_EXPORTS})

__all__ = (
    "__version__",
    # core
    "CustomLogger",
    "JobTracker",
    "EmailService",
    "JobRunReport",
    "sanitize_column_name",
    "classify_error",
    # lazy groups
    *_RUNTIME_EXPORTS,
    *_JDBC_EXPORTS,
    *_LOADER_EXPORTS,
    *_ORCH_EXPORTS,
)


def __getattr__(name: str) -> Any:  # pragma: no cover
    module_path = _LAZY_ATTR_MODULE.get(name)
    if module_path:
        mod = import_module(module_path)
        obj = getattr(mod, name)
        globals()[name] = obj  # cache
        return obj
    raise AttributeError(f"module 'dlh_ingestion' has no attribute '{name}'")
