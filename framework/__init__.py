# =============================================================================
# framework/__init__.py
# Convenience imports for the ETL framework package.
# In Databricks notebooks, modules can be imported directly:
#   %run ../framework/metadata_reader
# Or via sys.path after the repo is checked out:
#   import sys; sys.path.insert(0, "/Workspace/Repos/.../DATA_INTEGRATION")
#   from framework.metadata_reader import MetadataReader
# =============================================================================

from framework.metadata_reader     import MetadataReader
from framework.metadata_validator  import MetadataValidator, ValidationError, ConfigurationError
from framework.source_reader       import SourceReader, SourceReadError
from framework.transform_engine    import TransformEngine, TransformError
from framework.writer              import DeltaWriter, WriteError
from framework.audit_logger        import AuditLogger
from framework.exception_handler   import with_retry, format_error, print_traceback
from framework.spark_config_manager import SparkConfigManager

__all__ = [
    "MetadataReader",
    "MetadataValidator", "ValidationError", "ConfigurationError",
    "SourceReader", "SourceReadError",
    "TransformEngine", "TransformError",
    "DeltaWriter", "WriteError",
    "AuditLogger",
    "with_retry", "format_error", "print_traceback",
    "SparkConfigManager",
]
