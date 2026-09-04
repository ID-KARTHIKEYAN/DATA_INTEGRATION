"""Framework exceptions. Callers must never swallow these."""


class FrameworkError(Exception):
    """Base error. Always propagate after audit write."""


class MetadataNotFoundError(FrameworkError):
    pass


class MetadataValidationError(FrameworkError):
    pass


class TaskSelectionError(FrameworkError):
    pass


class SchemaValidationError(FrameworkError):
    pass


class UnsupportedOnFreeEditionError(FrameworkError):
    pass


class LoadExecutionError(FrameworkError):
    pass


class DataQualityError(FrameworkError):
    pass


class MergeExecutionError(FrameworkError):
    pass
