class FrameworkError(RuntimeError):
    """Expected framework failure that must fail the Databricks task."""


class MetadataError(FrameworkError):
    pass


class DataQualityError(FrameworkError):
    pass