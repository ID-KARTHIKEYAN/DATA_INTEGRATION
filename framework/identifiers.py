import re

from .errors import MetadataError

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(value: str, label: str) -> str:
    value = (value or "").strip()
    if not _IDENTIFIER.fullmatch(value):
        raise MetadataError(f"Invalid {label}: {value!r}")
    return f"`{value}`"


def qualified_name(catalog: str, schema: str, name: str) -> str:
    return ".".join((quote_identifier(catalog, "catalog"), quote_identifier(schema, "schema"), quote_identifier(name, "object")))