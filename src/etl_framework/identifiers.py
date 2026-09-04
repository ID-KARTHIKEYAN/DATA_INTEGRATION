from __future__ import annotations

import re

from etl_framework.exceptions import MetadataValidationError

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FILE_STRIP = re.compile(r"\.[A-Za-z0-9]+$")


def require_ident(name: str, field: str) -> str:
    value = (name or "").strip()
    if not value or not _IDENT.match(value):
        raise MetadataValidationError(f"{field} is not a valid identifier: {name!r}")
    return value


def sanitize_object_name(source_obj_name: str) -> str:
    """Derive a Delta table name from SOURCE_OBJ_NAME (may be a file name)."""
    base = (source_obj_name or "").strip().split("/")[-1]
    base = _FILE_STRIP.sub("", base)
    base = re.sub(r"[^A-Za-z0-9_]", "_", base)
    if base and base[0].isdigit():
        base = f"t_{base}"
    if not base or not _IDENT.match(base):
        raise MetadataValidationError(
            f"Cannot derive table name from SOURCE_OBJ_NAME={source_obj_name!r}"
        )
    return base.lower()


def split_qualified_object(value: str) -> tuple[str | None, str]:
    """schema.table or table. Does not accept catalog.schema.table (use header.target_catalog)."""
    text = (value or "").strip()
    if not text:
        return None, ""
    parts = text.split(".")
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise MetadataValidationError(
        f"TARGET_LOAD_TABLE must be object or schema.object, got {value!r}"
    )


def fqn(catalog: str, schema: str, table: str) -> str:
    return f"{require_ident(catalog, 'catalog')}.{require_ident(schema, 'schema')}.{require_ident(table, 'table')}"
