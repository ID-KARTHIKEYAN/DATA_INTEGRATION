from __future__ import annotations

import json
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_framework.exceptions import DataQualityError, SchemaValidationError
from etl_framework.logging_utils import log_event


def apply_l0_transform_map(df: DataFrame, transform_query: object | None, ls_flag: str | None) -> DataFrame:
    """L0 TRANSFORM_QUERY is map<string,string>: column -> Spark SQL cast/expression."""
    if str(ls_flag or "").strip().upper() == "Y":
        return df
    if not transform_query:
        return df
    if not isinstance(transform_query, dict):
        raise SchemaValidationError("L0 TRANSFORM_QUERY must be a map")
    exprs = []
    used = set()
    for col, expr in transform_query.items():
        if col not in df.columns:
            raise SchemaValidationError(
                f"TRANSFORM_QUERY key {col!r} is not in source columns {df.columns}"
            )
        used.add(col)
        exprs.append(f"{expr} AS `{col}`")
    for col in df.columns:
        if col not in used:
            exprs.append(f"`{col}`")
    return df.selectExpr(*exprs)


def apply_cdc_filter(df: DataFrame, cdc_logic: object | None, load_type: str) -> DataFrame:
    text = str(cdc_logic or "").strip()
    if not text:
        return df
    log_event("apply_cdc_logic", load_type=load_type, predicate=text[:300])
    try:
        return df.where(text)
    except Exception as exc:  # noqa: BLE001
        raise SchemaValidationError(f"CDC_LOGIC failed to resolve against columns {df.columns}: {exc}") from exc


def apply_dq(df: DataFrame, dq_logic: object | None, target: str) -> DataFrame:
    """
    DQ_LOGIC is an unconstrained STRING in metadata.
    Supported forms (no extra columns):
      1. SQL boolean expression — rows that fail are counted; any failure raises.
      2. JSON {"expr":"..."} or {"rules":[{"name":"...","expr":"..."}]}
    """
    text = str(dq_logic or "").strip()
    if not text:
        return df
    rules: list[tuple[str, str]] = []
    if text.startswith("{") or text.startswith("["):
        payload = json.loads(text)
        if isinstance(payload, dict) and "expr" in payload:
            rules.append((payload.get("name") or "dq", payload["expr"]))
        elif isinstance(payload, dict) and "rules" in payload:
            for i, rule in enumerate(payload["rules"]):
                rules.append((rule.get("name") or f"rule_{i}", rule["expr"]))
        elif isinstance(payload, list):
            for i, rule in enumerate(payload):
                rules.append((rule.get("name") or f"rule_{i}", rule["expr"]))
        else:
            raise DataQualityError(f"Unrecognized DQ_LOGIC JSON for {target}")
    else:
        rules.append(("dq_logic", text))

    total = df.count()
    for name, expr in rules:
        try:
            bad = df.filter(f"NOT ({expr})").count()
        except Exception as exc:  # noqa: BLE001
            raise DataQualityError(
                f"DQ_LOGIC '{name}' does not resolve on {target} columns={df.columns}: {exc}"
            ) from exc
        log_event("dq_check", target=target, rule=name, failed_rows=bad, total=total)
        if bad > 0:
            raise DataQualityError(
                f"DQ failed for {target} rule={name}: {bad}/{total} rows failed ({expr})"
            )
    return df


_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def assert_columns_exist(df: DataFrame, columns: list[str], *, context: str) -> None:
    missing = [c for c in columns if c and c not in df.columns]
    if missing:
        raise SchemaValidationError(f"{context}: unresolved columns {missing}; have {df.columns}")


def parse_column_list(text: object | None) -> list[str]:
    if not text:
        return []
    cols = [c.strip() for c in str(text).split(",") if c.strip()]
    bad = [c for c in cols if not _IDENT.match(c)]
    if bad:
        raise SchemaValidationError(f"Invalid column identifiers: {bad}")
    return cols


def validate_against_target(
    spark,
    df: DataFrame,
    full_name: str,
    *,
    load_type: str,
) -> None:
    if not spark.catalog.tableExists(full_name):
        return
    target_cols = {f.name: f.dataType.simpleString() for f in spark.table(full_name).schema.fields}
    source_cols = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    if load_type == "FULL":
        return
    missing = [c for c in source_cols if c not in target_cols]
    if missing:
        raise SchemaValidationError(
            f"Schema mismatch writing {full_name}: source has extra columns {missing}. "
            "Use LOAD_TYPE=FULL to rebuild or align TRANSFORM_QUERY."
        )
    type_mismatch = [
        c
        for c in source_cols
        if c in target_cols and _compat(source_cols[c], target_cols[c]) is False
    ]
    if type_mismatch:
        raise SchemaValidationError(
            f"Schema type mismatch on {full_name}: {type_mismatch} "
            f"source={ {c: source_cols[c] for c in type_mismatch} } "
            f"target={ {c: target_cols[c] for c in type_mismatch} }"
        )


def _compat(a: str, b: str) -> bool:
    if a == b:
        return True
    family = {
        "int": "numeric",
        "bigint": "numeric",
        "smallint": "numeric",
        "tinyint": "numeric",
        "double": "numeric",
        "float": "numeric",
        "decimal": "numeric",
        "string": "string",
        "varchar": "string",
    }
    def fam(t: str) -> str:
        t = t.lower()
        if t.startswith("decimal"):
            return "numeric"
        if t.startswith("varchar"):
            return "string"
        return family.get(t, t)
    return fam(a) == fam(b)


def add_load_ts(df: DataFrame) -> DataFrame:
    if "_framework_load_ts" in df.columns:
        return df
    return df.withColumn("_framework_load_ts", F.current_timestamp())
