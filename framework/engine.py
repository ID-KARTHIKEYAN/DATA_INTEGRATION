from datetime import datetime, timezone

from .errors import DataQualityError, MetadataError
from .identifiers import qualified_name


def _value(row, key, required=False):
    value = row.get(key)
    if required and (value is None or not str(value).strip()):
        raise MetadataError(f"Required metadata column {key} is empty")
    return value


def validate_metadata(task, header):
    row = task.row
    if task.layer == "L0":
        for key in ("SOURCE", "SOURCE_OBJ_SCHEMA", "SOURCE_OBJ_NAME", "INPUT_FILE_FORMAT", "LOAD_TYPE"):
            _value(row, key, True)
    else:
        for key in ("TARGET_OBJ_SCHEMA", "TARGET_OBJ_NAME", "TARGET_OBJ_TYPE", "TRANSFORM_QUERY", "LOAD_TYPE"):
            _value(row, key, True)
        if str(row["TARGET_OBJ_TYPE"]).upper() not in {"TABLE", "MV"}:
            raise MetadataError(f"Unsupported TARGET_OBJ_TYPE: {row['TARGET_OBJ_TYPE']!r}")
    if str(header.get("target_catalog") or "").strip() == "":
        raise MetadataError("Header target_catalog is required")


def _load_type(row):
    value = str(row.get("LOAD_TYPE") or "").upper()
    aliases = {"DELTA": "MERGE", "INCREMENTAL": "APPEND", "SCD": "MERGE"}
    value = aliases.get(value, value)
    if value not in {"FULL", "APPEND", "OVERWRITE", "MERGE"}:
        raise MetadataError(f"Unsupported LOAD_TYPE: {value!r}")
    return value


def _source_df(spark, row):
    reader = spark.read.format(str(row["INPUT_FILE_FORMAT"]).strip())
    delimiter = row.get("DELIMETER")
    if delimiter:
        reader = reader.option("delimiter", delimiter)
    custom_schema = row.get("CUSTOM_SCHEMA")
    if custom_schema:
        reader = reader.schema(custom_schema)
    return reader.option("header", "true").load(str(row["SOURCE"]).strip())


def _apply_metadata_filters(df, task, audit):
    row = task.row
    dq_logic = row.get("DQ_LOGIC") if task.layer == "L0" else None
    cdc_logic = row.get("CDC_LOGIC") if task.layer == "L0" else None
    if dq_logic:
        df = df.filter(str(dq_logic))
    if cdc_logic:
        last_load_ts = audit.last_success(task)
        logic = str(cdc_logic).replace("{last_load_ts}", "NULL" if last_load_ts is None else f"TIMESTAMP '{last_load_ts}'")
        df = df.filter(logic)
    return df


def _assert_query_columns(df, task):
    if not df.columns:
        raise DataQualityError(f"{task.task_id}: transformation returned no columns")


def run_task(spark, task, header, audit):
    validate_metadata(task, header)
    catalog = header["target_catalog"]
    started = datetime.now(timezone.utc)
    target_schema = task.row.get("SOURCE_OBJ_SCHEMA") if task.layer == "L0" else task.row.get("TARGET_OBJ_SCHEMA")
    target_name = task.row.get("SOURCE_OBJ_NAME") if task.layer == "L0" else task.row.get("TARGET_OBJ_NAME")
    target = qualified_name(catalog, target_schema, target_name)
    try:
        df = _source_df(spark, task.row) if task.layer == "L0" else spark.sql(task.row["TRANSFORM_QUERY"])
        df = _apply_metadata_filters(df, task, audit)
        _assert_query_columns(df, task)
        load_type = _load_type(task.row)
        if task.layer != "L0" and str(task.row["TARGET_OBJ_TYPE"]).upper() == "MV":
            raise MetadataError("TARGET_OBJ_TYPE=MV is not executable in this Free Edition framework; use Table")
        if load_type == "MERGE":
            keys = [key.strip() for key in str(task.row.get("TARGET_PK") or task.row.get("SOURCE_PK") or "").split(",") if key.strip()]
            if not keys:
                raise MetadataError(f"{task.task_id}: MERGE requires SOURCE_PK or TARGET_PK")
            missing = [key for key in keys if key not in df.columns]
            if missing:
                raise DataQualityError(f"{task.task_id}: merge keys missing from result: {missing}")
            if not spark.catalog.tableExists(target):
                df.write.format("delta").mode("overwrite").saveAsTable(target)
            else:
                df.createOrReplaceTempView("__etl_source")
                predicate = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
                assignments = ", ".join(f"t.`{column}` = s.`{column}`" for column in df.columns)
                spark.sql(f"MERGE INTO {target} t USING __etl_source s ON {predicate} WHEN MATCHED THEN UPDATE SET {assignments} WHEN NOT MATCHED THEN INSERT *")
        else:
            mode = "overwrite" if load_type in {"FULL", "OVERWRITE"} else "append"
            df.write.format("delta").mode(mode).saveAsTable(target)
        rows = df.count()
        audit.success(task, target, rows, started)
        return {"task_id": task.task_id, "target": target, "rows": rows}
    except Exception as exc:
        audit.failure(task, target, str(exc), started)
        raise