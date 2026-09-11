from typing import Any, Dict, List, Optional, Set, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, DataType, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, TimestampType, DateType, DecimalType,
    MapType, ArrayType, BinaryType
)
from pyspark.sql import functions as F
from datetime import datetime


class SchemaValidator:
    TYPE_MAP_SPARK = {
        "STRING": StringType,
        "VARCHAR": StringType,
        "CHAR": StringType,
        "INT": IntegerType,
        "INTEGER": IntegerType,
        "BIGINT": LongType,
        "LONG": LongType,
        "DOUBLE": DoubleType,
        "FLOAT": FloatType,
        "DECIMAL": DecimalType,
        "NUMERIC": DecimalType,
        "BOOL": BooleanType,
        "BOOLEAN": BooleanType,
        "TIMESTAMP": TimestampType,
        "DATETIME": TimestampType,
        "DATE": DateType,
        "BINARY": BinaryType,
    }

    @classmethod
    def _parse_type(cls, type_str: str) -> Optional[DataType]:
        if not type_str:
            return None
        raw = type_str.strip().upper()
        paren_pos = raw.find("(")
        base = raw[:paren_pos] if paren_pos > 0 else raw
        base = base.strip()
        if base in ("DECIMAL", "NUMERIC") and paren_pos > 0:
            inner = raw[paren_pos + 1:raw.rfind(")")]
            parts = [p.strip() for p in inner.split(",")]
            try:
                p_val = int(parts[0])
                s_val = int(parts[1]) if len(parts) > 1 else 0
                return DecimalType(p_val, s_val)
            except (ValueError, IndexError):
                return DecimalType(18, 2)
        if base in cls.TYPE_MAP_SPARK:
            if base in ("DATE", "TIMESTAMP", "DATETIME", "BOOL", "BOOLEAN"):
                return cls.TYPE_MAP_SPARK[base]()
            return cls.TYPE_MAP_SPARK[base]()
        return None

    @staticmethod
    def compare_dataframes(df_expected: DataFrame, df_actual: DataFrame,
                           ignore_order: bool = True, case_sensitive: bool = False) -> Tuple[bool, Dict[str, Any]]:
        issues: Dict[str, Any] = {}
        expected_cols = [(c, t) for c, t in df_expected.dtypes]
        actual_cols = [(c, t) for c, t in df_actual.dtypes]
        if not case_sensitive:
            expected_lookup = {c.lower(): t for c, t in expected_cols}
            actual_lookup = {c.lower(): t for c, t in actual_cols}
            actual_names_lower = {c.lower(): c for c, _ in actual_cols}
            missing = [c for c in expected_lookup if c not in actual_lookup]
            if missing:
                issues["missing_columns"] = missing
            extra = [c for c in actual_lookup if c not in expected_lookup]
            if extra:
                issues["extra_columns"] = extra
            type_mismatches = {}
            for cl in expected_lookup:
                if cl in actual_lookup and expected_lookup[cl] != actual_lookup[cl]:
                    type_mismatches[actual_names_lower.get(cl, cl)] = {
                        "expected": expected_lookup[cl],
                        "actual": actual_lookup[cl]
                    }
            if type_mismatches:
                issues["type_mismatches"] = type_mismatches
        else:
            expected_names = {c for c, _ in expected_cols}
            actual_names = {c for c, _ in actual_cols}
            missing = expected_names - actual_names
            extra = actual_names - expected_names
            if missing:
                issues["missing_columns"] = list(missing)
            if extra:
                issues["extra_columns"] = list(extra)
            expected_types = dict(expected_cols)
            actual_types = dict(actual_cols)
            type_mismatches = {}
            for c in expected_names & actual_names:
                if expected_types[c] != actual_types[c]:
                    type_mismatches[c] = {
                        "expected": expected_types[c],
                        "actual": actual_types[c]
                    }
            if type_mismatches:
                issues["type_mismatches"] = type_mismatches
        is_valid = len(issues) == 0
        return is_valid, issues

    @staticmethod
    def find_unresolved_columns(transform_sql: str, df_or_cols,
                                catalog: Optional[str] = None,
                                schema: Optional[str] = None) -> List[str]:
        unresolved = []
        import re
        tokens = re.findall(r"(\b[A-Za-z_][A-Za-z0-9_]*\b)", transform_sql or "")
        if isinstance(df_or_cols, DataFrame):
            available = set(c.lower() for c in df_or_cols.columns)
        else:
            available = set(str(c).lower() for c in (df_or_cols or []))
        sql_keywords = {
            "select", "from", "where", "and", "or", "not", "null", "as", "on",
            "join", "left", "right", "inner", "outer", "full", "cross", "group",
            "by", "order", "having", "limit", "offset", "union", "all", "case",
            "when", "then", "else", "end", "cast", "coalesce", "trim", "lower",
            "upper", "substring", "concat", "is", "in", "like", "between", "current_timestamp",
            "true", "false", "distinct", "count", "sum", "avg", "max", "min",
            "row_number", "over", "partition", "asc", "desc", "date", "timestamp",
            "string", "int", "bigint", "double", "boolean", "decimal"
        }
        if catalog:
            sql_keywords.add(catalog.lower())
        if schema:
            sql_keywords.add(schema.lower())
        for t in tokens:
            tl = t.lower()
            if tl in sql_keywords:
                continue
            if re.match(r"^.*\d.*$", t):
                pass
            if tl not in available and not t.startswith("_") and len(t) > 1:
                pass
            unresolved_potential = True
            if tl in available:
                unresolved_potential = False
            elif re.match(r"^(col|count|sum|avg|max|min|cast|trim|upper|lower|"
                          r"current_timestamp|date_format|to_date|to_timestamp|"
                          r"substr|substring|concat|split|coalesce|nvl|regexp|"
                          r"replace|left|right|length|abs|round|floor|ceil|"
                          r"explode|size|array|map|struct|row_number|rank|"
                          r"dense_rank|percent_rank|ntile|lead|lag|first_value|"
                          r"last_value|datediff|months_between|add_months|"
                          r"year|month|day|hour|minute|second|weekofyear|"
                          r"quarter|dayofweek|dayofmonth|dayofyear|initcap|"
                          r"lpad|rpad|ltrim|rtrim|reverse|translate|regexp_replace|"
                          r"parse_json|to_json|from_json|get_json_object|"
                          r"if|when|nullif)$", tl):
                unresolved_potential = False
            elif re.match(r"^\d+(\.\d+)?$", tl):
                unresolved_potential = False
            if unresolved_potential:
                unresolved.append(t)
        seen = set()
        deduped = []
        for u in unresolved:
            if u.lower() not in seen:
                seen.add(u.lower())
                deduped.append(u)
        return deduped

    @staticmethod
    def apply_transform_query_map(df: DataFrame, transform_query: Any) -> Tuple[DataFrame, Dict[str, Any]]:
        warnings: Dict[str, Any] = {}
        if not transform_query:
            return df, warnings
        col_map = None
        if isinstance(transform_query, dict):
            col_map = {str(k).strip(): str(v).strip() for k, v in transform_query.items() if str(k).strip()}
        elif isinstance(transform_query, str) and transform_query.strip():
            try:
                import json
                col_map = json.loads(transform_query.strip())
                if not isinstance(col_map, dict):
                    warnings["transform_query_parsed_not_map"] = True
                    col_map = None
            except Exception as e:
                warnings["transform_query_parse_error"] = str(e)
                col_map = None
        if not col_map:
            return df, warnings
        df_cols = set(c.lower() for c in df.columns)
        for col_name, cast_expr in col_map.items():
            if col_name.lower() not in df_cols:
                warnings.setdefault("missing_columns_in_transform", []).append(col_name)
                continue
            try:
                expr = cast_expr.strip()
                if not expr:
                    continue
                if expr.lower().startswith("cast(") or " as " in expr.lower():
                    df = df.withColumn(col_name, F.expr(cast_expr))
                else:
                    df = df.withColumn(col_name, F.col(col_name).cast(cast_expr))
            except Exception as e:
                warnings.setdefault("cast_errors", {})[col_name] = str(e)
        return df, warnings

    @staticmethod
    def check_merge_key_nulls(df: DataFrame, merge_keys: List[str],
                              threshold: float = 0.0) -> Tuple[bool, Dict[str, float]]:
        if not merge_keys:
            return True, {}
        total = df.count()
        if total == 0:
            return True, {}
        key_stats = {}
        all_ok = True
        for k in merge_keys:
            if k not in df.columns:
                key_stats[k] = -1.0
                all_ok = False
                continue
            nulls = df.filter(F.col(k).isNull()).count()
            ratio = nulls / total
            key_stats[k] = ratio
            if ratio > threshold:
                all_ok = False
        return all_ok, key_stats
