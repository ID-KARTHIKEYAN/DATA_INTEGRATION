import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..utils.structured_logger import StructuredLogger
from ..utils.schema_validator import SchemaValidator
from ..utils.spark_utils import SparkUtils
from ..utils.audit_manager import AuditManager


class Transformer:
    def __init__(self, spark, dbutils=None, logger: Optional[StructuredLogger] = None,
                 audit_manager: Optional[AuditManager] = None):
        self.spark = spark
        self.dbutils = dbutils
        self.logger = logger or StructuredLogger()
        self.audit = audit_manager
        self.schema_validator = SchemaValidator()

    def add_audit_columns(self, df: DataFrame, group_id: str, layer: str,
                        lob: Optional[str] = None, environment: Optional[str] = None,
                        extra: Optional[Dict[str, Any]] = None) -> DataFrame:
        if df is None:
            return df
        result = df
        try:
            result = result.withColumn("_etl_group_id", F.lit(group_id))
            result = result.withColumn("_etl_layer", F.lit(layer))
            if lob:
                result = result.withColumn("_etl_lob", F.lit(lob))
            if environment:
                result = result.withColumn("_etl_env", F.lit(environment))
            result = result.withColumn("_etl_load_ts", F.current_timestamp())
            if extra:
                for k, v in extra.items():
                    col_name = re.sub(r"[^A-Za-z0-9_]", "_", str(k))
                    if col_name and not col_name.startswith("_"):
                        col_name = "_" + col_name
                    result = result.withColumn(col_name, F.lit(str(v)))
        except Exception as e:
            self.logger.warn("Failed to add one or more audit columns", error=str(e)[:150])
        return result

    def execute_transform_sql(self, transform_query: str,
                              catalog: Optional[str] = None,
                              default_schema: Optional[str] = None,
                              template_params: Optional[Dict[str, Any]] = None,
                              source_df: Optional[DataFrame] = None) -> Tuple[DataFrame, Dict[str, Any]]:
        info: Dict[str, Any] = {
            "original_query": transform_query[:2000] if transform_query else None,
            "modified": False,
            "warnings": [],
        }
        if not transform_query or not str(transform_query).strip():
            raise ValueError("TRANSFORM_QUERY is empty")
        sql = str(transform_query).strip()
        if template_params:
            sql2 = SparkUtils.inject_template_params(sql, template_params)
            if sql2 != sql:
                info["modified"] = True
                sql = sql2
        if catalog:
            sql_prefixed = SparkUtils.apply_sql_catalog_prefix(sql, catalog, default_schema)
            if sql_prefixed != sql:
                info["catalog_prefix_applied"] = True
                sql = sql_prefixed
        if source_df is not None:
            src_cols = list(source_df.columns)
            unresolved = SchemaValidator.find_unresolved_columns(sql, src_cols, catalog, default_schema)
            if unresolved:
                info["potential_unresolved_columns"] = unresolved
                self.logger.warn("Potential unresolved columns in TRANSFORM_QUERY",
                               candidates=unresolved[:10])
        self.logger.debug("Executing transform SQL", query_preview=sql[:300])
        try:
            df = self.spark.sql(sql)
        except Exception as e:
            error_detail = f"TRANSFORM_QUERY execution failed: {type(e).__name__}: {str(e)[:500]}"
            info["error"] = error_detail
            hint = self._generate_hint(sql, str(e), source_df)
            if hint:
                info["hint"] = hint
                error_detail += f"\n  HINT: {hint}"
            raise RuntimeError(error_detail) from e
        info["output_rows"] = int(df.count())
        info["output_columns"] = list(df.columns)
        return df, info

    def apply_l0_transform_map(self, df: DataFrame, transform_query: Any) -> Tuple[DataFrame, Dict[str, Any]]:
        df_new, warnings = SchemaValidator.apply_transform_query_map(df, transform_query)
        info = {"warnings": warnings, "changes_applied": False}
        if df_new is not df:
            info["changes_applied"] = True
        if warnings:
            self.logger.warn("L0 TRANSFORM_QUERY map had issues", summary=str(warnings)[:300])
        return df_new, info

    def execute_generic_scripts(self, generic_scripts: Any,
                                 custom_script_params: Any = None,
                                 template_params: Optional[Dict[str, Any]] = None) -> str:
        if not generic_scripts or not str(generic_scripts).strip():
            return "no-op: GENERIC_SCRIPTS empty"
        scripts_str = str(generic_scripts).strip()
        params = SparkUtils.parse_map_param(custom_script_params) if custom_script_params else {}
        if template_params:
            for k, v in template_params.items():
                params.setdefault(k, v)
        scripts_list: List[str] = []
        if scripts_str.strip().startswith("{") or scripts_str.strip().startswith("["):
            try:
                import json
                parsed = json.loads(scripts_str)
                if isinstance(parsed, list):
                    scripts_list = [str(s) for s in parsed if str(s).strip()]
                elif isinstance(parsed, dict):
                    scripts_list = [str(v) for v in parsed.values() if str(v).strip()]
            except Exception:
                scripts_list = [scripts_str]
        elif "||" in scripts_str:
            scripts_list = [s.strip() for s in scripts_str.split("||") if s.strip()]
        elif ";;" in scripts_str:
            scripts_list = [s.strip() + ";" for s in scripts_str.split(";;") if s.strip()]
        else:
            scripts_list = [scripts_str]
        results = []
        for i, script in enumerate(scripts_list, 1):
            injected = SparkUtils.inject_template_params(script, params)
            try:
                if self._looks_like_sql(injected):
                    self.logger.debug(f"Running generic SQL script #{i}",
                                     preview=injected[:200])
                    res = self.spark.sql(injected)
                    try:
                        n = res.count()
                        results.append(f"script#{i}: SQL returned {n} rows")
                    except Exception:
                        results.append(f"script#{i}: SQL executed")
                else:
                    self.logger.debug(f"Running generic Python script #{i}", length=len(injected))
                    exec_globals = {
                        "spark": self.spark,
                        "dbutils": self.dbutils,
                        "F": F,
                        "params": params,
                        "logger": self.logger,
                    }
                    exec(injected, exec_globals)
                    results.append(f"script#{i}: Python executed")
            except Exception as e:
                msg = (f"GENERIC_SCRIPTS error in #{i}: "
                       f"{type(e).__name__}: {str(e)[:250]}")
                self.logger.error(msg, exception=e, script_index=i)
                raise RuntimeError(msg) from e
        return "; ".join(results)

    def apply_dq_logic(self, df: DataFrame, dq_logic: Optional[str]) -> Tuple[DataFrame, Dict[str, Any]]:
        info: Dict[str, Any] = {"applied": False, "valid_rows": 0, "invalid_rows": 0}
        if not dq_logic or not str(dq_logic).strip():
            info["valid_rows"] = int(df.count())
            return df, info
        logic = str(dq_logic).strip()
        initial_count = int(df.count())
        try:
            filtered = df.filter(F.expr(logic))
            valid_count = int(filtered.count())
            info["applied"] = True
            info["valid_rows"] = valid_count
            info["invalid_rows"] = initial_count - valid_count
            info["before"] = initial_count
            self.logger.info("Data quality filter applied", logic=logic[:120],
                           invalid=info["invalid_rows"])
            return filtered, info
        except Exception as e:
            raise RuntimeError(f"DQ_LOGIC execution failed ({logic[:80]}): {e}") from e

    @staticmethod
    def _looks_like_sql(text: str) -> bool:
        if not text:
            return False
        head = text.strip().lstrip("(").lstrip("{").strip()[:30].upper()
        return any(head.startswith(kw) for kw in (
            "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP",
            "WITH", "SET", "SHOW", "DESCRIBE", "EXPLAIN", "OPTIMIZE", "VACUUM", "COPY"
        ))

    def _generate_hint(self, sql: str, error_str: str, source_df: Optional[DataFrame]) -> Optional[str]:
        low = error_str.lower()
        if "cannot resolve" in low or "column is not" in low or "undefined column" in low:
            m = re.search(r"`?([A-Za-z_][A-Za-z0-9_]*)`?", error_str)
            if m and source_df is not None:
                bad_col = m.group(1)
                available = list(source_df.columns)
                closest = sorted(available, key=lambda c: abs(len(c) - len(bad_col)))[:5]
                return (f"Column '{bad_col}' not found. Available columns: "
                        f"{available[:15]} — did you mean any of {closest}?")
        if "table or view not found" in low:
            return (f"Check that your TRANSFORM_QUERY uses 3-part naming "
                    f"(catalog.schema.table) or verify the source exists.")
        if "mismatched input" in low:
            return "Check SQL syntax in TRANSFORM_QUERY for mismatched commas, parens, or keywords."
        return None
