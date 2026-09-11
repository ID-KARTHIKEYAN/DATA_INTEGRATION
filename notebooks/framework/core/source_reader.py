import io
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..utils.retry_handler import RetryHandler, retry_with_backoff
from ..utils.structured_logger import StructuredLogger
from ..utils.spark_utils import SparkUtils


class SourceReader:
    SUPPORTED_HTTP_FMTS = {"CSV", "TSV", "JSON", "PARQUET", "XLSX", "XLS", "EXCEL", "XML"}

    def __init__(self, spark, dbutils=None, logger: Optional[StructuredLogger] = None):
        self.spark = spark
        self.dbutils = dbutils
        self.logger = logger or StructuredLogger()
        self.retry = RetryHandler(max_attempts=3, base_delay_sec=2.0, max_delay_sec=30.0)

    def _read_http(self, url: str, fmt: str, delimiter: str, custom_params: Optional[Dict[str, str]]) -> DataFrame:
        import requests
        fmt_upper = fmt.upper()
        self.logger.debug(f"HTTP source: fetching {fmt_upper}", url=url[:80])
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        content = resp.content
        if fmt_upper in ("CSV", "TSV"):
            import pandas as pd
            sep = delimiter or ("," if fmt_upper == "CSV" else "\t")
            kwargs = {"sep": sep, "dtype": str}
            if custom_params:
                for k, v in custom_params.items():
                    if k.lower() == "encoding":
                        kwargs["encoding"] = v
                    elif k.lower() == "header":
                        kwargs["header"] = str(v).strip().lower() in ("true", "1", "yes")
                    elif k.lower() in ("low_memory", "keep_default_na"):
                        kwargs[k] = str(v).strip().lower() in ("true", "1", "yes")
            try:
                pdf = pd.read_csv(io.BytesIO(content), **kwargs)
            except UnicodeDecodeError:
                    pdf = pd.read_csv(io.BytesIO(content), sep=sep, encoding="latin-1", dtype=str)
            if pdf is None or len(pdf) == 0:
                pdf = pd.DataFrame()
            df = self.spark.createDataFrame(pdf.astype(str))
            return df
        elif fmt_upper == "JSON":
            import pandas as pd
            pdf = pd.read_json(io.BytesIO(content))
            return self.spark.createDataFrame(pdf.astype(str))
        elif fmt_upper == "PARQUET":
            import pandas as pd
            pdf = pd.read_parquet(io.BytesIO(content))
            return self.spark.createDataFrame(pdf)
        elif fmt_upper in ("XLSX", "XLS", "EXCEL"):
            import pandas as pd
            kwargs = {}
            if custom_params:
                if "sheet_name" in custom_params:
                    kwargs["sheet_name"] = custom_params["sheet_name"]
            pdf = pd.read_excel(io.BytesIO(content), **kwargs)
            return self.spark.createDataFrame(pdf.astype(str))
        elif fmt_upper == "XML":
            import pandas as pd
            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(content)
                rows = []
                columns = set()
                row_tag = delimiter if delimiter and delimiter.strip() else "row"
                for child in root.iter(row_tag):
                    row = {}
                    for sub in child:
                        row[sub.tag] = (sub.text or "").strip() if sub.text else None
                        columns.add(sub.tag)
                    rows.append(row)
                pdf = pd.DataFrame(rows, columns=list(columns))
                return self.spark.createDataFrame(pdf.astype(str))
            except Exception as e:
                    raise ValueError(f"XML parsing failed (row tag='{delimiter or 'row'}': {e}")
        raise ValueError(f"Unsupported HTTP format: {fmt}")

    def _read_spark_native(self, path: str, fmt: str, delimiter: str,
                            custom_params: Optional[Dict[str, str]]) -> DataFrame:
        fmt_lower = fmt.lower()
        reader = self.spark.read.format(fmt_lower)
        if fmt_lower == "csv" or fmt_lower == "tsv":
            sep = delimiter or ("," if fmt_lower == "csv" else "\t")
            reader = reader.option("header", "true") \
                .option("inferSchema", "false") \
                .option("sep", sep) \
                .option("multiLine", "true") \
                .option("escape", "\"")
        elif fmt_lower == "json":
            reader = reader.option("multiLine", "true")
        elif fmt_lower == "xml":
            row_tag = delimiter or "row"
            reader = reader.option("rowTag", row_tag)
        if custom_params:
            for k, v in custom_params.items():
                reader = reader.option(k, v)
        return reader.load(path)

    @retry_with_backoff(max_attempts=3, base_delay_sec=2.0)
    def read(self, source: str, file_format: str, delimiter: Optional[str] = None,
             custom_schema: Optional[str] = None, custom_params: Any = None,
             storage_type: Optional[str] = None) -> DataFrame:
        if not source:
            raise ValueError("SOURCE cannot be empty")
        fmt = (file_format or "csv").strip().upper()
        delim = (delimiter or ",").strip() or ","
        params_dict = SparkUtils.parse_map_param(custom_params) if custom_params else {}
        if custom_schema:
            params_dict["custom_schema"] = custom_schema
        self.logger.info("Reading source", source=source[:80], format=fmt, storage=storage_type or "")
        source_path = source.strip()
        if source_path.startswith("http://") or source_path.startswith("https://"):
            return self._read_http(source_path, fmt, delim, params_dict)
        return self._read_spark_native(source_path, fmt, delim, params_dict)

    def apply_cdc_filter(self, df: DataFrame, cdc_logic: Optional[str],
                        watermark_ts=None) -> Tuple[DataFrame, Dict[str, Any]]:
        info = {"applied": False, "logic": cdc_logic, "watermark_applied": False}
        if not df or not cdc_logic or not str(cdc_logic).strip():
            return df, info
        logic = str(cdc_logic).strip()
        if not logic:
            return df, info
        if watermark_ts:
            wm_str = watermark_ts.strftime("%Y-%m-%d %H:%M:%S")
            logic = logic.replace("${WATERMARK}", f"'{wm_str}'")
            logic = logic.replace("${watermark}", f"'{wm_str}'")
            info["watermark_applied"] = True
            info["watermark"] = wm_str
        try:
            df_filtered = df.filter(F.expr(logic))
            info["before_count"] = int(df.count())
            info["after_count"] = int(df_filtered.count())
            info["applied"] = True
            self.logger.info("CDC filter applied",
                           logic=logic[:120],
                           removed=info["before_count"] - info["after_count"])
            return df_filtered, info
        except Exception as e:
            raise RuntimeError(f"CDC logic failed on CDC_LOGIC='{logic[:100]}': {e}")
