# =============================================================================
# framework/spark_config_manager.py
# Applies SPARK_CONFIGS from data_flow_control_header to the active SparkSession.
# Also handles MIN_VERSION / MAX_VERSION wheel resolution.
# =============================================================================

from __future__ import annotations
import json
from typing import Optional


class SparkConfigManager:
    """
    Applies per-pipeline Spark configuration from control_header metadata.

    SPARK_CONFIGS format (stored as JSON string in control_header):
        '{"spark.sql.shuffle.partitions": "8",
          "spark.databricks.delta.optimizeWrite.enabled": "true"}'
    """

    def __init__(self, spark):
        self.spark = spark

    def apply_spark_configs(self, spark_configs_raw: Optional[str]) -> None:
        """
        Parses SPARK_CONFIGS JSON string and calls spark.conf.set for each key.
        Skips silently if empty. Warns (does not fail) if JSON is malformed.
        """
        if not spark_configs_raw or not spark_configs_raw.strip():
            return

        try:
            configs = json.loads(spark_configs_raw)
        except json.JSONDecodeError as e:
            print(
                f"  ⚠ SPARK_CONFIGS is not valid JSON — skipping. "
                f"Expected format: {{\"key\": \"value\", ...}}. "
                f"Parse error: {e}"
            )
            return

        if not isinstance(configs, dict):
            print(
                f"  ⚠ SPARK_CONFIGS must be a JSON object (dict). "
                f"Got: {type(configs).__name__}. Skipping."
            )
            return

        for k, v in configs.items():
            try:
                self.spark.conf.set(str(k), str(v))
                print(f"  SparkConf: {k} = {v}")
            except Exception as e:
                print(f"  ⚠ SparkConf.set('{k}', '{v}') failed: {e}")

    def resolve_compute_class(
        self,
        compute_dev: Optional[str],
        compute_prod: Optional[str],
        environment: str,
    ) -> str:
        """
        Returns the correct compute class based on the environment.
        Free Edition note: both should be 'serverless'.
        Warns if non-serverless compute is configured.
        """
        env = (environment or "dev").lower()
        if env == "dev":
            cls = (compute_dev or "serverless").lower()
        else:
            cls = (compute_prod or "serverless").lower()

        if cls != "serverless":
            print(
                f"  ⚠ COMPUTE_CLASS='{cls}' detected for env='{env}'. "
                f"Databricks Free Edition only supports 'serverless' compute. "
                f"Non-serverless configurations may fail job creation."
            )
        return cls
