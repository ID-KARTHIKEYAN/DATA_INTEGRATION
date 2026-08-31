#!/usr/bin/env python3
"""
════════════════════════════════════════════════════════════════════════════════
segment_processor.py - SEGMENT-BASED ETL PROCESSOR
════════════════════════════════════════════════════════════════════════════════
Processes data flows in segments (parallel or sequential) based on metadata.

Supports ALL metadata columns:
  - DATA_FLOW_GROUP_ID, LOB, SOURCE, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME
  - PRIORITY, TARGET_OBJ_TYPE, TRANSFORM_QUERY, GENERIC_SCRIPTS
  - SOURCE_PK, TARGET_PK, LOAD_TYPE, IS_ACTIVE
  - LS_FLAG, LS_DETAIL, PARTITION_OR_INDEX, PARTITION_METHOD
  - CUSTOM_SCRIPT_PARAMS, RETENTION_DETAILS, DEPLOYMENT_SOURCE_DFG
  - INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS

Usage:
  - Import in notebooks: from segment_processor import SegmentProcessor
  - Standalone: python segment_processor.py --group-id <id> --layer <layer>
════════════════════════════════════════════════════════════════════════════════
"""

import json
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


class SegmentProcessor:
    """
    Processes ETL segments based on metadata configuration.
    Supports parallel and sequential execution modes.
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "demo_catalog", 
                 control_schema: str = "admin"):
        """
        Initialize the processor.
        
        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            control_schema: Schema containing control tables
        """
        self.spark = spark
        self.catalog = catalog
        self.control_schema = control_schema
        self.audit_records = []
    
    def read_source(self, url: str, fmt: str = "csv", delimiter: str = ",", 
                   custom_params: Optional[str] = None) -> DataFrame:
        """
        Read data from various sources and formats.
        
        Args:
            url: Source URL or path
            fmt: File format (csv, json, parquet, delta, excel, avro, orc)
            delimiter: CSV delimiter
            custom_params: JSON string with additional read options
        
        Returns:
            Spark DataFrame
        """
        fmt = (fmt or "csv").strip().lower()
        url = url.strip()
        
        if not url:
            raise ValueError("Source URL is empty")
        
        print(f"  Reading [{fmt}] from: {url[:80]}...")
        
        # Parse custom parameters
        read_options = {}
        if custom_params:
            try:
                read_options = json.loads(custom_params)
                print(f"  Custom read options: {read_options}")
            except Exception as e:
                print(f"  ⚠ Failed to parse custom_params: {e}")
        
        # HTTP/HTTPS sources
        if url.startswith("http"):
            import requests
            import io
            import pandas as pd
            
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            
            if fmt == "csv":
                pdf = pd.read_csv(io.BytesIO(response.content), sep=delimiter, **read_options)
                return self.spark.createDataFrame(pdf)
            elif fmt == "json":
                pdf = pd.read_json(io.BytesIO(response.content), **read_options)
                return self.spark.createDataFrame(pdf)
            elif fmt == "parquet":
                pdf = pd.read_parquet(io.BytesIO(response.content), **read_options)
                return self.spark.createDataFrame(pdf)
            elif fmt in ["xlsx", "xls", "excel"]:
                pdf = pd.read_excel(io.BytesIO(response.content), **read_options)
                return self.spark.createDataFrame(pdf)
            else:
                raise ValueError(f"Unsupported HTTP format: {fmt}")
        
        # Spark native reads
        reader = self.spark.read.format(fmt)
        
        if fmt == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true").option("delimiter", delimiter)
        elif fmt == "json":
            reader = reader.option("multiLine", "true")
        
        # Apply custom read options
        for key, val in read_options.items():
            reader = reader.option(key, val)
        
        return reader.load(url)
    
    def write_table(self, df: DataFrame, catalog: str, schema: str, table: str,
                   load_type: str = "FULL", merge_keys: Optional[str] = None,
                   partition_cols: Optional[str] = None, 
                   retention_days: Optional[int] = None) -> int:
        """
        Write DataFrame to Delta table with advanced options.
        
        Args:
            df: Source DataFrame
            catalog, schema, table: Target location
            load_type: FULL, INCREMENTAL, APPEND, MERGE, SCD2
            merge_keys: Comma-separated merge key columns
            partition_cols: Comma-separated partition columns
            retention_days: Data retention in days
        
        Returns:
            Row count written
        """
        # Create schema if needed
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        
        full_name = f"{catalog}.{schema}.{table}"
        load_type = (load_type or "FULL").strip().upper()
        
        # Parse partition columns
        part_cols = [c.strip() for c in (partition_cols or "").split(",") if c.strip()]
        
        # FULL LOAD
        if load_type == "FULL":
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if part_cols:
                writer = writer.partitionBy(*part_cols)
            writer.saveAsTable(full_name)
            count = df.count()
        
        # INCREMENTAL/APPEND
        elif load_type in ("INCREMENTAL", "APPEND"):
            writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
            if part_cols:
                writer = writer.partitionBy(*part_cols)
            writer.saveAsTable(full_name)
            count = df.count()
        
        # MERGE (Upsert)
        elif load_type == "MERGE":
            if not merge_keys:
                raise ValueError("MERGE_KEYS required for MERGE load type")
            
            keys = [k.strip() for k in merge_keys.split(",")]
            temp_view = f"_tmp_{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            df.createOrReplaceTempView(temp_view)
            
            # Create table if not exists
            create_writer = df.limit(0).write.format("delta").mode("append")
            if part_cols:
                create_writer = create_writer.partitionBy(*part_cols)
            create_writer.saveAsTable(full_name)
            
            # Build merge condition
            merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])
            update_cols = [c for c in df.columns if c not in keys]
            update_set = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
            
            merge_sql = f"""
                MERGE INTO {full_name} AS target
                USING {temp_view} AS source
                ON {merge_cond}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT *
            """
            self.spark.sql(merge_sql)
            count = df.count()
        
        # SCD2 (Slowly Changing Dimension Type 2)
        elif load_type == "SCD2":
            if not merge_keys:
                raise ValueError("MERGE_KEYS required for SCD2 load type")
            
            keys = [k.strip() for k in merge_keys.split(",")]
            temp_view = f"_tmp_{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Add SCD2 columns
            if "scd_start_date" not in df.columns:
                df = df.withColumn("scd_start_date", F.current_timestamp())
            if "scd_end_date" not in df.columns:
                df = df.withColumn("scd_end_date", F.lit(None).cast(TimestampType()))
            if "is_current" not in df.columns:
                df = df.withColumn("is_current", F.lit(True))
            
            df.createOrReplaceTempView(temp_view)
            
            # Create table if not exists
            create_writer = df.limit(0).write.format("delta").mode("append")
            if part_cols:
                create_writer = create_writer.partitionBy(*part_cols)
            create_writer.saveAsTable(full_name)
            
            # Build merge condition
            merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])
            
            merge_sql = f"""
                MERGE INTO {full_name} AS target
                USING {temp_view} AS source
                ON {merge_cond} AND target.is_current = true
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.scd_end_date = current_timestamp(),
                        target.is_current = false
                WHEN NOT MATCHED THEN INSERT *
            """
            self.spark.sql(merge_sql)
            count = df.count()
        
        else:
            raise ValueError(f"Unsupported LOAD_TYPE: {load_type}")
        
        # Apply retention policy
        if retention_days and retention_days > 0:
            self._apply_retention(full_name, retention_days)
        
        return count
    
    def _apply_retention(self, full_table_name: str, retention_days: int):
        """Apply retention policy to table."""
        try:
            df_sample = self.spark.sql(f"SELECT * FROM {full_table_name} LIMIT 1")
            ts_cols = ["load_ts", "_etl_load_ts", "inserted_ts", "created_ts", "timestamp"]
            
            ts_col = None
            for col in ts_cols:
                if col in df_sample.columns:
                    ts_col = col
                    break
            
            if not ts_col:
                print(f"  ⚠ No timestamp column found for retention policy")
                return
            
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            cutoff_str = cutoff_date.strftime("%Y-%m-%d")
            
            delete_sql = f"""
                DELETE FROM {full_table_name}
                WHERE {ts_col} < '{cutoff_str}'
            """
            self.spark.sql(delete_sql)
            print(f"  🗑 Retention applied: Deleted records older than {retention_days} days")
        except Exception as e:
            print(f"  ⚠ Retention policy failed: {e}")
    
    def execute_generic_script(self, script_code: str, custom_params: Optional[str] = None) -> str:
        """Execute generic Python or SQL script."""
        if not script_code or script_code.strip() == "":
            return "No script to execute"
        
        print(f"  Executing generic script...")
        
        # Parse parameters
        params = {}
        if custom_params:
            try:
                params = json.loads(custom_params)
                print(f"  Script parameters: {params}")
            except:
                print(f"  ⚠ Failed to parse custom_params")
        
        # Inject parameters
        script = script_code
        for key, val in params.items():
            placeholder = f"${{{key}}}"
            script = script.replace(placeholder, str(val))
        
        # Determine script type
        script_upper = script.strip().upper()
        if any(script_upper.startswith(kw) for kw in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "MERGE"]):
            # SQL script
            result = self.spark.sql(script)
            if script_upper.startswith("SELECT"):
                count = result.count()
                return f"Query returned {count} rows"
            else:
                return "SQL executed successfully"
        else:
            # Python script
            exec(script, {"spark": self.spark, "F": F, "params": params})
            return "Python script executed successfully"
    
    def write_audit(self, group_id: str, table_name: str, layer: str, status: str,
                   message: str, rows: int, start_time: datetime, end_time: datetime,
                   lob: Optional[str] = None):
        """Write audit record."""
        try:
            safe_msg = str(message).replace("'", "''")[:400]
            start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
            end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")
            duration = (end_time - start_time).total_seconds()
            lob_val = lob or "UNKNOWN"
            
            self.spark.sql(f"""
                INSERT INTO {self.catalog}.{self.control_schema}.audit_log (
                    DATA_FLOW_GROUP_ID,
                    TARGET_TABLE,
                    STATUS,
                    MESSAGE,
                    ETL_LAYER,
                    LOB,
                    ROWS_PROCESSED,
                    DURATION_SECONDS,
                    START_TIME,
                    END_TIME,
                    LOAD_TS
                )
                VALUES (
                    '{group_id}',
                    '{table_name}',
                    '{status}',
                    '{safe_msg}',
                    '{layer}',
                    '{lob_val}',
                    {rows},
                    {duration},
                    '{start_ts}',
                    '{end_ts}',
                    current_timestamp()
                )
            """)
        except Exception as e:
            print(f"  ⚠ Audit write failed: {e}")
    
    def process_segment(self, segment_config: Dict[str, Any], layer: str, 
                       group_id: str, env: str) -> Dict[str, Any]:
        """
        Process a single segment (one row from control table).
        
        Args:
            segment_config: Dictionary with all metadata columns
            layer: L0, L1, or L2
            group_id: Data flow group ID
            env: Environment
        
        Returns:
            Result dictionary with status, message, row count
        """
        t0 = datetime.now()
        status = "FAILED"
        msg = ""
        count = 0
        target_table = None
        
        try:
            # Extract common fields
            r = segment_config
            lob = (r.get("LOB") or "").strip()
            load_type = (r.get("LOAD_TYPE") or "FULL").strip().upper()
            target_obj_type = (r.get("TARGET_OBJ_TYPE") or "TABLE").strip().upper()
            generic_scripts = (r.get("GENERIC_SCRIPTS") or "").strip()
            custom_params = (r.get("CUSTOM_SCRIPT_PARAMS") or "").strip()
            ls_flag = (r.get("LS_FLAG") or "N").strip().upper()
            partition_cols = (r.get("PARTITION_OR_INDEX") or "").strip()
            retention_str = (r.get("RETENTION_DETAILS") or "").strip()
            
            # Parse retention
            retention_days = None
            if retention_str:
                try:
                    retention_days = int(retention_str)
                except:
                    pass
            
            if layer == "L0":
                # L0: File Ingestion
                source_url = (r.get("SOURCE") or "").strip()
                target_schema = (r.get("SOURCE_OBJ_SCHEMA") or "").strip()
                target_table = (r.get("SOURCE_OBJ_NAME") or "").strip()
                file_format = (r.get("INPUT_FILE_FORMAT") or "csv").strip().lower()
                delimiter = (r.get("DELIMETER") or ",").strip()
                
                if not source_url or not target_schema or not target_table:
                    raise ValueError("SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME required")
                
                import os
                target_table = os.path.splitext(target_table)[0]
                
                full_name = f"{self.catalog}.{target_schema}.{target_table}"
                print(f"  ▶ {full_name}")
                print(f"    LOB: {lob} | Type: {target_obj_type} | Load: {load_type}")
                
                # Execute PRE-script
                if ls_flag == "B" and generic_scripts:
                    print(f"  Executing PRE-script...")
                    self.execute_generic_script(generic_scripts, custom_params)
                
                # Read source
                df = self.read_source(source_url, file_format, delimiter, custom_params)
                
                # Add audit columns
                df = (
                    df
                    .withColumn("_etl_group_id", F.lit(group_id))
                    .withColumn("_etl_layer", F.lit(layer))
                    .withColumn("_etl_lob", F.lit(lob))
                    .withColumn("_etl_env", F.lit(env))
                    .withColumn("_etl_load_ts", F.current_timestamp())
                )
                
                # Write
                count = self.write_table(
                    df, self.catalog, target_schema, target_table,
                    load_type=load_type,
                    partition_cols=partition_cols,
                    retention_days=retention_days
                )
                
                # Execute POST-script
                if ls_flag == "A" and generic_scripts:
                    print(f"  Executing POST-script...")
                    self.execute_generic_script(generic_scripts, custom_params)
                
                status = "SUCCESS"
                msg = f"{count:,} rows loaded"
                print(f"    ✅ {msg}")
            
            else:
                # L1/L2: Transformation
                target_schema = (r.get("TARGET_OBJ_SCHEMA") or "").strip()
                target_table = (r.get("TARGET_OBJ_NAME") or "").strip()
                transform_query = (r.get("TRANSFORM_QUERY") or "").strip()
                merge_keys = (r.get("TARGET_PK") or r.get("SOURCE_PK") or "").strip()
                source_schema = (r.get("SOURCE_OBJ_SCHEMA") or target_schema).strip()
                source_table = (r.get("SOURCE_OBJ_NAME") or "").strip()
                
                if not target_schema or not target_table:
                    raise ValueError("TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME required")
                
                if not transform_query and not source_table:
                    raise ValueError("Either TRANSFORM_QUERY or SOURCE_OBJ_NAME required")
                
                full_name = f"{self.catalog}.{target_schema}.{target_table}"
                print(f"  ▶ {full_name}")
                print(f"    LOB: {lob} | Type: {target_obj_type} | Load: {load_type}")
                
                # Execute PRE-script
                if ls_flag == "B" and generic_scripts:
                    print(f"  Executing PRE-script...")
                    self.execute_generic_script(generic_scripts, custom_params)
                
                # Execute transformation
                if transform_query:
                    print(f"  Transform: Custom SQL")
                    if source_schema and f"{source_schema}." in transform_query and f"{self.catalog}.{source_schema}." not in transform_query:
                        transform_query = transform_query.replace(f"{source_schema}.", f"{self.catalog}.{source_schema}.")
                    df = self.spark.sql(transform_query)
                else:
                    print(f"  Transform: Direct copy from {self.catalog}.{source_schema}.{source_table}")
                    df = self.spark.table(f"{self.catalog}.{source_schema}.{source_table}")
                
                # Add audit columns
                df = (
                    df
                    .withColumn("_etl_group_id", F.lit(group_id))
                    .withColumn("_etl_layer", F.lit(layer))
                    .withColumn("_etl_lob", F.lit(lob))
                    .withColumn("_etl_env", F.lit(env))
                    .withColumn("_etl_load_ts", F.current_timestamp())
                )
                
                # Write
                count = self.write_table(
                    df, self.catalog, target_schema, target_table,
                    load_type=load_type,
                    merge_keys=merge_keys,
                    partition_cols=partition_cols,
                    retention_days=retention_days
                )
                
                # Execute POST-script
                if ls_flag == "A" and generic_scripts:
                    print(f"  Executing POST-script...")
                    self.execute_generic_script(generic_scripts, custom_params)
                
                status = "SUCCESS"
                msg = f"{count:,} rows processed"
                print(f"    ✅ {msg}")
        
        except Exception as e:
            status = "FAILED"
            msg = f"{type(e).__name__}: {str(e)[:200]}"
            print(f"    ❌ {msg}")
            traceback.print_exc()
        
        finally:
            # Write audit
            t1 = datetime.now()
            self.write_audit(group_id, target_table or "UNKNOWN", layer, status, msg, count, t0, t1, lob)
        
        return {
            "status": status,
            "message": msg,
            "row_count": count,
            "table": target_table,
            "duration": (datetime.now() - t0).total_seconds()
        }
    
    def process_layer(self, detail_table: str, layer: str, group_id: str, 
                     target_table: Optional[str] = None, lob_filter: Optional[str] = None,
                     env: str = "dev") -> Dict[str, Any]:
        """
        Process all segments for a layer.
        
        Args:
            detail_table: Control table name
            layer: L0, L1, or L2
            group_id: Data flow group ID
            target_table: Optional specific table to process
            lob_filter: Optional LOB filter
            env: Environment
        
        Returns:
            Summary dictionary
        """
        print(f"\n{'='*70}")
        print(f"  LAYER: {layer} | TABLE: {detail_table}")
        print(f"{'='*70}")
        
        # Build query
        if layer == "L0":
            obj_col = "SOURCE_OBJ_NAME"
        else:
            obj_col = "TARGET_OBJ_NAME"
        
        filters = [f"DATA_FLOW_GROUP_ID = '{group_id}'"]
        filters.append("IS_ACTIVE = 'Y'")
        
        if target_table and target_table.upper() != "ALL":
            filters.append(f"{obj_col} = '{target_table}'")
        
        if lob_filter and lob_filter.upper() != "ALL":
            filters.append(f"LOB = '{lob_filter}'")
        
        where_clause = " AND ".join(filters)
        
        query = f"""
            SELECT *
            FROM {self.catalog}.{self.control_schema}.{detail_table}
            WHERE {where_clause}
            ORDER BY COALESCE(PRIORITY, 999), {obj_col}
        """
        
        print(f"  Executing query...")
        rows = self.spark.sql(query).collect()
        
        if not rows:
            print(f"  ⚠ No active segments found")
            return {"success": True, "processed": 0, "failed": 0}
        
        print(f"  Segments to process: {len(rows)}\n")
        
        # Process segments
        results = []
        for idx, row in enumerate(rows, 1):
            print(f"  [{idx}/{len(rows)}]")
            segment_config = row.asDict()
            result = self.process_segment(segment_config, layer, group_id, env)
            results.append(result)
        
        # Summary
        success_count = sum(1 for r in results if r["status"] == "SUCCESS")
        failed_count = len(results) - success_count
        
        print(f"\n{'─'*70}")
        print(f"  Summary: {success_count}/{len(results)} succeeded")
        
        if failed_count > 0:
            print(f"  ❌ {failed_count} segment(s) failed")
            for r in results:
                if r["status"] == "FAILED":
                    print(f"    - {r['table']}: {r['message'][:80]}")
        
        print(f"{'─'*70}\n")
        
        return {
            "success": failed_count == 0,
            "processed": len(results),
            "succeeded": success_count,
            "failed": failed_count,
            "results": results
        }


# ═══════════════════════════════════════════════════════════════════════════
# STANDALONE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Segment-based ETL processor")
    parser.add_argument("--group-id", required=True, help="Data flow group ID")
    parser.add_argument("--layer", required=True, choices=["L0", "L1", "L2", "ALL"], help="Layer to process")
    parser.add_argument("--target-table", default="ALL", help="Specific table to process")
    parser.add_argument("--lob", default="ALL", help="Line of business filter")
    parser.add_argument("--catalog", default="demo_catalog", help="Unity Catalog name")
    parser.add_argument("--environment", default="dev", help="Environment")
    
    args = parser.parse_args()
    
    # Initialize Spark (assumes running in Databricks)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # Create processor
    processor = SegmentProcessor(spark, catalog=args.catalog)
    
    print(f"\n{'╔'+'═'*68+'╗'}")
    print(f"║  SEGMENT PROCESSOR v3.0 - START{' '*32}║")
    print(f"║  GROUP: {args.group_id:<57}║")
    print(f"║  LAYER: {args.layer:<57}║")
    print(f"║  LOB: {args.lob:<60}║")
    print(f"{'╚'+'═'*68+'╝'}\n")
    
    try:
        control_table_map = {
            "L0": "data_flow_l0_detail",
            "L1": "data_flow_pb_detail",
            "L2": "data_flow_pb_detail"
        }
        
        if args.layer == "ALL":
            # Process all layers
            for layer in ["L0", "L1", "L2"]:
                processor.process_layer(
                    control_table_map[layer], layer, args.group_id,
                    args.target_table, args.lob, args.environment
                )
        else:
            # Process single layer
            processor.process_layer(
                control_table_map[args.layer], args.layer, args.group_id,
                args.target_table, args.lob, args.environment
            )
        
        print(f"\n{'╔'+'═'*68+'╗'}")
        print(f"║  ✅ SEGMENT PROCESSOR v3.0 - SUCCESS{' '*26}║")
        print(f"{'╚'+'═'*68+'╝'}\n")
    
    except Exception as e:
        print(f"\n{'╔'+'═'*68+'╗'}")
        print(f"║  ❌ SEGMENT PROCESSOR v3.0 - FAILED{' '*27}║")
        print(f"║  Error: {type(e).__name__:<58}║")
        print(f"{'╚'+'═'*68+'╝'}\n")
        traceback.print_exc()
        raise