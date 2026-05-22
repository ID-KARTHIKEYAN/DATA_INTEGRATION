# =====================================
# WIDGET PARAMETER
# =====================================

dbutils.widgets.text("GROUP_ID", "")

group_id = dbutils.widgets.get("GROUP_ID")

print(f"Running GROUP_ID : {group_id}")
# =====================================
# IMPORTS
# =====================================

import pandas as pd
from pyspark.sql.functions import current_timestamp
# =====================================
# READ METADATA
# =====================================

control_df = spark.sql(f"""
SELECT *
FROM demo_catalog.admin.data_flow_l0_detail
WHERE DATA_FLOW_GROUP_ID = '{group_id}'
AND IS_ACTIVE = 'Y'
""")

metadata_list = control_df.collect()

# =====================================
# LOOP THROUGH METADATA
# ====================================
for row in metadata_list:

    try:

        source_url = row['SOURCE_URL']
        target_schema = row['TARGET_SCHEMA']
        target_table = row['TARGET_TABLE']
        file_format = row['FILE_FORMAT']

        print(f"Processing : {target_table}")

        # =====================================
        # READ CSV USING PANDAS
        # =====================================

        pandas_df = pd.read_csv(source_url)

        # =====================================
        # CONVERT TO SPARK DATAFRAME
        # =====================================

        source_df = spark.createDataFrame(pandas_df)
        # =====================================
        # CREATE TARGET TABLE
        # =====================================

        full_table_name = f"demo_catalog.{target_schema}.{target_table}"

        source_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)

        print(f"Completed : {full_table_name}")

        # =====================================
        # AUDIT SUCCESS LOG
        # =====================================

        spark.sql(f"""
        INSERT INTO demo_catalog.admin.audit_log
        VALUES (
            '{group_id}',
            '{target_table}',
            'SUCCESS',
            'Table Loaded Successfully',
            current_timestamp()
        )
        """)
    except Exception as e:

        error_message = str(e)

        print(error_message)

        # =====================================
        # AUDIT FAILURE LOG
        # =====================================

        spark.sql(f"""
        INSERT INTO demo_catalog.admin.audit_log
        VALUES (
            '{group_id}',
            '{target_table}',
            'FAILED',
            '{error_message}',
            current_timestamp()
        )
        """)
