INSERT INTO demo_catalog.admin.data_flow_env_config_lookup (ENVIRONMENT,CONTEXT_KEY,CONTEXT_VALUE,IS_VALID,INSERTED_BY,UPDATED_BY,INSERTED_TS,UPDATED_TS) VALUES
	 ('dev','framework_control_catalog','demo_catalog','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','framework_control_schema','admin','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','control_table_header','data_flow_control_header','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','control_table_l0_detail','data_flow_l0_detail','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','control_table_pb_detail','data_flow_pb_detail','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','control_table_env_config','data_flow_env_config_lookup','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','control_table_audit_log','audit_log','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','control_table_pipeline_lookup','etl_pipeline_lookup','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','engine_notebook_path','/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/notebooks/kiro_etl_engine','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','engine_repo_path','/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453');
INSERT INTO demo_catalog.admin.data_flow_env_config_lookup (ENVIRONMENT,CONTEXT_KEY,CONTEXT_VALUE,IS_VALID,INSERTED_BY,UPDATED_BY,INSERTED_TS,UPDATED_TS) VALUES
	 ('dev','meta_desc_header','Pipeline group configuration and orchestration metadata','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','meta_desc_l0_detail','L0 layer ingestion metadata: source URLs, file formats, DQ logic, CDC settings','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','meta_desc_pb_detail','L1/L2 layer transformation metadata: queries, load types, primary keys, scripts','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','meta_desc_env_config','Environment-specific configuration using flexible key-value pattern','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','meta_desc_audit_log','Per-run audit trail: execution timestamps, status, row counts, durations','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','meta_desc_pipeline_lookup','Latest execution status per pipeline object across all runs','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','supported_trigger_types','DLT,JOB','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','supported_load_types','FULL,APPEND,DELTA,MERGE,SCD,PYSPARK','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','supported_etl_layers','L0,L1,L2','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','supported_object_types','TABLE,MV','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453');
INSERT INTO demo_catalog.admin.data_flow_env_config_lookup (ENVIRONMENT,CONTEXT_KEY,CONTEXT_VALUE,IS_VALID,INSERTED_BY,UPDATED_BY,INSERTED_TS,UPDATED_TS) VALUES
	 ('dev','framework_version','2.0','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','framework_last_updated','2026-10-02','Y','admin','admin','2026-10-02 05:56:59.888453','2026-10-02 05:56:59.888453'),
	 ('dev','landing_s3_static_path','s3://aws-te-eda-landing','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','conf','data_flow_conf_detail','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','github_source_path','https://github.com/ID-KARTHIKEYAN/DATA_INTEGRATION.git','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','target_catalog','demo_catalog','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','raw_schema','raw','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','silver_schema','silver','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','compute_class','serverless','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773'),
	 ('dev','dbr_version','15.4 LTS','Y','current user','current user','2026-10-02 05:54:25.705773','2026-10-02 05:54:25.705773');
