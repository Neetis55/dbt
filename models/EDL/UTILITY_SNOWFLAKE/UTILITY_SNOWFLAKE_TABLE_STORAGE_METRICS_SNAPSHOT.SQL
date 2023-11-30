/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select UTILITY_SNOWFLAKE_TABLE_STORAGE_METRICS_SNAPSHOT
--dbt run --select UTILITY_SNOWFLAKE_TABLE_STORAGE_METRICS_SNAPSHOT

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         31/OCT/2022      Kali D     Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['ID','SNAPSHOT_DATE']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID']-%}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_UTILITY_SNOWFLAKE_TABLE_STORAGE_METRICS_SNAPSHOT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('aliAS') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building table TABLE_STORAGE_METRICS_SNAPSHOT to capture daily storage metric'
        ,transient=true
        ,materialized='incremental'
        ,schema ='UTILITY_SNOWFLAKE'
        ,alias='TABLE_STORAGE_METRICS_SNAPSHOT'
        ,unique_key= v_pk_list
        ,tags ='SNOWFLAKE'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

SELECT 
current_date as SNAPSHOT_DATE
,ID
,TABLE_NAME
,TABLE_SCHEMA_ID
,TABLE_SCHEMA
,TABLE_CATALOG_ID
,TABLE_CATALOG
,CLONE_GROUP_ID
,IS_TRANSIENT
,ACTIVE_BYTES
,TIME_TRAVEL_BYTES
,FAILSAFE_BYTES
,RETAINED_FOR_CLONE_BYTES
,DELETED
,TABLE_CREATED
,TABLE_DROPPED
,TABLE_ENTERED_FAILSAFE
,SCHEMA_CREATED
,SCHEMA_DROPPED
,CATALOG_CREATED
,CATALOG_DROPPED
,COMMENT
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM
,{{V_BIW_BATCH_ID}}	as BIW_BATCH_ID
FROM {{source('ACCOUNT_USAGE','TABLE_STORAGE_METRICS')}}

