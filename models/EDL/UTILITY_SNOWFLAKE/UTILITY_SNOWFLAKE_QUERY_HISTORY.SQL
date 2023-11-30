/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select UTILITY_SNOWFLAKE_QUERY_HISTORY
--dbt run --select UTILITY_SNOWFLAKE_QUERY_HISTORY

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         31/OCT/2022      Kali D     Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['QUERY_ID']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID']-%}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_UTILITY_SNOWFLAKE_QUERY_HISTORY'-%}
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
         description = 'Building table QUERY_HISTORY to capture history'
        ,transient=true
        ,materialized='incremental'
        ,schema ='UTILITY_SNOWFLAKE'
        ,alias='QUERY_HISTORY'
        ,unique_key= v_pk_list
        ,tags ='SNOWFLAKE'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

SELECT 
QUERY_ID
,QUERY_TEXT
,DATABASE_ID
,DATABASE_NAME
,SCHEMA_ID
,SCHEMA_NAME
,QUERY_TYPE
,SESSION_ID
,USER_NAME
,ROLE_NAME
,WAREHOUSE_ID
,WAREHOUSE_NAME
,WAREHOUSE_SIZE
,WAREHOUSE_TYPE
,CLUSTER_NUMBER
,QUERY_TAG
,EXECUTION_STATUS
,ERROR_CODE
,ERROR_MESSAGE
,START_TIME
,END_TIME
,TOTAL_ELAPSED_TIME
,BYTES_SCANNED
,PERCENTAGE_SCANNED_FROM_CACHE
,BYTES_WRITTEN
,BYTES_WRITTEN_TO_RESULT
,BYTES_READ_FROM_RESULT
,ROWS_PRODUCED
,ROWS_INSERTED
,ROWS_UPDATED
,ROWS_DELETED
,ROWS_UNLOADED
,BYTES_DELETED
,PARTITIONS_SCANNED
,PARTITIONS_TOTAL
,BYTES_SPILLED_TO_LOCAL_STORAGE
,BYTES_SPILLED_TO_REMOTE_STORAGE
,BYTES_SENT_OVER_THE_NETWORK
,COMPILATION_TIME
,EXECUTION_TIME
,QUEUED_PROVISIONING_TIME
,QUEUED_REPAIR_TIME
,QUEUED_OVERLOAD_TIME
,TRANSACTION_BLOCKED_TIME
,OUTBOUND_DATA_TRANSFER_CLOUD
,OUTBOUND_DATA_TRANSFER_REGION
,OUTBOUND_DATA_TRANSFER_BYTES
,INBOUND_DATA_TRANSFER_CLOUD
,INBOUND_DATA_TRANSFER_REGION
,INBOUND_DATA_TRANSFER_BYTES
,LIST_EXTERNAL_FILES_TIME
,CREDITS_USED_CLOUD_SERVICES
,RELEASE_VERSION
,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS
,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS
,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS
,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES
,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES
,QUERY_LOAD_PERCENT
,IS_CLIENT_GENERATED_STATEMENT
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM
,{{V_BIW_BATCH_ID}}	as BIW_BATCH_ID
FROM {{ref('UTILITY_SNOWFLAKE_QUERY_HISTORY_LAST_30_DAYS')}}
{% if is_incremental() %}
    WHERE 
    {% if var('is_backfill') %}
        START_TIME::date >= '{{var('refresh_start_ts')}}'::date -1 
		AND START_TIME::date < '{{V_START_DTTM}}'::date 
    {% else %}
        START_TIME::date >= '{{V_LWM}}' ::date -1 
		AND START_TIME::date < '{{V_HWM}}'::date
    {% endif %}  
{% endif %}

