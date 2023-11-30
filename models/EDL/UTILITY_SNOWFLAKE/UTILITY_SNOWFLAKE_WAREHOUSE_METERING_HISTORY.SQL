/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select UTILITY_SNOWFLAKE_WAREHOUSE_METERING_HISTORY
--dbt run --select UTILITY_SNOWFLAKE_WAREHOUSE_METERING_HISTORY

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         31/OCT/2022      Kali D     Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['START_TIME','END_TIME','WAREHOUSE_ID']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID']-%}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_UTILITY_SNOWFLAKE_WAREHOUSE_METERING_HISTORY'-%}
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
         description = 'Building table WAREHOUSE_METERING_HISTORY to capture history'
        ,transient=true
        ,materialized='incremental'
        ,schema ='UTILITY_SNOWFLAKE'
        ,alias='WAREHOUSE_METERING_HISTORY'
        ,unique_key= v_pk_list
        ,tags ='SNOWFLAKE'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

SELECT 
    START_TIME
    ,END_TIME
    ,WAREHOUSE_ID
    ,WAREHOUSE_NAME
    ,CREDITS_USED
    ,CREDITS_USED_COMPUTE
    ,CREDITS_USED_CLOUD_SERVICES
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM
    ,{{V_BIW_BATCH_ID}}	as BIW_BATCH_ID
FROM {{source('ACCOUNT_USAGE','WAREHOUSE_METERING_HISTORY')}}
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

