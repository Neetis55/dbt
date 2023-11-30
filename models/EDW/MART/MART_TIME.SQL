
/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select MART_TIME

Version     Date            Author              Description
-------     --------        -----------        ----------------------------------
1.0         24 JAN 2023     Kali Dandapani     Initial Version: of time dimension
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['TIME_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list(  ref('ETL_MART_TIME')  ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_TIME'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building table TIME for MART'
        ,transient=true
        ,materialized='table'
        ,schema ='MART'
        ,alias='TIME'
		,unique_key= v_pk_list
        ,tags =['MART']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}


SELECT
    TIME_KEY,
    TIME_OF_DAY,
    HOUR24,
    HOUR24_FULL_DESCRIPTION,
    HOUR24_MINUTE_DESCRIPTION,
    HOUR24_SHORT_DESCRIPTION,
    HOUR12,
    HOUR12_FULL_DESCRIPTION,
    HOUR12_MINUTE_DESCRIPTION,
    HOUR12_MINUTE_CODE,
    HOUR12_SHORT_DESCRIPTION,
    IS_PM,
    AM_PM_DESCRIPTION,
    DAY_TIME_DESCRIPTION,
    DAY_NIGHT_DESCRIPTION,
    MINUTE,
    MINUTE_OF_DAY,
    MINUTE_FULL_DESCRIPTION_24,
    MINUTE_FULL_DESCRIPTION_12,
    MINUTE_SHORT_DESCRIPTION,
    SECOND,
    SECOND_SHORT_DESCRIPTION,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
FROM
	{{ref('ETL_MART_TIME')}}
