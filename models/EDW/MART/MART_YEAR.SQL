/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select MART_YEAR

Version     Date            Author              Description
-------     --------        -----------        ----------------------------------
1.0         06 DEV 2022     Kali Dandapani     Initial Version: Porting from airflow to dbt
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['YEAR_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_YEAR'-%}
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
         description = 'Building table YEAR dimension'
        ,transient=true
        ,materialized='table'
        ,schema ='MART'
        ,alias='YEAR'
		,unique_key= v_pk_list
        ,tags =['MART']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}


SELECT
    DISTINCT
    YEAR_KEY,
    YEAR,
    YEAR_DESCRIPTION,
    YEAR_BEGIN_DATE,
    YEAR_END_DATE,
    TOTAL_DAYS_IN_YEAR,
    TOTAL_MONTHS_IN_YEAR,
    TOTAL_QUARTERS_IN_YEAR,
    YEAR_SEQUENCE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
FROM {{ref('MART_DATE')}}