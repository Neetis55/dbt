/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select MART_FISCAL_WEEK

Version     Date            Author              Description
-------     --------        -----------        ----------------------------------
1.0         06 DEV 2022     Kali Dandapani     Initial Version: Porting from airflow to dbt
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['FISCAL_WEEK_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_FISCAL_WEEK'-%}
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
         description = 'Building table FISCAL_WEEK dimension'
        ,transient=true
        ,materialized='table'
        ,schema ='MART'
        ,alias='FISCAL_WEEK'
		,unique_key= v_pk_list
        ,tags =['MART']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    DISTINCT
	FISCAL_WEEK_KEY,
	FISCAL_WEEK,
	FISCAL_WEEK_NUMBER,
	FISCAL_WEEK_BEGIN_DATE,
	FISCAL_WEEK_END_DATE,
	TOTAL_DAYS_IN_FISCAL_WEEK,
	FISCAL_WEEK_SEQUENCE,
	FISCAL_WEEK_IN_FISCAL_MONTH,
	FISCAL_WEEK_IN_FISCAL_QUARTER,
	FISCAL_WEEK_IN_FISCAL_YEAR,
	CRAWL_WEEK_IN_FISCAL_QUARTER,
	CRAWL_WEEK_IN_FISCAL_QUARTER_PLUS_1,
	CRAWL_WEEK_IN_FISCAL_QUARTER_PLUS_2,
	FISCAL_MONTH_KEY,
	FISCAL_MONTH,
	FISCAL_MONTH_NUMBER,
	FISCAL_MONTH_DESCRIPTION,
	FISCAL_MONTH_BEGIN_DATE,
	FISCAL_MONTH_END_DATE,
	TOTAL_DAYS_IN_FISCAL_MONTH,
	TOTAL_FISCAL_WEEKS_IN_FISCAL_MONTH,
	FISCAL_MONTH_SEQUENCE,
	FISCAL_MONTH_IN_FISCAL_QUARTER,
	FISCAL_MONTH_IN_FISCAL_YEAR,
	FISCAL_QUARTER_KEY,
	FISCAL_QUARTER,
	FISCAL_QUARTER_YYYYQQ,
	FISCAL_QUARTER_NUMBER,
	FISCAL_QUARTER_DESCRIPTION,
	FISCAL_QUARTER_BEGIN_DATE,
	FISCAL_QUARTER_END_DATE,
	TOTAL_DAYS_IN_FISCAL_QUARTER,
	TOTAL_FISCAL_WEEKS_IN_FISCAL_QUARTER,
	TOTAL_FISCAL_MONTHS_IN_FISCAL_QUARTER,
	FISCAL_QUARTER_SEQUENCE,
	FISCAL_QUARTER_IN_FISCAL_YEAR,
	FISCAL_QUARTER_PRIOR_FOUR,
	FISCAL_QUARTER_PRIOR_THREE,
	FISCAL_QUARTER_PRIOR_TWO,
	FISCAL_QUARTER_PRIOR,
	FISCAL_QUARTER_NEXT,
	FISCAL_QUARTER_NEXT_TWO,
	FISCAL_QUARTER_NEXT_THREE,
	FISCAL_QUARTER_NEXT_FOUR,
	FISCAL_QUARTER_PRIOR_YEAR,
	FISCAL_YEAR_KEY,
	FISCAL_YEAR,
	FISCAL_YEAR_BEGIN_DATE,
	FISCAL_YEAR_END_DATE,
	TOTAL_DAYS_IN_FISCAL_YEAR,
	TOTAL_FISCAL_WEEKS_IN_FISCAL_YEAR,
	TOTAL_FISCAL_MONTHS_IN_FISCAL_YEAR,
	TOTAL_FISCAL_QUARTERS_IN_FISCAL_YEAR,
	FISCAL_YEAR_SEQUENCE,
	FISCAL_YEAR_PRIOR,
    FISCAL_WEEK_PRIOR,
    FISCAL_WEEK_PRIOR_BEGIN_DATE,
    FISCAL_WEEK_PRIOR_END_DATE,
    FISCAL_MONTH_PRIOR,
    FISCAL_MONTH_PRIOR_BEGIN_DATE,
    FISCAL_MONTH_PRIOR_END_DATE,
    FISCAL_QUARTER_PRIOR_BEGIN_DATE,
    FISCAL_QUARTER_PRIOR_END_DATE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
FROM {{ref('MART_DATE')}}