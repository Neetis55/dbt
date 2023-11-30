/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT
-- dbt build --full-refresh --select +MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT+

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022     KALI DANDAPANI      Initial Version
1.1         04-JAN-2023     CLINT WHETTEN       Added measures from FACT_PPM_PTC_DTL
1.4         20-JAN-2023     CLINT WHETTEN       Added NEXT_MILESTONE
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_SCHEDULE_SUMMARY_KEY']-%}

{##-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-##}
{##-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT') ) -##}
{##-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -##}
{##-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT'),'STG.') -##}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT'-%}
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
         description = 'Building ETL table PROJECTS_SCHEDULE_SUMMARY_RPT for NPD LANDING PROJECT'
        ,transient=false   
        ,materialized='incremental'
        ,schema ='MART_NPD'
        ,alias='PROJECTS_SCHEDULE_SUMMARY_RPT'
        ,tags =['MART_NPD']
        ,unique_key= v_pk_list
        ,merge_update_columns =['SNAPSHOT_WEEK_KEY', 'PROJECT_NUMBER', 'PROJECT_ID', 'STEP_TYPE', 'STEP_NAME', 'SORT_ORDER', 'STATUS',
                                'OS_STEP_COLUMN_NAME', 'OS_DATE', 'OS_DATE_WEEK', 'CS_STEP_COLUMN_NAME', 'CS_DATE', 'CS_DATE_WEEK',
                                'FORECAST_STEP_COLUMN_NAME', 'FORECAST_DATE', 'FORECAST_DATE_WEEK', 'ACTUAL_STEP_COLUMN_NAME', 'ACTUAL_DATE',
                                'ACTUAL_DATE_WEEK', 'NUMBER_OF_DEV_PASSES', 'PRODUCT_REVENUE_XCLUDING_NRE_$', 'TOTAL_NUMBER_OF_NEW_MASK',
                                'IDEAL_NUMBER_OF_NEW_MASK', 'PROJECTED_TOTAL_OTHER_EXPENSES', 'PROJECTED_TOTAL_LABOR_COST', 'PTC_TOTAL_COST',
                                'PTC_PROJ_COMPLEXITY', 'PRODUCT_GM_EXCLUDING_NRE_PCT', 'PRODUCT_GM_EXCLUDING_NRE', 'TOTAL_UNITS',
                                'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY'] 
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    STG.PROJECT_SCHEDULE_SUMMARY_KEY,
    STG.SNAPSHOT_WEEK_KEY,
    STG.PROJECT_NUMBER,
    STG.PROJECT_ID,
    STG.STEP_TYPE,
    STG.STEP_NAME,
    STG.SORT_ORDER,
    STG.STATUS,
    STG.OS_STEP_COLUMN_NAME,
    STG.OS_DATE,
    STG.OS_DATE_WEEK,
    STG.CS_STEP_COLUMN_NAME,
    STG.CS_DATE,
    STG.CS_DATE_WEEK,
    STG.FORECAST_STEP_COLUMN_NAME,
    STG.FORECAST_DATE,
    STG.FORECAST_DATE_WEEK,
    STG.ACTUAL_STEP_COLUMN_NAME,
    STG.ACTUAL_DATE,
    STG.ACTUAL_DATE_WEEK,
    STG.NUMBER_OF_DEV_PASSES,
    STG.PRODUCT_REVENUE_XCLUDING_NRE_$,
    STG.TOTAL_NUMBER_OF_NEW_MASK,
    STG.IDEAL_NUMBER_OF_NEW_MASK,
    STG.PROJECTED_TOTAL_OTHER_EXPENSES,
    STG.PROJECTED_TOTAL_LABOR_COST,
    STG.PTC_TOTAL_COST,
    STG.PTC_PROJ_COMPLEXITY,
    STG.PRODUCT_GM_EXCLUDING_NRE_PCT,
    STG.PRODUCT_GM_EXCLUDING_NRE,
    STG.TOTAL_UNITS,
    STG.NEXT_MILESTONE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    STG.BIW_MD5_KEY
FROM 
    {{ref ('ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT')}}  STG
   {% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.PROJECT_SCHEDULE_SUMMARY_KEY= TGT.PROJECT_SCHEDULE_SUMMARY_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}