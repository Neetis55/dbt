/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_NPD_PROJECTS_RISK_RPT
-- dbt build --full-refresh --select MART_NPD_PROJECTS_RISK_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022     KALI DANDAPANI      Initial Version
1.1         04-JAN-2023     CLINT WHETTEN       Added BUSINESS_UNIT, DIVISION, DIVISION_PRIORITY, and pivoted the data
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_RISK_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}  
--SELECT {{v_update_column_list}}  
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_NPD_PROJECTS_RISK_RPT'-%}
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
         description = 'Building ETL table PROJECTS_RISK_RPT for NPD LANDING PROJECT'
        ,transient=false   
        ,materialized='incremental'
        ,schema ='MART_NPD'
        ,alias='PROJECTS_RISK_RPT'
        ,tags =['MART_NPD']
        ,unique_key= v_pk_list
        ,merge_update_columns =['SNAPSHOT_WEEK_KEY', 'PROJECT_NUMBER', 'PROJECT_NAME', 'BUSINESS_UNIT', 'DIVISION', 'DIVISION_PRIORITY',
                                'PROGRAM_MANAGER', 'TO_ACT', 'RISK_TYPE', 'RISK', 'RISK_DESCRIPTION', 'TOP_PROJECT', 'ADDITIONAL_PROJECT_LINKS',
                                'CREATED', 'CREATED_BY', 'MODIFIED', 'MODIFIED_BY', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']  
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    STG.PROJECT_RISK_KEY,
    STG.SNAPSHOT_WEEK_KEY,
    STG.PROJECT_NUMBER,
    STG.PROJECT_NAME,
    STG.BUSINESS_UNIT,
    STG.DIVISION,
    STG.DIVISION_PRIORITY,
    STG.PROGRAM_MANAGER,
    STG.TO_ACT,
    STG.RISK_TYPE,
    STG.RISK,
    STG.RISK_DESCRIPTION,
    STG.TOP_PROJECT,
    STG.ADDITIONAL_PROJECT_LINKS,
    STG.CREATED,
    STG.CREATED_BY,
    STG.MODIFIED,
    STG.MODIFIED_BY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    STG.BIW_MD5_KEY
FROM 
    {{ref ('ETL_MART_NPD_PROJECTS_RISK_RPT')}}  STG
   {% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.PROJECT_RISK_KEY= TGT.PROJECT_RISK_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}