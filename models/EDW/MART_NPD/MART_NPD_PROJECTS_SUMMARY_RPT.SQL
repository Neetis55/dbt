/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_NPD_PROJECTS_SUMMARY_RPT
-- dbt build --full-refresh --select +MART_NPD_PROJECTS_SUMMARY_RPT+

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022      KALI DANDAPANI     Initial Version
1.1         03-JAN-2023     CLINT WHETTEN       Added DELEGATED_OWNER, BUSINESS_UNIT, PROJECT_PAL2
1.2         18-JAN-2023     CLINT WHETTEN       Added ITEM_NUMBER
1.3         19-JAN-2023     KALI DANDAPANI      Adding logic to override the source division and bu using MART_NPD.PROJECT_DIVISION_REF
1.4         20-JAN-2023     CLINT WHETTEN       Added NPD_COST_OBJECT
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_SUMMARY_KEY']-%}

{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_NPD_PROJECTS_SUMMARY_RPT'-%}
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
         description = 'Building ETL table PROJECTS_SUMMARY_RPT for NPD LANDING PROJECT'
        ,transient=false   
        ,materialized='incremental'
        ,schema ='MART_NPD'
        ,alias='PROJECTS_SUMMARY_RPT'
        ,tags =['MART_NPD']
        ,unique_key= v_pk_list
        ,merge_update_columns =['SNAPSHOT_WEEK_KEY', 'PROJECT_NUMBER', 'PROJECT_ID', 'PROJECT_NAME', 'PROJECT_PRIORITY', 'TOP_PROJECT', 'PROJECT_DESCRIPTION', 
                                'PROJECT_START_DATE', 'PROJECT_STATE', 'PROJECT_STATUS', 'ONSEMICONDUCTOR_STATUS', 'CURRENT_PHASE', 'DEVELOPMENT_CENTRE',
                                'PROGRAM_MANAGER', 'ENGR_PRODUCT_OWNER', 'PRODUCT_MRKT_LEAD', 'OWNER', 'DELEGATED_OWNER', 'PROJECT_SPONSOR', 'GROUP_NAME',
                                'DIVISION_NAME','DIVISION_ABBR','ITEM_NUMBER', 'BUSINESS_UNIT', 'PROJECT_PAL2','NPD_COST_OBJECT', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']  
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    STG.PROJECT_SUMMARY_KEY,
    STG.SNAPSHOT_WEEK_KEY,
    STG.PROJECT_NUMBER, 
    STG.PROJECT_ID,
    STG.PROJECT_NAME,
    STG.PROJECT_PRIORITY,
    STG.TOP_PROJECT,
    STG.PROJECT_DESCRIPTION,  
    STG.PROJECT_START_DATE,
    STG.PROJECT_STATE,
    STG.PROJECT_STATUS,
    STG.ONSEMICONDUCTOR_STATUS,
    STG.CURRENT_PHASE,
    STG.DEVELOPMENT_CENTRE,
    STG.PROGRAM_MANAGER,
    STG.ENGR_PRODUCT_OWNER,
    STG.PRODUCT_MRKT_LEAD,
    STG.OWNER,
    STG.DELEGATED_OWNER,
    STG.PROJECT_SPONSOR ,
    STG.GROUP_NAME,
    STG.DIVISION_ABBR,
    STG.DIVISION_NAME,
    STG.ITEM_NUMBER,
    STG.BUSINESS_UNIT,
    STG.PROJECT_PAL2,
    STG.NPD_COST_OBJECT,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    STG.BIW_MD5_KEY
FROM 
    {{ref ('ETL_MART_NPD_PROJECTS_SUMMARY_RPT')}}  STG
   {% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.PROJECT_SUMMARY_KEY= TGT.PROJECT_SUMMARY_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}