/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_JIRA_ISSUES_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_JIRA_ISSUES_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         30-JAN-2023     CLINT WHETTEN      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_JIRA_ISSUES_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_JIRA_ISSUES_RPT'-%}
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
         description = 'Building ETL table PROJECTS_JIRA_ISSUES_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_JIRA_ISSUES_RPT'
        ,tags =['MART_NPD']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH FISCAL_WEEK AS 
(
    SELECT 
        DISTINCT FISCAL_WEEK_KEY
    FROM 
    {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.DATE  
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() )::DATE
)

,NPD_ISSUES AS (
    SELECT 
        ID,
        KEY AS JIRA_KEY,
        PROJECT AS JIRA_PROJECT,
        CREATOR AS CREATED_BY,
        REPORTER AS REPORTED_BY,
        ASSIGNEE AS ASSIGNED_TO,
        ISSUE_TYPE,
        STATUS,
        RESOLUTION,
        CREATED,
        RESOLUTION_DATE
    FROM 
    {{source ('STG_JIRA','NPD_ISSUES')}}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ID ORDER BY BIW_UPD_DTTM DESC) =1)
)

,AGILE_TO_JIRA AS (
    SELECT DISTINCT
        AGILE_NUMBER AS PROJECT_NUMBER,
        JIRA_KEY AS JIRA_PROJECT
    FROM 
    {{source ('STG_API_NPD','AGILE_TO_JIRA')}}  
    WHERE AGILE_NUMBER IS NOT NULL
    AND JIRA_KEY IS NOT NULL
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY JIRA_KEY,AGILE_NUMBER ORDER BY  BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',ISS.JIRA_KEY::STRING
                            ,'COL3',ISS.JIRA_PROJECT::STRING
                            ,'COL4',ATJ.PROJECT_NUMBER::STRING
                         )::STRING 
        )::BINARY AS PROJECT_JIRA_ISSUES_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
    ISS.JIRA_KEY,
    ISS.JIRA_PROJECT,
    ATJ.PROJECT_NUMBER,
    ISS.CREATED_BY,
    ISS.REPORTED_BY,
    ISS.ASSIGNED_TO,
    ISS.ISSUE_TYPE,
    ISS.STATUS,
    ISS.RESOLUTION,
    ISS.CREATED,
    ISS.RESOLUTION_DATE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',ISS.JIRA_KEY::string, 'col3',ISS.JIRA_PROJECT::string,
    'col4',ATJ.PROJECT_NUMBER::string, 'col5',ISS.CREATED_BY::string, 'col6',ISS.REPORTED_BY::string, 'col7',ISS.ASSIGNED_TO::string, 
    'col8',ISS.ISSUE_TYPE::string, 'col9',ISS.STATUS::string, 'col10',ISS.RESOLUTION::string, 'col11',ISS.CREATED::string, 
    'col12',ISS.RESOLUTION_DATE::string)::string )::BINARY as BIW_MD5_KEY
FROM     
NPD_ISSUES ISS 
LEFT JOIN AGILE_TO_JIRA ATJ
    ON ISS.JIRA_PROJECT = ATJ.JIRA_PROJECT
CROSS JOIN FISCAL_WEEK FSC_WK