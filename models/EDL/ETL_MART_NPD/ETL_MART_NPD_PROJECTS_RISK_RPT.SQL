/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_RISK_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_RISK_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022     KALI DANDAPANI     Initial Version
1.1         04-JAN-2023     CLINT WHETTEN      Added BUSINESS_UNIT, DIVISION, DIVISION_PRIORITY, and pivoted the data
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_RISK_RPT'-%}
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
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_RISK_RPT'
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

,PROJECT_RISK AS (
    SELECT * FROM (
        SELECT 
            PROJECT_NUMBER,
            PROJECT_NAME,
            BU AS BUSINESS_UNIT,
            DIVISION,
            DIVISION_PRIORITY,
            PROGRAM_MANAGER,
            TO_ACT,
            FINANCIAL_RISK,
            SCHEDULE_RISK,
            RESOURCES_RISK,
            SPECS_REQ_ISSUE,
            OTHER_ISSUES,
            TOP_PROJECT,
            ADDITIONAL_PROJECT_LINKS,
            CREATED,
            CREATED_BY,
            MODIFIED,
            MODIFIED_BY
        FROM 
        {{source ('STG_SHAREPOINT_NPD','PM_DASHBOARD_ENTRY')}} 
        QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER ORDER BY BIW_UPD_DTTM DESC) =1)
    )
    UNPIVOT(RISK FOR RISK_TYPE IN (
        FINANCIAL_RISK, SCHEDULE_RISK, RESOURCES_RISK, SPECS_REQ_ISSUE, OTHER_ISSUES
        )
    )
)

,PROJECT_RISK_DESC AS (
    SELECT * FROM (
        SELECT 
            PROJECT_NUMBER,
            FINANCIAL_DESCRIPTION FINANCIAL_RISK,
            SCHEDULE_DESCRIPTION SCHEDULE_RISK,
            RESOURCES_DESCRIPTION RESOURCES_RISK,
            SPECS_REQ_ISSUE_DESCRIPTION SPECS_REQ_ISSUE,
            OTHER_ISSUES_DESCRIPTION OTHER_ISSUES
        FROM 
        {{source ('STG_SHAREPOINT_NPD','PM_DASHBOARD_ENTRY')}} 
        QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER ORDER BY BIW_UPD_DTTM DESC) =1)
    )
    UNPIVOT(RISK_DESCRIPTION FOR RISK_TYPE IN (
        FINANCIAL_RISK, SCHEDULE_RISK, RESOURCES_RISK, SPECS_REQ_ISSUE, OTHER_ISSUES
    ))
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',RISK.PROJECT_NUMBER::STRING
                            ,'COL3',RISK.RISK_TYPE::STRING
                         )::STRING 
        )::BINARY AS PROJECT_RISK_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
    RISK.PROJECT_NUMBER,
    RISK.PROJECT_NAME,
    RISK.BUSINESS_UNIT,
    RISK.DIVISION,
    RISK.DIVISION_PRIORITY,
    RISK.PROGRAM_MANAGER,
    RISK.TO_ACT,
    RISK.RISK_TYPE,
    RISK.RISK,
    RISK_DESC.RISK_DESCRIPTION,
    RISK.TOP_PROJECT,
    RISK.ADDITIONAL_PROJECT_LINKS,
    RISK.CREATED,
    RISK.CREATED_BY,
    RISK.MODIFIED,
    RISK.MODIFIED_BY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',RISK.PROJECT_NUMBER::string, 'col3',RISK.PROJECT_NAME::string,
    'col4',RISK.BUSINESS_UNIT::string, 'col5',RISK.DIVISION::string, 'col6',RISK.DIVISION_PRIORITY::string, 'col7',RISK.PROGRAM_MANAGER::string, 'col8',RISK.TO_ACT::string,
    'col9',RISK.RISK_TYPE::string, 'col10',RISK.RISK::string, 'col11',RISK_DESC.RISK_DESCRIPTION::string, 'col12',RISK.TOP_PROJECT::string, 'col13',RISK.ADDITIONAL_PROJECT_LINKS::string,
    'col14',RISK.CREATED::string, 'col15',RISK.CREATED_BY::string, 'col16',RISK.MODIFIED::string, 'col17',RISK.MODIFIED_BY::string)::string )::BINARY as BIW_MD5_KEY  
FROM PROJECT_RISK RISK
LEFT JOIN PROJECT_RISK_DESC RISK_DESC ON (
    RISK.PROJECT_NUMBER = RISK_DESC.PROJECT_NUMBER 
    AND RISK.RISK_TYPE = RISK_DESC.RISK_TYPE
)
CROSS JOIN FISCAL_WEEK FSC_WK