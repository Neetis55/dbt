/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_JIRA_CREATED_VS_RESOLVED_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_JIRA_CREATED_VS_RESOLVED_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         30-JAN-2023     CLINT WHETTEN      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_JIRA_CREATED_VS_RESOLVED_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_JIRA_CREATED_VS_RESOLVED_RPT'-%}
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
         description = 'Building ETL table PROJECTS_JIRA_CREATED_VS_RESOLVED_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_JIRA_CREATED_VS_RESOLVED_RPT'
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

,ALL_FISCAL_WEEK AS 
(
       SELECT 
        CALENDAR_DATE,
        FISCAL_WEEK_KEY
    FROM 
    {{source('MART','DATE')}}
)

-- STEP 1 GET LIST OF PROJECTS
,DIM_PPM_PROJECT_DTL AS (
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,
        PROJECT_NAME
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_PROJECT_DTL')}}  
    WHERE PROJECT_TYPE = 'NPD'
        AND PROJECT_WIP_EXCEPTION IN ( 'PROJECT','EXCEPTION')
        AND ONSEMICONDUCTOR_STATUS NOT IN ('6 Cancelled (Customer)','7 Cancelled (Internal)' )
        AND PROJECT_STATE NOT IN ('Template','Proposed')
        AND PROJECT_STATUS IN ('In Process')
        AND CURRENT_PHASE NOT IN('6 Sustain','7 Retire')
        AND (PROJECT_NUMBER IS NOT NULL  OR LEN(PROJECT_NUMBER) <> 0)
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER ORDER BY PROJECT_KEY DESC, BIW_UPD_DTTM DESC) =1)
)

-- STEP 2 GET PROJECTS CS DATES
,FACT_PPM_PTC_DTL_CS AS (
    SELECT 
        PTC.PROJECT_NUMBER
        ,PTC.PROJECT_ID     
        ,DATE(PTC.FTO_DATE) AS FTO_DATE
        ,PTC_CS_FTO.FISCAL_WEEK_KEY AS CS_FTO_DATE_WEEK
        ,DATE(PTC.DEVELOP_COMPLETE_DATE) AS DEVELOP_COMPLETE_DATE
        ,PTC_CS_VC.FISCAL_WEEK_KEY AS CS_VC_DATE_WEEK
        ,DATE(PTC.TAPEOUT1_DATE) AS TAPEOUT1_DATE
        ,DATE(PTC.TAPEOUT2_DATE) AS TAPEOUT2_DATE
        ,DATE(PTC.TAPEOUT3_DATE) AS TAPEOUT3_DATE
        ,DATE(PTC.TAPEOUT4_DATE) AS TAPEOUT4_DATE
        ,DATE(PTC.TAPEOUT5_DATE) AS TAPEOUT5_DATE
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','FACT_PPM_PTC_DTL')}} PTC
    INNER JOIN DIM_PPM_PROJECT_DTL DTL
        ON PTC.PROJECT_ID = DTL.PROJECT_ID
    LEFT JOIN ALL_FISCAL_WEEK PTC_CS_FTO
        ON PTC_CS_FTO.CALENDAR_DATE = PTC.FTO_DATE
    LEFT JOIN ALL_FISCAL_WEEK PTC_CS_VC
        ON PTC_CS_VC.CALENDAR_DATE = PTC.DEVELOP_COMPLETE_DATE        
    WHERE PTC.LATEST_REV_FLAG=1
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PTC.PROJECT_NUMBER,PTC.PTC_KEY ORDER BY BIW_UPD_DTTM DESC) =1)
)

,FISCAL_WEEK_RANGE AS (
    SELECT DISTINCT
        PTC.PROJECT_NUMBER,
        PTC.CS_FTO_DATE_WEEK,
        PTC.CS_VC_DATE_WEEK,
        FISCAL_WEEK_KEY
    FROM
    {{source('MART','DATE')}} D
    INNER JOIN FACT_PPM_PTC_DTL_CS PTC
        ON FISCAL_WEEK_KEY BETWEEN CS_FTO_DATE_WEEK AND CS_VC_DATE_WEEK
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
        DATE(CREATED) AS CREATED_DATE,
        FSC_WK_C.FISCAL_WEEK_KEY AS CREATED_DATE_WEEK,
        DATE(RESOLUTION_DATE) AS RESOLUTION_DATE,
        FSC_WK_R.FISCAL_WEEK_KEY AS RESOLUTION_DATE_WEEK
    FROM 
    {{source ('STG_JIRA','NPD_ISSUES')}} NI
    LEFT JOIN ALL_FISCAL_WEEK FSC_WK_C
        ON FSC_WK_C.CALENDAR_DATE = DATE(NI.CREATED)
    LEFT JOIN ALL_FISCAL_WEEK FSC_WK_R
        ON FSC_WK_R.CALENDAR_DATE = DATE(NI.RESOLUTION_DATE)
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

,BASE_DATA AS (
    SELECT 
        AGG_DATA.*,
        FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    FROM (
        SELECT
            PTC.PROJECT_NUMBER,
            FSCR_WK.FISCAL_WEEK_KEY AS ISSUE_WEEK_KEY,
            COUNT_IF(ISS.CREATED_DATE_WEEK IS NOT NULL) AS CREATED_ISSUES,
            COUNT_IF(ISS.RESOLUTION_DATE_WEEK IS NOT NULL) AS RESOLVED_ISSUES,
            COUNT_IF(ISS.CREATED_DATE_WEEK IS NOT NULL AND ISS.RESOLUTION_DATE_WEEK IS NULL) AS OPEN_ISSUES
        FROM
        FACT_PPM_PTC_DTL_CS PTC
        INNER JOIN FISCAL_WEEK_RANGE FSCR_WK
            ON PTC.PROJECT_NUMBER = FSCR_WK.PROJECT_NUMBER
        INNER JOIN AGILE_TO_JIRA ATJ
            ON PTC.PROJECT_NUMBER = ATJ.PROJECT_NUMBER
        LEFT JOIN NPD_ISSUES ISS
            ON ISS.JIRA_PROJECT = ATJ.JIRA_PROJECT
            AND (
                FSCR_WK.FISCAL_WEEK_KEY = ISS.CREATED_DATE_WEEK
                OR FSCR_WK.FISCAL_WEEK_KEY = ISS.RESOLUTION_DATE_WEEK
            )
        GROUP BY PTC.PROJECT_NUMBER, FSCR_WK.FISCAL_WEEK_KEY
    ) AGG_DATA
    --FANNING OUT THE DATA ON SATURADAY FOR 7 HOUR WINDOW
    CROSS JOIN FISCAL_WEEK FSC_WK
)
SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',BASE_DATA.SNAPSHOT_WEEK_KEY::STRING
                            ,'COL2',BASE_DATA.PROJECT_NUMBER::STRING
                            ,'COL3',BASE_DATA.ISSUE_WEEK_KEY::STRING
                         )::STRING 
        )::BINARY AS PROJECT_JIRA_CREATED_VS_RESOLVED_KEY,
    BASE_DATA.SNAPSHOT_WEEK_KEY,
    BASE_DATA.ISSUE_WEEK_KEY,
    BASE_DATA.PROJECT_NUMBER,
    BASE_DATA.CREATED_ISSUES,
    BASE_DATA.RESOLVED_ISSUES,
    BASE_DATA.OPEN_ISSUES,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',ISSUE_WEEK_KEY::string, 'col3',PROJECT_NUMBER::string, 'col4',CREATED_ISSUES::string,
    'col5',RESOLVED_ISSUES::string, 'col6',OPEN_ISSUES::string)::string )::BINARY as BIW_MD5_KEY
FROM BASE_DATA