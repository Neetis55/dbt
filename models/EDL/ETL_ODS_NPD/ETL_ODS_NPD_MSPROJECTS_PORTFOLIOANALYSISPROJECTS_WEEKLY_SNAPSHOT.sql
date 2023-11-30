/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_PORTFOLIOANALYSISPROJECTS_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_PORTFOLIOANALYSISPROJECTS_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['ANALYSISID','PROJECTID']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_PORTFOLIOANALYSISPROJECTS_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table PORTFOLIOANALYSISPROJECTS_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_PORTFOLIOANALYSISPROJECTS_WEEKLY_SNAPSHOT'
        ,tags =['ODS_NPD']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH FISCAL_WEEK AS 
(
    SELECT 
        DISTINCT FISCAL_WEEK_KEY
    FROM 
    {{ref('MART_DATE') }}
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() )::DATE
)

,PORTFOLIOANALYSISPROJECTS AS (
SELECT ANALYSISID
, PROJECTID
, ABSOLUTEPRIORITY
, ANALYSISNAME
, DURATION
, FINISHNOLATERTHAN
, LOCKED
, ORIGINALENDDATE
, ORIGINALSTARTDATE
, PRIORITY
, PROJECTNAME
, STARTDATE
, STARTNOEARLIERTHAN
, LINKEDANALYSIS
, LINKEDPROJECT
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','PORTFOLIOANALYSISPROJECTS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ANALYSISID,PROJECTID  ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.ANALYSISID::STRING
                            ,'COL3',STG.PROJECTID::STRING
                         )::STRING 
        )::BINARY AS PORTFOLIOANALYSISPROJECTS_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,ANALYSISID
, STG.PROJECTID
, STG.ABSOLUTEPRIORITY
, STG.ANALYSISNAME
, STG.DURATION
, STG.FINISHNOLATERTHAN
, STG.LOCKED
, STG.ORIGINALENDDATE
, STG.ORIGINALSTARTDATE
, STG.PRIORITY
, STG.PROJECTNAME
, STG.STARTDATE
, STG.STARTNOEARLIERTHAN
, STG.LINKEDANALYSIS
,STG.LINKEDPROJECT
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
   ,md5(object_construct ('COL1',ANALYSISID::string ,'COL2',PROJECTID::string ,'COL3',ABSOLUTEPRIORITY::string ,'COL4',ANALYSISNAME::string ,'COL5',DURATION::string ,'COL6',FINISHNOLATERTHAN::string ,'COL7',LOCKED::string ,'COL8',ORIGINALENDDATE::string ,'COL9',ORIGINALSTARTDATE::string ,'COL10',PRIORITY::string ,'COL11',PROJECTNAME::string ,'COL12',STARTDATE::string ,'COL13',STARTNOEARLIERTHAN::string ,'COL14',LINKEDANALYSIS::string ,'COL15',LINKEDPROJECT::string )::string )::BINARY as BIW_MD5_KEY

FROM     
PORTFOLIOANALYSISPROJECTS  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
