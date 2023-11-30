/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_ASSIGNMENTBASELINETIMEPHASEDDATASET_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_ASSIGNMENTBASELINETIMEPHASEDDATASET_WEEKLY_SNAPSHOT 
pk: AssignmentId ,BaselineNumber ,ProjectId,TimeByDay 
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['AssignmentId' ,'BaselineNumber' ,'ProjectId','TimeByDay']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_ASSIGNMENTBASELINETIMEPHASEDDATASET_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table ASSIGNMENTBASELINETIMEPHASEDDATASET_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_ASSIGNMENTBASELINETIMEPHASEDDATASET_WEEKLY_SNAPSHOT'
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

,ASSIGNMENTBASELINETIMEPHASEDDATASET AS (
SELECT ASSIGNMENTID
, BASELINENUMBER
, PROJECTID
, TIMEBYDAY
, ASSIGNMENTBASELINEBUDGETCOST
, ASSIGNMENTBASELINEBUDGETMATERIALWORK
, ASSIGNMENTBASELINEBUDGETWORK
, ASSIGNMENTBASELINECOST
, ASSIGNMENTBASELINEMATERIALWORK
, ASSIGNMENTBASELINEMODIFIEDDATE
, ASSIGNMENTBASELINEWORK
, FISCALPERIODID
, PROJECTNAME
, RESOURCEID
, TASKID
, TASKNAME
, LINKEDASSIGNMENT
, LINKEDBASELINE
, LINKEDPROJECT
, LINKEDTASKS
, LINKEDTIME
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','ASSIGNMENTBASELINETIMEPHASEDDATASET')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY AssignmentId ,BaselineNumber ,ProjectId,TimeByDay  ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.ASSIGNMENTID::STRING
                            ,'COL3',STG.BaselineNumber::STRING 
                            ,'COL4',STG.ProjectId::STRING 
                            ,'COL5',STG.TimeByDay::STRING 
                         )::STRING 
        )::BINARY AS ASSIGNMENTBASELINETIMEPHASEDDATASET_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,ASSIGNMENTID
, STG.BASELINENUMBER
, STG.PROJECTID
, STG.TIMEBYDAY
, STG.ASSIGNMENTBASELINEBUDGETCOST
, STG.ASSIGNMENTBASELINEBUDGETMATERIALWORK
, STG.ASSIGNMENTBASELINEBUDGETWORK
, STG.ASSIGNMENTBASELINECOST
, STG.ASSIGNMENTBASELINEMATERIALWORK
, STG.ASSIGNMENTBASELINEMODIFIEDDATE
, STG.ASSIGNMENTBASELINEWORK
, STG.FISCALPERIODID
, STG.PROJECTNAME
, STG.RESOURCEID
, STG.TASKID
, STG.TASKNAME
, STG.LINKEDASSIGNMENT
, STG.LINKEDBASELINE
, STG.LINKEDPROJECT
, STG.LINKEDTASKS
,STG.LINKEDTIME
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
,md5(object_construct ('COL1',ASSIGNMENTID::string ,'COL2',BASELINENUMBER::string ,'COL3',PROJECTID::string ,'COL4',TIMEBYDAY::string ,'COL5',ASSIGNMENTBASELINEBUDGETCOST::string ,'COL6',ASSIGNMENTBASELINEBUDGETMATERIALWORK::string ,'COL7',ASSIGNMENTBASELINEBUDGETWORK::string ,'COL8',ASSIGNMENTBASELINECOST::string ,'COL9',ASSIGNMENTBASELINEMATERIALWORK::string ,'COL10',ASSIGNMENTBASELINEMODIFIEDDATE::string ,'COL11',ASSIGNMENTBASELINEWORK::string ,'COL12',FISCALPERIODID::string ,'COL13',PROJECTNAME::string ,'COL14',RESOURCEID::string ,'COL15',TASKID::string ,'COL16',TASKNAME::string ,'COL17',LINKEDASSIGNMENT::string ,'COL18',LINKEDBASELINE::string ,'COL19',LINKEDPROJECT::string ,'COL20',LINKEDTASKS::string ,'COL21',LINKEDTIME::string )::string )::BINARY as BIW_MD5_KEY

FROM     
ASSIGNMENTBASELINETIMEPHASEDDATASET  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
