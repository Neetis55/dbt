
/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_TASKTIMEPHASEDDATASET_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_TASKTIMEPHASEDDATASET_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECTID','TASKID' ,'TIMEBYDAY']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_TASKTIMEPHASEDDATASET_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table TASKTIMEPHASEDDATASET_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_TASKTIMEPHASEDDATASET_WEEKLY_SNAPSHOT'
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

,TASKTIMEPHASEDDATASET AS (
SELECT PROJECTID
, TASKID
, TIMEBYDAY
, FISCALPERIODID
, PROJECTNAME
, TASKACTUALCOST
, TASKACTUALWORK
, TASKBUDGETCOST
, TASKBUDGETWORK
, TASKCOST
, TASKISACTIVE
, TASKISPROJECTSUMMARY
, TASKMODIFIEDDATE
, TASKNAME
, TASKOVERTIMEWORK
, TASKRESOURCEPLANWORK
, TASKWORK
, LINKEDPROJECT
, LINKEDTASK
, LINKEDTIME
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','TASKTIMEPHASEDDATASET')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECTID,TASKID ,TIMEBYDAY ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.PROJECTID::STRING
                            ,'COL3',STG.TASKID::STRING
                            ,'COL4',STG.TIMEBYDAY::STRING
                         )::STRING 
        )::BINARY AS TASKTIMEPHASEDDATASET_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,PROJECTID
, STG.TASKID
, STG.TIMEBYDAY
, STG.FISCALPERIODID
, STG.PROJECTNAME
, STG.TASKACTUALCOST
, STG.TASKACTUALWORK
, STG.TASKBUDGETCOST
, STG.TASKBUDGETWORK
, STG.TASKCOST
, STG.TASKISACTIVE
, STG.TASKISPROJECTSUMMARY
, STG.TASKMODIFIEDDATE
, STG.TASKNAME
, STG.TASKOVERTIMEWORK
, STG.TASKRESOURCEPLANWORK
, STG.TASKWORK
, STG.LINKEDPROJECT
, STG.LINKEDTASK
,STG.LINKEDTIME
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
   ,md5(object_construct ('COL1',PROJECTID::string ,'COL2',TASKID::string ,'COL3',TIMEBYDAY::string ,'COL4',FISCALPERIODID::string ,'COL5',PROJECTNAME::string ,'COL6',TASKACTUALCOST::string ,'COL7',TASKACTUALWORK::string ,'COL8',TASKBUDGETCOST::string ,'COL9',TASKBUDGETWORK::string ,'COL10',TASKCOST::string ,'COL11',TASKISACTIVE::string ,'COL12',TASKISPROJECTSUMMARY::string ,'COL13',TASKMODIFIEDDATE::string ,'COL14',TASKNAME::string ,'COL15',TASKOVERTIMEWORK::string ,'COL16',TASKRESOURCEPLANWORK::string ,'COL17',TASKWORK::string ,'COL18',LINKEDPROJECT::string ,'COL19',LINKEDTASK::string ,'COL20',LINKEDTIME::string )::string )::BINARY as BIW_MD5_KEY

FROM     
TASKTIMEPHASEDDATASET  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
