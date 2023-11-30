/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_ISSUETASKASSOCIATIONS_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_ISSUETASKASSOCIATIONS_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['ISSUEID','PROJECTID','RELATIONSHIPTYPE','TASKID']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_ISSUETASKASSOCIATIONS_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table ISSUETASKASSOCIATIONS_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_ISSUETASKASSOCIATIONS_WEEKLY_SNAPSHOT'
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

,ISSUETASKASSOCIATIONS AS (
SELECT ISSUEID
, PROJECTID
, RELATIONSHIPTYPE
, TASKID
, PROJECTNAME
, RELATEDPROJECTID
, RELATEDPROJECTNAME
, TASKNAME
, TITLE
, LINKEDISSUE
, LINKEDPROJECT
, LINKEDRELATEDPROJECT
, LINKEDTASK
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','ISSUETASKASSOCIATIONS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ISSUEID,PROJECTID,RELATIONSHIPTYPE,TASKID  ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.ISSUEID::STRING
                            ,'COL3',STG.PROJECTID::STRING
                            ,'COL4',STG.RELATIONSHIPTYPE::STRING
                            ,'COL5',STG.TASKID::STRING
                         )::STRING 
        )::BINARY AS ISSUETASKASSOCIATIONS_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,ISSUEID
, STG.PROJECTID
, STG.RELATIONSHIPTYPE
, STG.TASKID
, STG.PROJECTNAME
, STG.RELATEDPROJECTID
, STG.RELATEDPROJECTNAME
, STG.TASKNAME
, STG.TITLE
, STG.LINKEDISSUE
, STG.LINKEDPROJECT
, STG.LINKEDRELATEDPROJECT
,STG.LINKEDTASK
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
   ,md5(object_construct ('COL1',ISSUEID::string ,'COL2',PROJECTID::string ,'COL3',RELATIONSHIPTYPE::string ,'COL4',TASKID::string ,'COL5',PROJECTNAME::string ,'COL6',RELATEDPROJECTID::string ,'COL7',RELATEDPROJECTNAME::string ,'COL8',TASKNAME::string ,'COL9',TITLE::string ,'COL10',LINKEDISSUE::string ,'COL11',LINKEDPROJECT::string ,'COL12',LINKEDRELATEDPROJECT::string ,'COL13',LINKEDTASK::string )::string )::BINARY as BIW_MD5_KEY

FROM     
ISSUETASKASSOCIATIONS  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
