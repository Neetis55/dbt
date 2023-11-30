/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_ISSUES_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_ISSUES_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['ISSUEID','PROJECTID']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_ISSUES_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table ISSUES_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_ISSUES_WEEKLY_SNAPSHOT'
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

,ISSUES AS (
SELECT ISSUEID
, PROJECTID
, ASSIGNEDTORESOURCE
, CATEGORY
, CREATEBYRESOURCE
, CREATEDDATE
, DISCUSSION
, DUEDATE
, ISFOLDER
, ITEMRELATIVEURLPATH
, MODIFIEDBYRESOURCE
, MODIFIEDDATE
, NUMBEROFATTACHMENTS
, OWNER
, PRIORITY
, PROJECTNAME
, RESOLUTION
, STATUS
, TITLE
, LINKEDPROJECT
, LINKEDRELATEDRISKS
, LINKEDSUBISSUES
, LINKEDTASKS
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','ISSUES')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ISSUEID,PROJECTID ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.ISSUEID::STRING
                            ,'COL3',STG.PROJECTID::STRING
                         )::STRING 
        )::BINARY AS ISSUES_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
,stg.ISSUEID
, STG.PROJECTID
, STG.ASSIGNEDTORESOURCE
, STG.CATEGORY
, STG.CREATEBYRESOURCE
, STG.CREATEDDATE
, STG.DISCUSSION
, STG.DUEDATE
, STG.ISFOLDER
, STG.ITEMRELATIVEURLPATH
, STG.MODIFIEDBYRESOURCE
, STG.MODIFIEDDATE
, STG.NUMBEROFATTACHMENTS
, STG.OWNER
, STG.PRIORITY
, STG.PROJECTNAME
, STG.RESOLUTION
, STG.STATUS
, STG.TITLE
, STG.LINKEDPROJECT
, STG.LINKEDRELATEDRISKS
, STG.LINKEDSUBISSUES
,STG.LINKEDTASKS
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
,md5(object_construct ('COL1',ISSUEID::string ,'COL2',PROJECTID::string ,'COL3',ASSIGNEDTORESOURCE::string ,'COL4',CATEGORY::string ,'COL5',CREATEBYRESOURCE::string ,'COL6',CREATEDDATE::string ,'COL7',DISCUSSION::string ,'COL8',DUEDATE::string ,'COL9',ISFOLDER::string ,'COL10',ITEMRELATIVEURLPATH::string ,'COL11',MODIFIEDBYRESOURCE::string ,'COL12',MODIFIEDDATE::string ,'COL13',NUMBEROFATTACHMENTS::string ,'COL14',OWNER::string ,'COL15',PRIORITY::string ,'COL16',PROJECTNAME::string ,'COL17',RESOLUTION::string ,'COL18',STATUS::string ,'COL19',TITLE::string ,'COL20',LINKEDPROJECT::string ,'COL21',LINKEDRELATEDRISKS::string ,'COL22',LINKEDSUBISSUES::string ,'COL23',LINKEDTASKS::string )::string )::BINARY as BIW_MD5_KEY

FROM     
ISSUES  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
