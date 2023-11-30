
/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_RESOURCEDEMANDTIMEPHASEDDATASET_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_RESOURCEDEMANDTIMEPHASEDDATASET_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECTID','RESOURCEID' ,'TIMEBYDAY']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_RESOURCEDEMANDTIMEPHASEDDATASET_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table RESOURCEDEMANDTIMEPHASEDDATASET_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_RESOURCEDEMANDTIMEPHASEDDATASET_WEEKLY_SNAPSHOT'
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

,RESOURCEDEMANDTIMEPHASEDDATASET AS (
SELECT PROJECTID
, RESOURCEID
, TIMEBYDAY
, FISCALPERIODID
, PROJECTNAME
, RESOURCEDEMAND
, RESOURCEDEMANDMODIFIEDDATE
, RESOURCENAME
, RESOURCEPLANUTILIZATIONDATE
, RESOURCEPLANUTILIZATIONTYPE
, LINKEDPROJECT
, LINKEDRESOURCE
, LINKEDTIME
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','RESOURCEDEMANDTIMEPHASEDDATASET')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECTID,RESOURCEID ,TIMEBYDAY ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.PROJECTID::STRING
                            ,'COL3',STG.RESOURCEID::STRING
                            ,'COL4',STG.TIMEBYDAY::STRING
                         )::STRING 
        )::BINARY AS RESOURCEDEMANDTIMEPHASEDDATASET_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,PROJECTID
, STG.RESOURCEID
, STG.TIMEBYDAY
, STG.FISCALPERIODID
, STG.PROJECTNAME
, STG.RESOURCEDEMAND
, STG.RESOURCEDEMANDMODIFIEDDATE
, STG.RESOURCENAME
, STG.RESOURCEPLANUTILIZATIONDATE
, STG.RESOURCEPLANUTILIZATIONTYPE
, STG.LINKEDPROJECT
, STG.LINKEDRESOURCE
,STG.LINKEDTIME
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
   ,md5(object_construct ('COL1',PROJECTID::string ,'COL2',RESOURCEID::string ,'COL3',TIMEBYDAY::string ,'COL4',FISCALPERIODID::string ,'COL5',PROJECTNAME::string ,'COL6',RESOURCEDEMAND::string ,'COL7',RESOURCEDEMANDMODIFIEDDATE::string ,'COL8',RESOURCENAME::string ,'COL9',RESOURCEPLANUTILIZATIONDATE::string ,'COL10',RESOURCEPLANUTILIZATIONTYPE::string ,'COL11',LINKEDPROJECT::string ,'COL12',LINKEDRESOURCE::string ,'COL13',LINKEDTIME::string)::string )::BINARY as BIW_MD5_KEY

FROM     
RESOURCEDEMANDTIMEPHASEDDATASET  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
