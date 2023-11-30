/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_RISKS_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_RISKS_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECTID','RISKID']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_RISKS_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table RISKS_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_RISKS_WEEKLY_SNAPSHOT'
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

,RISKS AS (
SELECT PROJECTID
, RISKID
, ASSIGNEDTORESOURCE
, CATEGORY
, CONTINGENCYPLAN
, COST
, COSTEXPOSURE
, CREATEBYRESOURCE
, CREATEDDATE
, DESCRIPTION
, DUEDATE
, EXPOSURE
, IMPACT
, ISFOLDER
, ITEMRELATIVEURLPATH
, MITIGATIONPLAN
, MODIFIEDBYRESOURCE
, MODIFIEDDATE
, NUMBEROFATTACHMENTS
, OWNER
, PROBABILITY
, PROJECTNAME
, STATUS
, TITLE
, TRIGGERDESCRIPTION
, TRIGGERTASK
, LINKEDPROJECT
, LINKEDRELATEDISSUES
, LINKEDSUBRISKS
, LINKEDTASKS
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','RISKS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECTID ,RISKID ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.PROJECTID::STRING
                            ,'COL3',STG.RISKID::STRING
                         )::STRING 
        )::BINARY AS RISKS_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,PROJECTID
, STG.RISKID
, STG.ASSIGNEDTORESOURCE
, STG.CATEGORY
, STG.CONTINGENCYPLAN
, STG.COST
, STG.COSTEXPOSURE
, STG.CREATEBYRESOURCE
, STG.CREATEDDATE
, STG.DESCRIPTION
, STG.DUEDATE
, STG.EXPOSURE
, STG.IMPACT
, STG.ISFOLDER
, STG.ITEMRELATIVEURLPATH
, STG.MITIGATIONPLAN
, STG.MODIFIEDBYRESOURCE
, STG.MODIFIEDDATE
, STG.NUMBEROFATTACHMENTS
, STG.OWNER
, STG.PROBABILITY
, STG.PROJECTNAME
, STG.STATUS
, STG.TITLE
, STG.TRIGGERDESCRIPTION
, STG.TRIGGERTASK
, STG.LINKEDPROJECT
, STG.LINKEDRELATEDISSUES
, STG.LINKEDSUBRISKS
,STG.LINKEDTASKS
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
   ,md5(object_construct ('COL1',PROJECTID::string ,'COL2',RISKID::string ,'COL3',ASSIGNEDTORESOURCE::string ,'COL4',CATEGORY::string ,'COL5',CONTINGENCYPLAN::string ,'COL6',COST::string ,'COL7',COSTEXPOSURE::string ,'COL8',CREATEBYRESOURCE::string ,'COL9',CREATEDDATE::string ,'COL10',DESCRIPTION::string ,'COL11',DUEDATE::string ,'COL12',EXPOSURE::string ,'COL13',IMPACT::string ,'COL14',ISFOLDER::string ,'COL15',ITEMRELATIVEURLPATH::string ,'COL16',MITIGATIONPLAN::string ,'COL17',MODIFIEDBYRESOURCE::string ,'COL18',MODIFIEDDATE::string ,'COL19',NUMBEROFATTACHMENTS::string ,'COL20',OWNER::string ,'COL21',PROBABILITY::string ,'COL22',PROJECTNAME::string ,'COL23',STATUS::string ,'COL24',TITLE::string ,'COL25',TRIGGERDESCRIPTION::string ,'COL26',TRIGGERTASK::string ,'COL27',LINKEDPROJECT::string ,'COL28',LINKEDRELATEDISSUES::string ,'COL29',LINKEDSUBRISKS::string ,'COL30',LINKEDTASKS::string )::string )::BINARY as BIW_MD5_KEY

FROM     
RISKS  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
