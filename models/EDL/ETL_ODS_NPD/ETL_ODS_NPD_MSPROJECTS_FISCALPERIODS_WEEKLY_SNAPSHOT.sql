/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_ODS_NPD_MSPROJECTS_FISCALPERIODS_WEEKLY_SNAPSHOT 
-- dbt build --full-refresh --select ETL_ODS_NPD_MSPROJECTS_FISCALPERIODS_WEEKLY_SNAPSHOT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['FISCALPERIODID']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_ODS_NPD_MSPROJECTS_FISCALPERIODS_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ETL table FISCALPERIODS_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_ODS_NPD'
        ,alias= 'MSPROJECTS_FISCALPERIODS_WEEKLY_SNAPSHOT'
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

,FISCALPERIODS AS (
SELECT FISCALPERIODID
, CREATEDDATE
, FISCALPERIODFINISH
, FISCALPERIODMODIFIEDDATE
, FISCALPERIODNAME
, FISCALPERIODQUARTER
, FISCALPERIODSTART
, FISCALPERIODYEAR
, BIW_INS_DTTM
, BIW_UPD_DTTM
FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','FISCALPERIODS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY FISCALPERIODID  ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',STG.FISCALPERIODID::STRING
                         )::STRING 
        )::BINARY AS FISCALPERIODS_KEY 
    ,FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY
    ,FISCALPERIODID
, STG.CREATEDDATE
, STG.FISCALPERIODFINISH
, STG.FISCALPERIODMODIFIEDDATE
, STG.FISCALPERIODNAME
, STG.FISCALPERIODQUARTER
, STG.FISCALPERIODSTART
,STG.FISCALPERIODYEAR

,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID 
   ,md5(object_construct ('COL1',FISCALPERIODID::string ,'COL2',CREATEDDATE::string ,'COL3',FISCALPERIODFINISH::string ,'COL4',FISCALPERIODMODIFIEDDATE::string ,'COL5',FISCALPERIODNAME::string ,'COL6',FISCALPERIODQUARTER::string ,'COL7',FISCALPERIODSTART::string ,'COL8',FISCALPERIODYEAR::string )::string )::BINARY as BIW_MD5_KEY

FROM     
FISCALPERIODS  STG 

CROSS JOIN FISCAL_WEEK FSC_WK
