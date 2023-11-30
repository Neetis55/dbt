/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_TASK_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_TASK_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022     KALI DANDAPANI      Initial Version
1.1         03-JAN-2023     CLINT WHETTEN       Added PROJECT_OWNER, PROJECT_INTERNAL_URL, TASK_OWNER
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['MSPROJECT_TASK_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_TASK_RPT'-%}
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
         description = 'Building ETL table PROJECTS_TASK_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_TASK_RPT'
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

,PROJECTS AS (
    SELECT 
        AGILE AS PROJECT_NUMBER,
        PROJECTID AS PROJECT_ID,
        PROJECTNAME AS PROJECT_NAME,
        PROJECTOWNERNAME AS PROJECT_OWNER,
        PROJECTWORKSPACEINTERNALURL AS PROJECT_INTERNAL_URL
    FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','PROJECTS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY AGILE ORDER BY BIW_UPD_DTTM DESC) =1)
)

,TASKS AS (
    SELECT 
        PROJECTID AS PROJECT_ID,
        TASKID AS TASK_ID,
        TASKNAME AS TASK_NAME,
        TASKSTARTDATE AS TASK_START_DATE,
        TASKFINISHDATE AS TASK_FINISH_DATE,
        TASKACTUALSTARTDATE AS TASK_ACTUAL_START_DATE,
        TASKACTUALFINISHDATE AS TASK_ACTUAL_FINISH_DATE,
        TASKLATESTART AS TASK_LATE_START,
        TASKLATEFINISH AS TASK_LATE_FINISH,
        TASKPERCENTCOMPLETED AS TASK_PERCENT_COMPLETED,
        CASE 
            WHEN TASKFINISHDATE < CURRENT_DATE
            AND  TASKPERCENTCOMPLETED <> 100 
                THEN TRUE
            ELSE FALSE
        END::BOOLEAN AS IS_PAST_DUE,
        CASE 
            WHEN TASKFINISHDATE  <= CURRENT_DATE + 6 
            AND  TASKFINISHDATE  >= CURRENT_DATE
                THEN TRUE
            ELSE FALSE
        END::BOOLEAN AS IS_DUE_IN_NEXT_7DAYS,
        TASKCREATEDDATE AS TASK_CREATED_DATE
    FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','TASKS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECTID,TASKID ORDER BY  BIW_UPD_DTTM DESC) =1)
)

,ASSIGNMENTS AS (
    SELECT
        PROJECTID AS PROJECT_ID,
        TASKID AS TASK_ID,
        RESOURCENAME AS TASK_OWNER
    FROM
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','ASSIGNMENTS')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECTID,TASKID ORDER BY  BIW_UPD_DTTM DESC) =1)    
)

SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',PRJ.PROJECT_NUMBER::STRING
                            ,'COL3',TSK.TASK_ID::STRING
                         )::STRING 
        )::BINARY AS MSPROJECT_TASK_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
    PRJ.PROJECT_NUMBER,
    PRJ.PROJECT_ID,
    PRJ.PROJECT_NAME,
    PRJ.PROJECT_OWNER,
    PRJ.PROJECT_INTERNAL_URL,
    TSK.TASK_ID,
    TSK.TASK_NAME,
    ASG.TASK_OWNER,
    TSK.TASK_START_DATE,
    TSK.TASK_FINISH_DATE,
    TSK.TASK_ACTUAL_START_DATE,
    TSK.TASK_ACTUAL_FINISH_DATE,
    TSK.TASK_LATE_START,
    TSK.TASK_LATE_FINISH,
    TSK.TASK_PERCENT_COMPLETED,
    TSK.IS_PAST_DUE,
    TSK.IS_DUE_IN_NEXT_7DAYS,
    TSK.TASK_CREATED_DATE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',PRJ.PROJECT_NUMBER::string, 'col3',PRJ.PROJECT_ID::string,
    'col4',PRJ.PROJECT_NAME::string, 'col5',PRJ.PROJECT_OWNER::string, 'col6',PRJ.PROJECT_INTERNAL_URL::string, 'col7',TSK.TASK_ID::string, 
    'col8',TASK_NAME::string, 'col9',TASK_NAME::string, 'col10',TASK_START_DATE::string, 'col11',TASK_FINISH_DATE::string, 
    'col12',TASK_ACTUAL_START_DATE::string, 'col13',TASK_ACTUAL_FINISH_DATE::string, 'col14',TASK_LATE_START::string,
    'col15',TASK_LATE_FINISH::string, 'col16',TASK_PERCENT_COMPLETED::string, 'col17',IS_PAST_DUE::string, 'col18',IS_DUE_IN_NEXT_7DAYS::string,
    'col19',TASK_CREATED_DATE::string)::string )::BINARY as BIW_MD5_KEY
FROM     
PROJECTS  PRJ 
LEFT JOIN TASKS TSK
    ON PRJ.PROJECT_ID = TSK.PROJECT_ID
LEFT JOIN ASSIGNMENTS ASG
    ON (
        TSK.PROJECT_ID = ASG.PROJECT_ID
        AND TSK.TASK_ID = ASG.TASK_ID
    )
CROSS JOIN FISCAL_WEEK FSC_WK