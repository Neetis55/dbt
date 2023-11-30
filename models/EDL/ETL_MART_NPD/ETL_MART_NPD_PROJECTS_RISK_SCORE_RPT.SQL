/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_RISK_SCORE_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_RISK_SCORE_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         21-DEC-2022     CLINT WHETTEN       Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_RISK_SCORE_KEY']-%}

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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_RISK_SCORE_RPT'-%}
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
         description = 'Building ETL table PROJECTS_RISK_SCORE_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_RISK_SCORE_RPT'
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

,PROJECTS as (
    SELECT
        PROJECTID, 
        AGILE, 
        PROJECTSTARTDATE, 
        PROJECTFINISHDATE, 
        PROJECTMODIFIEDDATE
    FROM 
    {{source ('STG_NPD_MSPROJECTS_ODATAV1','PROJECTS')}}
    WHERE AGILE IS NOT NULL
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY PROJECTID ORDER BY PROJECTMODIFIEDDATE DESC) = 1)
)

,TASK_RISKS AS (
    select 
        p.PROJECTID, 
        p.AGILE, 
        t.TASKID, 
        t.PARENTTASKID, 
        t.TASKNAME, 
        tr.TASKRISKID,
        tr.PARENTTASKRISKID, 
        tr.TASKRISKNAME, 
        tr.RISKSCORE, 
        tr.TASKINDEX,
        tr.TASKFINISHDATE, 
        DATE(DATE_TRUNC(day, t.TASKMODIFIEDDATE)) TASKMODIFIEDDATE,
        DATE(DATE_TRUNC(month, t.TASKSTARTDATE)) TASKSTARTDATE, 
        DATE(DATE_TRUNC(month, tr.TASKRISKSTARTDATE)) TASKRISKSTARTDATE
    FROM  PROJECTS p
    INNER JOIN (
        SELECT
            PROJECTID,
            TASKID,
            PARENTTASKID,
            TASKNAME,
            DATE (DATE_TRUNC(day, TASKMODIFIEDDATE)) AS TASKMODIFIEDDATE,
            DATE (DATE_TRUNC(month, TASKSTARTDATE)) AS TASKSTARTDATE
        FROM {{source ('STG_NPD_MSPROJECTS_ODATAV1','TASKS')}}
        WHERE (RISKFLAG IS NULL OR RISKFLAG = FALSE)
            AND (MITIGATIONFLAG IS NULL OR MITIGATIONFLAG = FALSE)
        QUALIFY (DENSE_RANK() OVER (PARTITION BY PROJECTID ORDER BY BIW_UPD_DTTM DESC) = 1)
    ) t on (p.PROJECTID = t.PROJECTID)
    INNER JOIN (
        SELECT
            PROJECTID,
            TASKID AS TASKRISKID,
            PARENTTASKID AS PARENTTASKRISKID,
            TASKINDEX,
            TASKNAME AS TASKRISKNAME,
            DATE (DATE_TRUNC(month, TASKSTARTDATE)) AS TASKRISKSTARTDATE,
            RISKSCORE,
            TASKFINISHDATE
        FROM {{source ('STG_NPD_MSPROJECTS_ODATAV1','TASKS')}}
        WHERE RISKFLAG = TRUE
        QUALIFY (DENSE_RANK() OVER (PARTITION BY PROJECTID, TASKID ORDER BY BIW_UPD_DTTM DESC) = 1)
    ) tr on (
        t.PROJECTID = tr.PROJECTID
        AND t.TASKID = tr.PARENTTASKRISKID
    )
)

,TASK_MITIGATIONS AS (
    SELECT
        tr.PROJECTID, 
        tr.AGILE, 
        tr.TASKID, 
        tr.PARENTTASKID, 
        tr.TASKNAME,
        tr.TASKRISKID, 
        tr.PARENTTASKRISKID, 
        tr.TASKINDEX, 
        t.TASKID TASKMITID, 
        t.PARENTTASKID PARENTTASKMITID,
        tr.TASKRISKNAME, 
        tr.RISKSCORE, 
        t.TASKNAME TASKMITNAME, 
        t.RISKSCORE MITRISKSCORE, 
        t.TASKINDEX MITTASKINDEX,
        tr.TASKSTARTDATE, 
        tr.TASKRISKSTARTDATE, 
        DATE(DATE_TRUNC(month, t.TASKFINISHDATE)) TASKMITFINISHDATE, 
        t.TASKACTUALFINISHDATE
    FROM {{source ('STG_NPD_MSPROJECTS_ODATAV1','TASKS')}} t
    INNER JOIN TASK_RISKS tr ON (
        t.PROJECTID = tr.PROJECTID
        AND t.PARENTTASKID = tr.TASKRISKID
    )
    WHERE MITIGATIONFLAG = TRUE
    QUALIFY (DENSE_RANK() OVER (PARTITION BY t.PROJECTID, t.PARENTTASKID ORDER BY BIW_UPD_DTTM DESC) = 1)
)

,START_END AS (
    SELECT
        tm.PROJECTID, 
        tm.AGILE,
        tm.TASKRISKID, 
        tm.TASKRISKNAME, 
        tm.TASKINDEX,
        tm.RISKSCORE, 
        tm.FIRSTMITRISKSCORE, 
        tm.LASTMITRISKSCORE, 
        tm.STARTDATE,
        tm.TASKRISKSTARTDATE, 
        tm.LASTTASKMITFINISHDATE, 
        m.MONTH_BEGIN_DATE TASKRISKPROCESSDATE
    FROM (
        SELECT DISTINCT
            PROJECTID
            ,AGILE
            ,TASKRISKID
            ,TASKRISKNAME
            ,first_value(TASKINDEX) over (partition by PROJECTID, TASKRISKID ORDER BY TASKRISKSTARTDATE) TASKINDEX
            ,first_value(RISKSCORE) over (partition by PROJECTID, TASKRISKID ORDER BY TASKRISKSTARTDATE) RISKSCORE
            ,first_value(MITRISKSCORE) over (partition by PROJECTID, TASKRISKID ORDER BY TASKMITFINISHDATE) FIRSTMITRISKSCORE
            ,last_value(MITRISKSCORE) over (partition by PROJECTID, TASKRISKID ORDER BY TASKMITFINISHDATE) LASTMITRISKSCORE
            ,first_value(TASKRISKSTARTDATE) over (partition by PROJECTID, TASKRISKID ORDER BY TASKRISKSTARTDATE) TASKRISKSTARTDATE
            ,last_value(TASKMITFINISHDATE) over (partition by PROJECTID, TASKRISKID ORDER BY TASKMITFINISHDATE) LASTTASKMITFINISHDATE
            ,first_value(TASKSTARTDATE) over (partition by PROJECTID ORDER BY TASKSTARTDATE) STARTDATE
            ,last_value(TASKMITFINISHDATE) over (partition by PROJECTID ORDER BY TASKMITFINISHDATE) ENDDATE
        FROM TASK_MITIGATIONS
    ) tm
    INNER JOIN {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.MONTH m ON (
        m.MONTH_BEGIN_DATE BETWEEN tm.STARTDATE AND tm.ENDDATE
    )
)

,BASE_DATA AS (
    SELECT DISTINCT
        iff(m.PROJECTID is null, s.PROJECTID, m.PROJECTID) PROJECTID
        ,iff(m.AGILE is null, s.AGILE, m.AGILE) AGILE
        ,iff(m.TASKRISKID is null, s.TASKRISKID, m.TASKRISKID) TASKRISKID
        ,iff(m.TASKRISKNAME is null, s.TASKRISKNAME, m.TASKRISKNAME) TASKRISKNAME
        ,s.STARTDATE
        ,iff(m.TASKINDEX is null, s.TASKINDEX, m.TASKINDEX) TASKINDEX
        ,iff(m.TASKRISKSTARTDATE is null, s.TASKRISKSTARTDATE, m.TASKRISKSTARTDATE) TASKRISKSTARTDATE
        ,iff(m.TASKMITFINISHDATE is null, s.LASTTASKMITFINISHDATE, m.TASKMITFINISHDATE) TASKMITFINISHDATE
        ,s.TASKRISKPROCESSDATE
        ,s.RISKSCORE
        ,m.MITRISKSCORE
        ,lag(MITRISKSCORE, 1, 0) ignore nulls over (partition by iff(m.PROJECTID is null, s.PROJECTID, m.PROJECTID), iff(m.TASKRISKID is null, s.TASKRISKID, m.TASKRISKID) order by TASKRISKPROCESSDATE) PREV_MITRISKSCORE
        ,s.FIRSTMITRISKSCORE
        ,s.LASTMITRISKSCORE
    FROM START_END s
    FULL OUTER JOIN TASK_MITIGATIONS m ON (
        s.PROJECTID = m.PROJECTID
        AND s.TASKRISKID = m.TASKRISKID
        AND s.TASKRISKPROCESSDATE = m.TASKMITFINISHDATE
    )
)

,AGG_DATA AS (
    SELECT
        AGILE PROJECT_NUMBER,
        TASKRISKPROCESSDATE PROCESS_DATE,
        SUM(
            CASE
                WHEN MITRISKSCORE is not null and TASKRISKPROCESSDATE = STARTDATE THEN RISKSCORE
                WHEN MITRISKSCORE is not null and TASKRISKPROCESSDATE = TASKMITFINISHDATE THEN MITRISKSCORE
                WHEN MITRISKSCORE is null and PREV_MITRISKSCORE = 0 THEN RISKSCORE
                WHEN MITRISKSCORE is null and PREV_MITRISKSCORE > 0 THEN PREV_MITRISKSCORE
            END
        ) PROJECT_RISKSCORE
    FROM BASE_DATA
    GROUP BY 1,2
)
SELECT
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',PROJECT_NUMBER::STRING
                            ,'COL3',PROCESS_DATE::STRING
                         )::STRING 
        )::BINARY AS PROJECT_RISK_SCORE_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,    
    PROJECT_NUMBER,
    PROCESS_DATE,
    PROJECT_RISKSCORE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',PROJECT_NUMBER::string, 'col3',PROCESS_DATE::string,
    'col4',PROJECT_RISKSCORE::string)::string )::BINARY as BIW_MD5_KEY
FROM AGG_DATA
CROSS JOIN FISCAL_WEEK FSC_WK