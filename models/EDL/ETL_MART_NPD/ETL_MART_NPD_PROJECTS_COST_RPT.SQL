/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_COST_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_COST_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         21-DEC-2022     CLINT WHETTEN       Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_COST_KEY']-%}

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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_COST_RPT'-%}
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
         description = 'Building ETL table PROJECTS_COST_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_COST_RPT'
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

,period_data as (
    select
        a.lfagileprojectnumber PROJECT_NUMBER,
        b.PERIOD1, b.PERIOD2, b.PERIOD3, b.PERIOD4, b.PERIOD5, b.PERIOD6, b.PERIOD7, b.PERIOD8,
        b.PERIOD9, b.PERIOD10, b.PERIOD11, b.PERIOD12, b.PERIOD13, b.PERIOD14, b.PERIOD15,
        b.PERIOD16, b.PERIOD17, b.PERIOD18, b.PERIOD19, b.PERIOD20, b.PERIOD21, b.PERIOD22,
        b.PERIOD23, b.PERIOD24, b.PERIOD25, b.PERIOD26, b.PERIOD27, b.PERIOD28, b.PERIOD29,
        b.PERIOD30, b.PERIOD31, b.PERIOD32, b.PERIOD33, b.PERIOD34, b.PERIOD35, b.PERIOD36,
        b.PERIOD37, b.PERIOD38, b.PERIOD39, b.PERIOD40
    from {{source ('STG_NPD_DBO','OB_PROJLF')}} a
    inner join {{source ('STG_NPD_DBO','OB_PROJPL')}} b on (
        a.COSTOBJECTNAME = b.COSTOBJECTNAME
        and b.lineitemnumber = 1
    )
    where length(a.lfagileprojectnumber) > 0
)
, pl_data as (
    select
        a.lfagileprojectnumber PROJECT_NUMBER,
        b.PERIOD1, b.PERIOD2, b.PERIOD3, b.PERIOD4, b.PERIOD5, b.PERIOD6, b.PERIOD7, b.PERIOD8,
        b.PERIOD9, b.PERIOD10, b.PERIOD11, b.PERIOD12, b.PERIOD13, b.PERIOD14, b.PERIOD15,
        b.PERIOD16, b.PERIOD17, b.PERIOD18, b.PERIOD19, b.PERIOD20, b.PERIOD21, b.PERIOD22,
        b.PERIOD23, b.PERIOD24, b.PERIOD25, b.PERIOD26, b.PERIOD27, b.PERIOD28, b.PERIOD29,
        b.PERIOD30, b.PERIOD31, b.PERIOD32, b.PERIOD33, b.PERIOD34, b.PERIOD35, b.PERIOD36,
        b.PERIOD37, b.PERIOD38, b.PERIOD39, b.PERIOD40
    from {{source ('STG_NPD_DBO','OB_PROJLF')}} a
    inner join {{source ('STG_NPD_DBO','OB_PROJPL')}} b on (
        a.COSTOBJECTNAME = b.COSTOBJECTNAME
        and b.lineitemnumber = 27
    )
    where length(a.lfagileprojectnumber) > 0
)
, pivot_period as (
    select
        PROJECT_NUMBER
        ,PERIOD
        ,left(QTR,4) COST_YEAR
        ,try_to_number(right(QTR,2)) COST_QTR
    from period_data
    unpivot(QTR for PERIOD in (
        PERIOD1, PERIOD2, PERIOD3, PERIOD4, PERIOD5, PERIOD6, PERIOD7, PERIOD8,
        PERIOD9, PERIOD10, PERIOD11, PERIOD12, PERIOD13, PERIOD14, PERIOD15,
        PERIOD16, PERIOD17, PERIOD18, PERIOD19, PERIOD20, PERIOD21, PERIOD22,
        PERIOD23, PERIOD24, PERIOD25, PERIOD26, PERIOD27, PERIOD28, PERIOD29,
        PERIOD30, PERIOD31, PERIOD32, PERIOD33, PERIOD34, PERIOD35, PERIOD36,
        PERIOD37, PERIOD38, PERIOD39, PERIOD40
    ))
)
, pivot_pl as (
    select
        PROJECT_NUMBER
        ,PERIOD
        ,QTR COST
    from pl_data
    unpivot(QTR for PERIOD in (
        PERIOD1, PERIOD2, PERIOD3, PERIOD4, PERIOD5, PERIOD6, PERIOD7, PERIOD8,
        PERIOD9, PERIOD10, PERIOD11, PERIOD12, PERIOD13, PERIOD14, PERIOD15,
        PERIOD16, PERIOD17, PERIOD18, PERIOD19, PERIOD20, PERIOD21, PERIOD22,
        PERIOD23, PERIOD24, PERIOD25, PERIOD26, PERIOD27, PERIOD28, PERIOD29,
        PERIOD30, PERIOD31, PERIOD32, PERIOD33, PERIOD34, PERIOD35, PERIOD36,
        PERIOD37, PERIOD38, PERIOD39, PERIOD40
    ))
)

,AGG_DATA AS (
    select
        p.PROJECT_NUMBER, p.PERIOD
        ,m.MONTH_BEGIN_DATE PROCESS_DATE
        ,sum((pl.COST / 3)) over (partition by p.PROJECT_NUMBER order by m.MONTH_BEGIN_DATE) RUNNING_COST
    from pivot_period p
    left join pivot_pl pl on (
        p.PROJECT_NUMBER = pl.PROJECT_NUMBER
        and p.PERIOD = pl.PERIOD
    )
    inner join {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.MONTH m on (
        m.YEAR = p.COST_YEAR
        and m.QUARTER_NUMBER = p.COST_QTR
    )
)
SELECT
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',PROJECT_NUMBER::STRING
                            ,'COL3',PROCESS_DATE::STRING
                         )::STRING 
        )::BINARY AS PROJECT_COST_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY, 
    PROJECT_NUMBER,
    PERIOD,
    PROCESS_DATE,
    RUNNING_COST,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',PROJECT_NUMBER::string, 'col3',PROCESS_DATE::string,
    'col4',RUNNING_COST::string)::string )::BINARY as BIW_MD5_KEY
FROM AGG_DATA
CROSS JOIN FISCAL_WEEK FSC_WK