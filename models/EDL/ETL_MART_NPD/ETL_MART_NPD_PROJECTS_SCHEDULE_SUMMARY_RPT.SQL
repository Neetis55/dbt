/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022     KALI DANDAPANI      Initial Version
1.1         04-JAN-2023     CLINT WHETTEN       Added measures from FACT_PPM_PTC_DTL
1.2         18-JAN-2023     CLINT WHETTEN       Updated OS Date logic
1.3         19-JAN-2023     CLINT WHETTEN       Updated project step list to enable GATE_TYPE: TAPEOUT[1-5], added logic to enable correct SORT_ORDER
1.4         20-JAN-2023     CLINT WHETTEN       Added NEXT_MILESTONE
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_SCHEDULE_SUMMARY_KEY']-%}

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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_SCHEDULE_SUMMARY_RPT'-%}
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
         description = 'Building ETL table PROJECTS_SCHEDULE_SUMMARY_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_SCHEDULE_SUMMARY_RPT'
        ,tags =['MART_NPD']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

{%- set GET_STEP_SQL -%}
SELECT 
    STEP_TYPE,
    STEP_NAME,
    OS_STEP_COLUMN_NAME,
    CS_STEP_COLUMN_NAME,
    FORECAST_STEP_COLUMN_NAME,
    ACTUAL_STEP_COLUMN_NAME
FROM  {{ref('MART_NPD_STEP_REF')}}
--WHERE STEP_TYPE ='GATE'
{%- endset -%}

{%- set GET_FORECAST_STEP_SQL -%}
SELECT DISTINCT
    STEP_TYPE,
    STEP_NAME,
    FORECAST_STEP_COLUMN_NAME
FROM  {{ref('MART_NPD_STEP_REF')}}
WHERE FORECAST_STEP_COLUMN_NAME IS NOT NULL
{%- endset -%}

{# -- Prevent querying of db in parsing mode #}
{%- if execute -%}
{%- set list_all_step = run_query(GET_STEP_SQL) -%}
{%- set list_forecast_step = run_query(GET_FORECAST_STEP_SQL) -%}
{%- endif -%}

WITH STEP_REF AS 
(
    SELECT 
        STEP_TYPE,
        STEP_NAME,
        SORT_ORDER,
        FORECAST_STEP_COLUMN_NAME,
        OS_STEP_COLUMN_NAME,
        CS_STEP_COLUMN_NAME,
        ACTUAL_STEP_COLUMN_NAME
    FROM  {{ref('MART_NPD_STEP_REF')}}
) 
-- STEP 1 GET LIST OF PROJECTS
,DIM_PPM_PROJECT_DTL AS (
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,
        PROJECT_NAME
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_PROJECT_DTL')}}  
   -- WHERE PROJECT_NUMBER='N06IR'
    WHERE PROJECT_TYPE = 'NPD'
        AND PROJECT_WIP_EXCEPTION IN ( 'PROJECT','EXCEPTION')
        AND ONSEMICONDUCTOR_STATUS NOT IN ('6 Cancelled (Customer)','7 Cancelled (Internal)' )
        AND PROJECT_STATE NOT IN ('Template','Proposed')
        AND PROJECT_STATUS IN ('In Process')
        AND CURRENT_PHASE NOT IN('6 Sustain','7 Retire')
        AND (PROJECT_NUMBER IS NOT NULL  OR LEN(PROJECT_NUMBER) <> 0)
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER ORDER BY PROJECT_KEY DESC, BIW_UPD_DTTM DESC) =1)
)

--STEP 2 GET LIST OF STEPS IN EACH PROJECT, BREAK OUT BETWEEN TAPEOUT AND NON-TAPEOUT
,PROJECT_STEP_LIST_TAPEOUT AS (
    SELECT
        GATE.PROJECT_NUMBER,
        GATE.PROJECT_ID,
        GATE.STEP_TYPE,
        STEP.STEP_NAME,
        GATE.STATUS
    FROM (
        SELECT *
        FROM (
            SELECT
                PROJECT_NUMBER,
                PROJECT_ID,
                'GATE' AS STEP_TYPE
                ,TAPEOUT1_STATUS
                ,TAPEOUT2_STATUS
                ,TAPEOUT3_STATUS
                ,TAPEOUT4_STATUS
                ,TAPEOUT5_STATUS
                ,TO_VARCHAR(TAPEOUT2_DATE) TAPEOUT1_DATE
                ,TO_VARCHAR(TAPEOUT3_DATE) TAPEOUT2_DATE
                ,TO_VARCHAR(TAPEOUT4_DATE) TAPEOUT3_DATE
                ,TO_VARCHAR(TAPEOUT5_DATE) TAPEOUT4_DATE
                ,TO_VARCHAR(TAPEOUT1_DATE) TAPEOUT5_DATE
            FROM
            {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_GATE_DTL')}}
            WHERE SUBCLASS_WID = 2000008867
            AND (
                TAPEOUT1_DATE IS NOT NULL
                OR TAPEOUT2_DATE IS NOT NULL
                OR TAPEOUT3_DATE IS NOT NULL
                OR TAPEOUT4_DATE IS NOT NULL
                OR TAPEOUT5_DATE IS NOT NULL
            )
            QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER,GATE_TYPE ORDER BY BIW_UPD_DTTM DESC) =1)
        )
        UNPIVOT(STATUS FOR STEP_NAME IN (
            TAPEOUT1_STATUS,
            TAPEOUT2_STATUS,
            TAPEOUT3_STATUS,
            TAPEOUT4_STATUS,
            TAPEOUT5_STATUS
        ))
    ) GATE
    INNER JOIN STEP_REF STEP ON (
        REPLACE(GATE.STEP_NAME,'_STATUS','_DATE') = STEP.FORECAST_STEP_COLUMN_NAME
    )
)
,PROJECT_STEP_LIST AS (
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,        
        'GATE' AS STEP_TYPE,
        GATE_TYPE AS STEP_NAME,
        GATE_STATUS AS STATUS
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_GATE_DTL')}}  
    WHERE  GATE_TYPE <> 'PHASE OUT'
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER,GATE_TYPE ORDER BY BIW_UPD_DTTM DESC) =1)
    UNION ALL
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,        
        'MILESTONE' AS STEP_TYPE,
        MS_NAME AS STEP_NAME,
        STATUS 
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_MILESTONE_DTL')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER,MS_NAME ORDER BY BIW_UPD_DTTM DESC) =1)
    UNION
    SELECT
        PROJECT_NUMBER,
        PROJECT_ID,
        STEP_TYPE,
        STEP_NAME,
        STATUS
    FROM PROJECT_STEP_LIST_TAPEOUT    
)

-- PTC IS WIDE TABLE, NOT LIKE MS OR GATE WHICH IS LONG TABLE
-- FIRST WE HAVE TO CONVERT THE DATA INTO LONG 

,FACT_PPM_PTC_DTL AS (
    SELECT 
        PTC.PROJECT_NUMBER,
        PTC.PROJECT_ID,
        PTC.PTC_KEY,
        STEPS.STEP_NAME,
        PTC.NUMBER_OF_DEV_PASSES,
        PTC.PRODUCT_REVENUE_XCLUDING_NRE_$,
        PTC.TOTAL_NUMBER_OF_NEW_MASK,
        PTC.IDEAL_NUMBER_OF_NEW_MASK,
        PTC.PROJECTED_TOTAL_OTHER_EXPENSES,
        PTC.PROJECTED_TOTAL_LABOR_COST,
        (PTC.PROJECTED_TOTAL_OTHER_EXPENSES+PTC.PROJECTED_TOTAL_LABOR_COST) AS PTC_TOTAL_COST,
        PTC.PROJECT_COMPLEXITY AS PTC_PROJ_COMPLEXITY,
        PTC.PRODUCT_GM_EXCLUDING_NRE_PCT,
        PTC.PRODUCT_GM_EXCLUDING_NRE,
        PTC.TOTAL_UNITS
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','FACT_PPM_PTC_DTL')}} PTC
    LEFT JOIN PROJECT_STEP_LIST STEPS
        ON PTC.PROJECT_ID= STEPS.PROJECT_ID 
    WHERE PTC.LATEST_REV_FLAG=1
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PTC.PROJECT_NUMBER,PTC.PTC_KEY,STEPS.STEP_NAME ORDER BY BIW_UPD_DTTM DESC) =1)
)

,FACT_PPM_PTC_DTL_OS AS (
    SELECT 
        PTC.PROJECT_NUMBER,
        PTC.PROJECT_ID,
        PTC.PTC_KEY,
        STEPS.STEP_NAME,
        CASE 
            {% for step in list_all_step %}
                WHEN STEPS.STEP_NAME = '{{step[1]}}' THEN {{'PTC.'~step [2] if step[2] else 'NULL'}}
            {% endfor %}     
        END AS OS_DATE,
        CASE 
            {% for step in list_all_step %}
                WHEN STEPS.STEP_NAME = '{{step[1]}}' THEN '{{step [2] if step[2] else 'NULL'}}'
            {% endfor %}     
        END AS OS_STEP_COLUMN_NAME
        ,IFF(
            HIST_BASE.REVISION_NUMBER IS NOT NULL
                ,1
                ,IFF(HIST_BASE.REVISION_NUMBER IS NULL AND BASELINE_REV_FLAG = 1
                    ,2
                    ,IFF(HIST_BASE.REVISION_NUMBER IS NULL AND BASELINE_REV_FLAG = 0 AND LATEST_REV_FLAG = 1 AND
                          DTL.ACTUAL_END_DATE IS NULL
                             ,3
                             ,4)
                    )
        ) AS SORT_ORDER        
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','FACT_PPM_PTC_DTL')}} PTC
    LEFT JOIN PROJECT_STEP_LIST STEPS
        ON PTC.PROJECT_ID= STEPS.PROJECT_ID 
    LEFT JOIN {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_GATE_DTL')}} DTL
        ON PTC.PROJECT_ID= DTL.PROJECT_ID
        AND STEPS.STEP_NAME = DTL.GATE_TYPE        
    LEFT JOIN  {{source ('STG_PPM_ON02_NPD_MARTS','BASELINE_REV_FLAG_REFERENCE')}} HIST_BASE
        ON PTC.PROJECT_NUMBER = HIST_BASE.PROJECT_NUMBER
        AND PTC.REVISION_NUMBER = HIST_BASE.REVISION_NUMBER
    WHERE
        HIST_BASE.REVISION_NUMBER IS NOT NULL
        OR (HIST_BASE.REVISION_NUMBER IS NULL AND BASELINE_REV_FLAG = 1)
        OR (HIST_BASE.REVISION_NUMBER IS NULL AND BASELINE_REV_FLAG = 0 AND LATEST_REV_FLAG = 1 AND DTL.ACTUAL_END_DATE IS NULL)
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PTC.PROJECT_NUMBER,STEP_NAME ORDER BY SORT_ORDER) = 1 )
)

,FACT_PPM_PTC_DTL_CS AS (
    SELECT 
        PTC.PROJECT_NUMBER,
        PTC.PROJECT_ID,
        PTC.PTC_KEY,
        STEPS.STEP_NAME,
        CASE 
            {% for step in list_all_step %}
                WHEN STEPS.STEP_NAME = '{{step[1]}}' THEN  {{'PTC.'~step [3] if step[3] else 'NULL'}}
            {% endfor %}     
        END AS CS_DATE,
        CASE 
            {% for step in list_all_step %}
                WHEN STEPS.STEP_NAME = '{{step[1]}}' THEN '{{step [3] if step[3] else 'NULL'}}'
            {% endfor %}     
        END AS CS_STEP_COLUMN_NAME
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','FACT_PPM_PTC_DTL')}} PTC
    LEFT JOIN PROJECT_STEP_LIST STEPS
        ON PTC.PROJECT_ID= STEPS.PROJECT_ID 
    WHERE PTC.LATEST_REV_FLAG=1
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PTC.PROJECT_NUMBER,PTC.PTC_KEY,STEPS.STEP_NAME ORDER BY BIW_UPD_DTTM DESC) =1)
)

,DIM_PPM_GATE_DTL_FORECAST AS (
    -- Had to implement this dynamic pivot to get the forecast dates
    -- forecast dates only come from SUBCLASS_WID = 2000008867 
    SELECT
        GATE.PROJECT_NUMBER,
        GATE.PROJECT_ID,
        GATE.STEP_TYPE,
        GATE.STATUS,
        GATE.FORECAST_DATE,
        GATE.FORECAST_STEP_COLUMN_NAME,
        STEP.STEP_NAME
    FROM (
        SELECT *
        FROM (
            SELECT 
                PROJECT_NUMBER,
                PROJECT_ID,        
                'GATE' AS STEP_TYPE,
                GATE_STATUS AS STATUS
                {% for step in list_forecast_step %}
                ,TO_VARCHAR({{step[2]}}) {{step[2]}}
                {% endfor %}
            FROM 
            {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_GATE_DTL')}}
            WHERE SUBCLASS_WID = 2000008867
            QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER,GATE_TYPE ORDER BY BIW_UPD_DTTM DESC) =1)
        )
        UNPIVOT(FORECAST_DATE FOR FORECAST_STEP_COLUMN_NAME IN (
            {% for step in list_forecast_step %}
            {{step[2]}} {% if not loop.last %},{% endif %}
            {% endfor %}
        ))
    ) GATE
    INNER JOIN STEP_REF STEP ON (
        GATE.FORECAST_STEP_COLUMN_NAME = STEP.FORECAST_STEP_COLUMN_NAME
    )
)

,DIM_PPM_GATE_DTL_ACTUAL AS (
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,        
        'GATE' AS STEP_TYPE,
        GATE_TYPE AS STEP_NAME,
        GATE_STATUS AS STATUS,
        CASE 
            {% for step in list_all_step %}
                WHEN STEP_NAME = '{{step[1]}}' THEN {{step [5] if step[5] and step[0]=='GATE' else 'NULL'}}
            {% endfor %}     
        END AS ACTUAL_DATE,
        CASE 
            {% for step in list_all_step %}
                WHEN STEP_NAME = '{{step[1]}}' THEN '{{step [5] if step[5] and step[0]=='GATE' else 'NULL'}}'
            {% endfor %}     
        END AS ACTUAL_STEP_COLUMN_NAME
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_GATE_DTL')}}
    WHERE  GATE_TYPE <> 'PHASE OUT'
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER,GATE_TYPE ORDER BY BIW_UPD_DTTM DESC) =1)
)

,DIM_PPM_MILESTONE_DTL AS (
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,        
        'MILESTONE' AS STEP_TYPE,
        MS_NAME AS STEP_NAME,
        STATUS ,
        CASE 
            {% for step in list_all_step %}
                WHEN STEP_NAME = '{{step[1]}}' THEN {{step [5] if step[5] and step[0]=='MILESTONE' else 'NULL'}}
            {% endfor %}     
        END AS ACTUAL_DATE,
        CASE 
            {% for step in list_all_step %}
                WHEN STEP_NAME = '{{step[1]}}' THEN '{{step [5] if step[5] and step[0]=='MILESTONE' else 'NULL'}}'
            {% endfor %}     
        END AS ACTUAL_STEP_COLUMN_NAME
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_MILESTONE_DTL')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER,MS_NAME ORDER BY BIW_UPD_DTTM DESC) =1)
)

,FISCAL_WEEK AS 
(
    SELECT 
        DISTINCT FISCAL_WEEK_KEY
    FROM 
    {{source('MART','DATE')}}
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() )::DATE
)

,ALL_FISCAL_WEEK AS 
(
       SELECT 
        CALENDAR_DATE,
        FISCAL_WEEK_KEY
    FROM 
    {{source('MART','DATE')}}
)

,BASE_DATA AS (
    SELECT 
        MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                                ,'COL2',PRJ.PROJECT_NUMBER::STRING
                                ,'COL3',STEPS.STEP_NAME::STRING
                            )::STRING 
            )::BINARY AS PROJECT_SCHEDULE_SUMMARY_KEY,
        FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
        PRJ.PROJECT_NUMBER,
        PRJ.PROJECT_ID,
        
        STEPS.STEP_TYPE,
        STEPS.STEP_NAME,
        STP_REF.SORT_ORDER,
        STEPS.STATUS,
        
        STP_REF.OS_STEP_COLUMN_NAME,
        PTC_OS.OS_DATE,
        PTC_OS_WK.FISCAL_WEEK_KEY AS  OS_DATE_WEEK,
        
        STP_REF.CS_STEP_COLUMN_NAME,
        PTC_CS.CS_DATE,
        PTC_CS_WK.FISCAL_WEEK_KEY AS  CS_DATE_WEEK,
        
        STP_REF.FORECAST_STEP_COLUMN_NAME,
        TO_DATE(FORECAST_GATE.FORECAST_DATE) AS FORECAST_DATE,
        GATE_FCST_WK.FISCAL_WEEK_KEY AS  FORECAST_DATE_WEEK,

        STP_REF.ACTUAL_STEP_COLUMN_NAME,
        TO_DATE(COALESCE(ACTUAL_GATE.ACTUAL_DATE,MILE.ACTUAL_DATE)) AS ACTUAL_DATE,
        COALESCE(GATE_ACT_WK.FISCAL_WEEK_KEY,MILE_ACT_WK.FISCAL_WEEK_KEY) AS ACTUAL_DATE_WEEK,

        -- Create NEXT_MILESTONE_DATE
        COALESCE(TO_DATE(COALESCE(ACTUAL_GATE.ACTUAL_DATE,MILE.ACTUAL_DATE)),PTC_CS.CS_DATE) NEXT_MILESTONE_DATE,

        PTC.NUMBER_OF_DEV_PASSES,
        PTC.PRODUCT_REVENUE_XCLUDING_NRE_$,
        PTC.TOTAL_NUMBER_OF_NEW_MASK,
        PTC.IDEAL_NUMBER_OF_NEW_MASK,
        PTC.PROJECTED_TOTAL_OTHER_EXPENSES,
        PTC.PROJECTED_TOTAL_LABOR_COST,
        PTC.PTC_TOTAL_COST,
        PTC.PTC_PROJ_COMPLEXITY,
        PTC.PRODUCT_GM_EXCLUDING_NRE_PCT,
        PTC.PRODUCT_GM_EXCLUDING_NRE,
        PTC.TOTAL_UNITS    
    FROM     
    -- STEP 1 GET ALL PROJECTS
    DIM_PPM_PROJECT_DTL  PRJ 
    -- STEP 2 FAN OUT PROJECT WITH LIST OF STEPS AVAILABLE
    LEFT JOIN PROJECT_STEP_LIST STEPS
        ON PRJ.PROJECT_ID = STEPS.PROJECT_ID

    -- LIST OF COLUMNS REFERENCE USED 
    LEFT JOIN STEP_REF STP_REF
        ON STP_REF.STEP_NAME= STEPS.STEP_NAME

    -- STEP 3 GET MEASURES FROM PTC
    LEFT JOIN FACT_PPM_PTC_DTL PTC
        ON STEPS.PROJECT_ID = PTC.PROJECT_ID
        AND STEPS.STEP_NAME = PTC.STEP_NAME
    -- STEP 4 GET OS DATE FROM PTC
    LEFT JOIN FACT_PPM_PTC_DTL_OS PTC_OS
        ON STEPS.PROJECT_ID = PTC_OS.PROJECT_ID
        AND STEPS.STEP_NAME = PTC_OS.STEP_NAME
    -- STEP 5 GET CS DATE FROM PTC 
    LEFT JOIN FACT_PPM_PTC_DTL_CS PTC_CS
        ON STEPS.PROJECT_ID = PTC_CS.PROJECT_ID
        AND STEPS.STEP_NAME = PTC_CS.STEP_NAME
    -- STEP 6 GET THE FORECAST AND ACTUAL DATE
    LEFT JOIN DIM_PPM_GATE_DTL_FORECAST FORECAST_GATE
        ON STEPS.PROJECT_ID = FORECAST_GATE.PROJECT_ID
        AND STEPS.STEP_NAME = FORECAST_GATE.STEP_NAME
    LEFT JOIN DIM_PPM_GATE_DTL_ACTUAL ACTUAL_GATE
        ON STEPS.PROJECT_ID = ACTUAL_GATE.PROJECT_ID
        AND STEPS.STEP_NAME = ACTUAL_GATE.STEP_NAME
    LEFT JOIN DIM_PPM_MILESTONE_DTL MILE
    ON STEPS.PROJECT_ID = MILE.PROJECT_ID
        AND STEPS.STEP_NAME = MILE.STEP_NAME

    -- FISCAL WEEK NUMBER FOR DATES 
    LEFT JOIN ALL_FISCAL_WEEK PTC_OS_WK
        ON PTC_OS_WK.CALENDAR_DATE = PTC_OS.OS_DATE
    LEFT JOIN ALL_FISCAL_WEEK PTC_CS_WK
        ON PTC_CS_WK.CALENDAR_DATE = PTC_CS.CS_DATE
    LEFT JOIN ALL_FISCAL_WEEK GATE_FCST_WK
        ON GATE_FCST_WK.CALENDAR_DATE = FORECAST_GATE.FORECAST_DATE
    LEFT JOIN ALL_FISCAL_WEEK GATE_ACT_WK
        ON GATE_ACT_WK.CALENDAR_DATE = ACTUAL_GATE.ACTUAL_DATE
    LEFT JOIN ALL_FISCAL_WEEK MILE_ACT_WK
    ON MILE_ACT_WK.CALENDAR_DATE = MILE.ACTUAL_DATE

    --FANNING OUT THE DATA ON SATURADAY FOR 7 HOUR WINDOW
    CROSS JOIN FISCAL_WEEK FSC_WK
)
,NON_TAPEOUT AS (
    SELECT *
    FROM BASE_DATA
    WHERE STEP_NAME NOT ILIKE 'TAPEOUT%'
    ORDER BY SORT_ORDER
)
,TAPEOUT AS (
    SELECT
        t2.PROJECT_SCHEDULE_SUMMARY_KEY,
        t2.SNAPSHOT_WEEK_KEY,
        t2.PROJECT_NUMBER,
        t2.PROJECT_ID,
        t2.STEP_TYPE,
        t2.STEP_NAME,
        (
            SELECT MAX(t1.SORT_ORDER)
            FROM NON_TAPEOUT t1
            WHERE t1.PROJECT_ID = t2.PROJECT_ID
            AND t1.SNAPSHOT_WEEK_KEY = t2.SNAPSHOT_WEEK_KEY
            AND t1.CS_DATE <= t2.CS_DATE 
        ) AS SORT_ORDER,
        t2.STATUS,
        t2.OS_STEP_COLUMN_NAME,
        t2.OS_DATE,
        t2.OS_DATE_WEEK,
        t2.CS_STEP_COLUMN_NAME,
        t2.CS_DATE,
        t2.CS_DATE_WEEK,
        t2.FORECAST_STEP_COLUMN_NAME,
        t2.FORECAST_DATE,
        t2.FORECAST_DATE_WEEK,
        t2.ACTUAL_STEP_COLUMN_NAME,
        t2.ACTUAL_DATE,
        t2.ACTUAL_DATE_WEEK,
        t2.NEXT_MILESTONE_DATE,
        t2.NUMBER_OF_DEV_PASSES,
        t2.PRODUCT_REVENUE_XCLUDING_NRE_$,
        t2.TOTAL_NUMBER_OF_NEW_MASK,
        t2.IDEAL_NUMBER_OF_NEW_MASK,
        t2.PROJECTED_TOTAL_OTHER_EXPENSES,
        t2.PROJECTED_TOTAL_LABOR_COST,
        t2.PTC_TOTAL_COST,
        t2.PTC_PROJ_COMPLEXITY,
        t2.PRODUCT_GM_EXCLUDING_NRE_PCT,
        t2.PRODUCT_GM_EXCLUDING_NRE,
        t2.TOTAL_UNITS
    FROM BASE_DATA t2
    WHERE t2.STEP_NAME ILIKE 'TAPEOUT%' 
)
,SORTED_SCHEDULE_SUMMARY AS (
    SELECT
        PROJECT_SCHEDULE_SUMMARY_KEY,
        SNAPSHOT_WEEK_KEY,
        PROJECT_NUMBER,
        PROJECT_ID,
        STEP_TYPE,
        STEP_NAME,
        STATUS,
        OS_STEP_COLUMN_NAME,
        OS_DATE,
        OS_DATE_WEEK,
        CS_STEP_COLUMN_NAME,
        CS_DATE,
        CS_DATE_WEEK,
        FORECAST_STEP_COLUMN_NAME,
        FORECAST_DATE,
        FORECAST_DATE_WEEK,
        ACTUAL_STEP_COLUMN_NAME,
        ACTUAL_DATE,
        ACTUAL_DATE_WEEK,
        NEXT_MILESTONE_DATE,
        NUMBER_OF_DEV_PASSES,
        PRODUCT_REVENUE_XCLUDING_NRE_$,
        TOTAL_NUMBER_OF_NEW_MASK,
        IDEAL_NUMBER_OF_NEW_MASK,
        PROJECTED_TOTAL_OTHER_EXPENSES,
        PROJECTED_TOTAL_LABOR_COST,
        PTC_TOTAL_COST,
        PTC_PROJ_COMPLEXITY,
        PRODUCT_GM_EXCLUDING_NRE_PCT,
        PRODUCT_GM_EXCLUDING_NRE,
        TOTAL_UNITS,
        ROW_NUMBER() OVER (PARTITION BY SNAPSHOT_WEEK_KEY, PROJECT_ID ORDER BY SORT_ORDER, CS_DATE) AS SORT_ORDER
    FROM (
        SELECT *
        FROM NON_TAPEOUT
        UNION
        SELECT *
        FROM TAPEOUT
    )
)
,NEXT_MILESTONE_SUMMARY AS (
    SELECT
        SNAPSHOT_WEEK_KEY,
        PROJECT_ID,
        STEP_NAME AS NEXT_MILESTONE
    FROM SORTED_SCHEDULE_SUMMARY
    WHERE STATUS IN ('Closed', 'In Review')
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY SNAPSHOT_WEEK_KEY, PROJECT_ID ORDER BY NEXT_MILESTONE_DATE) = 1)
)
SELECT 
        PROJECT_SCHEDULE_SUMMARY_KEY,
        SS.SNAPSHOT_WEEK_KEY,
        PROJECT_NUMBER,
        SS.PROJECT_ID,
        STEP_TYPE,
        STEP_NAME,
        SORT_ORDER,
        STATUS,
        OS_STEP_COLUMN_NAME,
        OS_DATE,
        OS_DATE_WEEK,
        CS_STEP_COLUMN_NAME,
        CS_DATE,
        CS_DATE_WEEK,
        FORECAST_STEP_COLUMN_NAME,
        FORECAST_DATE,
        FORECAST_DATE_WEEK,
        ACTUAL_STEP_COLUMN_NAME,
        ACTUAL_DATE,
        ACTUAL_DATE_WEEK,
        NUMBER_OF_DEV_PASSES,
        PRODUCT_REVENUE_XCLUDING_NRE_$,
        TOTAL_NUMBER_OF_NEW_MASK,
        IDEAL_NUMBER_OF_NEW_MASK,
        PROJECTED_TOTAL_OTHER_EXPENSES,
        PROJECTED_TOTAL_LABOR_COST,
        PTC_TOTAL_COST,
        PTC_PROJ_COMPLEXITY,
        PRODUCT_GM_EXCLUDING_NRE_PCT,
        PRODUCT_GM_EXCLUDING_NRE,
        TOTAL_UNITS,
        NEXT_MILESTONE,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
        {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
        md5(object_construct (
        'col1',SS.SNAPSHOT_WEEK_KEY::string, 'col2',PROJECT_NUMBER::string, 'col3',SS.PROJECT_ID::string,
        'col4',STEP_TYPE::string, 'col5',STEP_NAME::string, 'col6',SORT_ORDER::string, 'col7',STATUS::string,
        'col8',OS_STEP_COLUMN_NAME::string, 'col9',OS_DATE::string,'col10',OS_DATE_WEEK::string,
        'col11',CS_STEP_COLUMN_NAME::string, 'col12',CS_DATE::string, 'col13',CS_DATE_WEEK::string,
        'col14',FORECAST_STEP_COLUMN_NAME::string, 'col15',FORECAST_DATE::string, 'col16',FORECAST_DATE_WEEK::string,
        'col17',ACTUAL_STEP_COLUMN_NAME::string, 'col18',ACTUAL_DATE::string,'col19',ACTUAL_DATE_WEEK::string,
        'col20',NUMBER_OF_DEV_PASSES::string, 'col21',PRODUCT_REVENUE_XCLUDING_NRE_$::string, 'col22',TOTAL_NUMBER_OF_NEW_MASK::string,
        'col23',IDEAL_NUMBER_OF_NEW_MASK::string, 'col24',PROJECTED_TOTAL_OTHER_EXPENSES::string, 'col25',PROJECTED_TOTAL_LABOR_COST::string,
        'col26',PTC_TOTAL_COST::string, 'col27',PTC_PROJ_COMPLEXITY::string, 'col28',PRODUCT_GM_EXCLUDING_NRE_PCT::string,
        'col29',PRODUCT_GM_EXCLUDING_NRE::string, 'col30',TOTAL_UNITS::string, 'col31',NEXT_MILESTONE::string)::string )::BINARY as BIW_MD5_KEY 
FROM SORTED_SCHEDULE_SUMMARY SS
LEFT JOIN NEXT_MILESTONE_SUMMARY MS ON (
    SS.SNAPSHOT_WEEK_KEY = MS.SNAPSHOT_WEEK_KEY
    AND SS.PROJECT_ID = MS.PROJECT_ID
)