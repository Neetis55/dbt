
/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select ETL_MART_TIME

Version     Date            Author              Description
-------     --------        -----------        ----------------------------------
1.0         24 JAN 2023     Kali Dandapani     Initial Version: of time dimension
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['time_key']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_TIME'-%}
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
         description = 'Building table TIME dimension'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART'
        ,alias='TIME'
		,unique_key= v_pk_list
        ,tags =['MART']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH
    ROW_GENERATOR AS (
        SELECT
            SEQ4() AS SEQ_GEN,
            ROW_NUMBER()OVER(PARTITION BY 1 ORDER BY SEQ_GEN) AS COUNTER,
            TO_TIME('00:00:00', 'HH24:MI:SS') AS START_TIME,
            DATEADD(MINUTE, COUNTER, START_TIME) AS RUNNING_TS,
            TO_VARCHAR(RUNNING_TS, 'HH12:MI:SS AM') AS RUNNING_TS_12
        FROM TABLE(GENERATOR(ROWCOUNT => 1440)) V
    )
SELECT
    -- START_TIME, COUNTER,RUNNING_TS
    REPLACE(RUNNING_TS, ':')::NUMBER AS TIME_KEY,
    RUNNING_TS AS TIME_OF_DAY,
    HOUR(RUNNING_TS) AS HOUR24,
    RUNNING_TS::STRING AS HOUR24_FULL_DESCRIPTION,
    SUBSTRING(RUNNING_TS, 1, 5) AS HOUR24_MINUTE_DESCRIPTION,
    SUBSTRING(RUNNING_TS, 1, 2) AS HOUR24_SHORT_DESCRIPTION,

    SUBSTRING(RUNNING_TS_12, 1, 2)::NUMBER AS HOUR12,
    RUNNING_TS_12 AS HOUR12_FULL_DESCRIPTION,
    SUBSTRING(RUNNING_TS_12, 1, 5) HOUR12_MINUTE_DESCRIPTION,
    REPLACE(SUBSTRING(RUNNING_TS_12, 1, 5), ':')::NUMBER AS HOUR12_MINUTE_CODE,
    SUBSTRING(RUNNING_TS_12, 1, 2) AS HOUR12_SHORT_DESCRIPTION,
    CASE WHEN HOUR24 >= 12 THEN TRUE ELSE FALSE END IS_PM,
    SUBSTRING(RUNNING_TS_12, 10, 2) AS AM_PM_DESCRIPTION,
    CASE
        WHEN HOUR24 < 5
        THEN 'Early Morning'
        WHEN HOUR24 < 12
        THEN 'Morning'
        WHEN HOUR24 < 18
        THEN 'Afternoon'
        ELSE 'Evening'
    END AS DAY_TIME_DESCRIPTION,
    CASE
        WHEN HOUR24 BETWEEN 6 AND 17 
		THEN 'Day' 
		ELSE 'Night'
    END AS DAY_NIGHT_DESCRIPTION,
    SUBSTRING(RUNNING_TS, 4, 2)::NUMBER MINUTE,
    ROW_NUMBER()OVER(PARTITION BY 1 ORDER BY TIME_KEY) AS MINUTE_OF_DAY,
    HOUR24_FULL_DESCRIPTION AS MINUTE_FULL_DESCRIPTION_24,
    SUBSTRING(RUNNING_TS_12, 1, 8) AS MINUTE_FULL_DESCRIPTION_12,
    SUBSTRING(RUNNING_TS, 4, 2) AS MINUTE_SHORT_DESCRIPTION,
    0 AS SECOND,
    '00' AS SECOND_SHORT_DESCRIPTION
FROM ROW_GENERATOR
ORDER BY MINUTE_OF_DAY
