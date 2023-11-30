/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_SALES_PRODUCT_LEADTIME_BUCKET_BRIDGE
-- dbt run --full-refresh --select MART_SALES_PRODUCT_LEADTIME_BUCKET_BRIDGE

Version     Date            Author              Description
-------     --------        -----------------   ----------------------------------
1.0         25 NOV 2022      VINAY SUBRAMANIAN  Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_LEADTIME_BUCKET_BRIDGE_KEY']-%}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_LEADTIME_BUCKET_BRIDGE'-%}
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
         description = 'Building table PRODUCT_LEADTIME_BUCKET_BRIDGE for sales mart '
        ,transient=false
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_LEADTIME_BUCKET_BRIDGE'
        ,tags ='MART_SALES'
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH PRODUCT_LEADTIME AS 
(
    SELECT 
        DISTINCT MASTER_PLANNING_RUN_DATE
    FROM {{ref('MART_SALES_PRODUCT_LEADTIME_FACT')}}
)
SELECT
    MD5(MASTER_PLANNING_RUN_DATE::STRING) AS PRODUCT_LEADTIME_BUCKET_BRIDGE_KEY,
    REPLACE(SUBSTRING(MASTER_PLANNING_RUN_DATE,1,10),'-')::NUMBER AS MASTER_PLANNING_RUN_DATE_KEY,
    MASTER_PLANNING_RUN_DATE,
    (row_number() OVER (PARTITION BY 1 ORDER BY MASTER_PLANNING_RUN_DATE DESC)-1)*-1 AS BUCKET_SEQUENCE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID
FROM PRODUCT_LEADTIME