/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_SALES_PRODUCT_LEADTIME_FACT
-- dbt run --full-refresh --select MART_SALES_PRODUCT_LEADTIME_FACT

Version     Date            Author              Description
-------     --------        -----------------   ----------------------------------
1.0         25 NOV 2022      VINAY SUBRAMANIAN  Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_LEADTIME_KEY']-%}

{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_PRODUCT_LEADTIME_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_PRODUCT_LEADTIME_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_PRODUCT_LEADTIME_FACT'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{% endif %}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_LEADTIME_FACT'-%}
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
         description = 'Building table PRODUCT_LEADTIME_FACT for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_LEADTIME_FACT'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,merge_update_columns= ['INTERNAL_PART_NUMBER_KEY', 'FISCAL_WEEK_KEY', 'INTERNAL_PART_NUMBER',
                                'MASTER_PLANNING_RUN_DATE', 'BACKLOG_QUANTITY', 'CAPACITY_CONSTRAINT_COLOR',
                                'CYCLETIME_PLUS_WEEKS_LEADTIME', 'DCE_CD', 'DCE_CYCLETIME', 'FAMILY_LEADTIME', 'FLOOR_OVERRIDE_DATE', 'FLOOR_OVERRIDE_LEADTIME',
                                'FLOOR_OVERRIDE_USERID', 'LEADTIME_OVERRIDE_LEVEL', 'OPT_01_LEADTIME', 'OPT_01_SIMULATED_LEADTIME', 'OPT_02_LEADTIME',
                                'OPT_02_SIMULATED_LEADTIME', 'OPT_03_LEADTIME', 'OPT_03_SIMULATED_LEADTIME', 'OPT_04_LEADTIME', 'OPT_04_SIMULATED_LEADTIME',
                                'OPT_05_LEADTIME', 'OPT_05_SIMULATED_LEADTIME', 'OPT_06_LEADTIME', 'OPT_06_SIMULATED_LEADTIME', 'OPT_07_LEADTIME',
                                'OPT_07_SIMULATED_LEADTIME', 'OPT_08_LEADTIME', 'OPT_08_SIMULATED_LEADTIME', 'OVERRIDE_REASON_CODE',
                                'OVERRIDE_REASON_DESCRIPTION', 'OVRRIDE_LEADTIME', 'PART_STATUS_CODE', 'PERCENTAGE_LEADTIME', 'PACKAGE_GROUP', 'POINT_LEADTIME',
                                'PREVIOUS_TRANSACTION_BASED_ORDER_TRACKING_LEADTIME', 'REGULAR_OVERRIDE_DATE', 'REGULAR_OVERRIDE_REQUESTING_USERID', 'REGULAR_OVERRIDE_TO_DATE',
                                'REGULAR_OVERRIDE_USERID', 'TRANSACTION_BASED_ORDER_TRACKING_LEADTIME', 'TOP_PERFORMING_MEASURE_NAME',
                                'TOP_PERFORMING_MEASURE_VALUE', 'TOP_PERFORMING_MEASURE_VALUE_REFINED', 'TREND_LEADTIME', 'SOURCE_UPDATE_DATE',
                                'WEIGHTED_GAP_LEADTIME', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT
    PRODUCT_LEADTIME_KEY,
    INTERNAL_PART_NUMBER_KEY,
    FISCAL_WEEK_KEY,
    INTERNAL_PART_NUMBER,
    MASTER_PLANNING_RUN_DATE,
    BACKLOG_QUANTITY,
    CAPACITY_CONSTRAINT_COLOR,
    CYCLETIME_PLUS_WEEKS_LEADTIME,
    DCE_CD,
    DCE_CYCLETIME,
    FAMILY_LEADTIME,
    FLOOR_OVERRIDE_DATE,
    FLOOR_OVERRIDE_LEADTIME,
    FLOOR_OVERRIDE_USERID,
    LEADTIME_OVERRIDE_LEVEL,
    OPT_01_LEADTIME,
    OPT_01_SIMULATED_LEADTIME,
    OPT_02_LEADTIME,
    OPT_02_SIMULATED_LEADTIME,
    OPT_03_LEADTIME,
    OPT_03_SIMULATED_LEADTIME,
    OPT_04_LEADTIME,
    OPT_04_SIMULATED_LEADTIME,
    OPT_05_LEADTIME,
    OPT_05_SIMULATED_LEADTIME,
    OPT_06_LEADTIME,
    OPT_06_SIMULATED_LEADTIME,
    OPT_07_LEADTIME,
    OPT_07_SIMULATED_LEADTIME,
    OPT_08_LEADTIME,
    OPT_08_SIMULATED_LEADTIME,
    OVERRIDE_REASON_CODE,
    OVERRIDE_REASON_DESCRIPTION,
    OVRRIDE_LEADTIME,
    PART_STATUS_CODE,
    PERCENTAGE_LEADTIME,
    PACKAGE_GROUP,
    POINT_LEADTIME,
    PREVIOUS_TRANSACTION_BASED_ORDER_TRACKING_LEADTIME,
    REGULAR_OVERRIDE_DATE,
    REGULAR_OVERRIDE_REQUESTING_USERID,
    REGULAR_OVERRIDE_TO_DATE,
    REGULAR_OVERRIDE_USERID,
    TRANSACTION_BASED_ORDER_TRACKING_LEADTIME,
    TOP_PERFORMING_MEASURE_NAME,
    TOP_PERFORMING_MEASURE_VALUE,
    TOP_PERFORMING_MEASURE_VALUE_REFINED,
    TREND_LEADTIME,
    SOURCE_UPDATE_DATE,
    WEIGHTED_GAP_LEADTIME,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
    BIW_MD5_KEY
FROM {{ref('ETL_MART_SALES_PRODUCT_LEADTIME_FACT')}}