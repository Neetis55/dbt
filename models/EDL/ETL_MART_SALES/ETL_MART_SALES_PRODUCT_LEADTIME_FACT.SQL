/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_PRODUCT_LEADTIME_FACT
dbt build --full-refresh --select ETL_MART_SALES_PRODUCT_LEADTIME_FACT+ --vars 'is_backfill: True'
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25 NOV 2022      VINAY SUBRAMANIAN  Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_LEADTIME_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT_LEADTIME_FACT'-%}
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
      description = 'Building PRODUCT LEADTIME FACT FOR SALES MART', 
        transient=true,
        materialized = 'table', 
        schema = 'ETL_MART_SALES', 
        tags = ['MART_SALES'],
        alias = 'PRODUCT_LEADTIME_FACT',
        post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    MD5(OBJECT_CONSTRUCT ('COL1',PART_ID::STRING,
                        'COL2',MP_RUN_DT::STRING)::STRING) AS PRODUCT_LEADTIME_KEY,
    MD5(PART_ID) AS INTERNAL_PART_NUMBER_KEY, 
    ARCHIVE_YYYYWK AS FISCAL_WEEK_KEY,
    PART_ID AS INTERNAL_PART_NUMBER,
    MP_RUN_DT AS MASTER_PLANNING_RUN_DATE,   
    BACKLOG_QTY AS BACKLOG_QUANTITY,
    BUCKET_SEQ AS BUCKET_SEQUENCE,
    CAPC_CONSTRAINT_COLOR AS CAPACITY_CONSTRAINT_COLOR,
    CT_PLUS_WKS_LT AS CYCLETIME_PLUS_WEEKS_LEADTIME,
    DCE_CD,
    DCE_CT AS DCE_CYCLETIME,
    FAMILY_LT AS FAMILY_LEADTIME,
    FLOOR_OVERRIDE_DTTM AS FLOOR_OVERRIDE_DATE,
    FLOOR_OVERRIDE_LT AS FLOOR_OVERRIDE_LEADTIME,
    FLOOR_OVERRIDE_USERID,
    REPLACE(LT_OVERRIDE_LVL,' ') AS LEADTIME_OVERRIDE_LEVEL,
    OPT_01_LT AS OPT_01_LEADTIME,
    OPT_01_S_LT AS OPT_01_SIMULATED_LEADTIME,
    OPT_02_LT AS OPT_02_LEADTIME,
    OPT_02_S_LT AS OPT_02_SIMULATED_LEADTIME,
    OPT_03_LT AS OPT_03_LEADTIME,
    OPT_03_S_LT AS OPT_03_SIMULATED_LEADTIME,
    OPT_04_LT AS OPT_04_LEADTIME,
    OPT_04_S_LT AS OPT_04_SIMULATED_LEADTIME,
    OPT_05_LT AS OPT_05_LEADTIME,
    OPT_05_S_LT AS OPT_05_SIMULATED_LEADTIME,
    OPT_06_LT AS OPT_06_LEADTIME,
    OPT_06_S_LT AS OPT_06_SIMULATED_LEADTIME,
    OPT_07_LT AS OPT_07_LEADTIME,
    OPT_07_S_LT AS OPT_07_SIMULATED_LEADTIME,
    OPT_08_LT AS OPT_08_LEADTIME,
    OPT_08_S_LT AS OPT_08_SIMULATED_LEADTIME,
    OVERRIDE_RSN_CD AS OVERRIDE_REASON_CODE,
    OVERRIDE_RSN_DESC AS OVERRIDE_REASON_DESCRIPTION,
    OVERRIDE_LT AS OVRRIDE_LEADTIME,
    PARTSTATUS AS PART_STATUS_CODE,
    PERCENTAGE_LT AS PERCENTAGE_LEADTIME,
    PACKAGE_GROUP AS PACKAGE_GROUP,
    POINT_LT AS POINT_LEADTIME,
    PREV_TBOT_LT AS PREVIOUS_TRANSACTION_BASED_ORDER_TRACKING_LEADTIME,
    REG_OVERRIDE_DT AS REGULAR_OVERRIDE_DATE,
    REG_OVERRIDE_REQUESTING_USERID AS REGULAR_OVERRIDE_REQUESTING_USERID,
    REG_OVERRIDE_TO_DT AS REGULAR_OVERRIDE_TO_DATE,
    REG_OVERRIDE_USERID AS REGULAR_OVERRIDE_USERID,
    TBOT_LT AS TRANSACTION_BASED_ORDER_TRACKING_LEADTIME,
    TOP_PERFORMING_MEASURE_NAME AS TOP_PERFORMING_MEASURE_NAME,
    TOP_PERFORMING_MEASURE_VAL AS TOP_PERFORMING_MEASURE_VALUE,
    TOP_PERFORMING_MEAS_VAL_REF AS TOP_PERFORMING_MEASURE_VALUE_REFINED,
    TREND_LT AS TREND_LEADTIME,
    UPDATE_DT AS SOURCE_UPDATE_DATE,
    WT_GAP_LT AS WEIGHTED_GAP_LEADTIME,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
    md5(object_construct ('col1',INTERNAL_PART_NUMBER::string,
    'col2',MASTER_PLANNING_RUN_DATE::string, 'col3',INTERNAL_PART_NUMBER_KEY::string, 'col4',FISCAL_WEEK_KEY::string,
    'col5',BACKLOG_QUANTITY::string, 'col6',BUCKET_SEQUENCE::string, 'col7',CAPACITY_CONSTRAINT_COLOR::string,
    'col8',CYCLETIME_PLUS_WEEKS_LEADTIME::string, 'col9',DCE_CD::string, 'col10',DCE_CYCLETIME::string, 'col11',FAMILY_LEADTIME::string,
    'col12',FLOOR_OVERRIDE_DATE::string, 'col13',FLOOR_OVERRIDE_LEADTIME::string, 'col14',FLOOR_OVERRIDE_USERID::string,
    'col15',LEADTIME_OVERRIDE_LEVEL::string, 'col16',OPT_01_LEADTIME::string, 'col17',OPT_01_SIMULATED_LEADTIME::string,
    'col18',OPT_02_LEADTIME::string, 'col19',OPT_02_SIMULATED_LEADTIME::string, 'col20',OPT_03_LEADTIME::string,
    'col21',OPT_03_SIMULATED_LEADTIME::string, 'col22',OPT_04_LEADTIME::string, 'col23',OPT_04_SIMULATED_LEADTIME::string,
    'col24',OPT_05_LEADTIME::string, 'col25',OPT_05_SIMULATED_LEADTIME::string, 'col26',OPT_06_LEADTIME::string,
    'col27',OPT_06_SIMULATED_LEADTIME::string, 'col28',OPT_07_LEADTIME::string, 'col29',OPT_07_SIMULATED_LEADTIME::string,
    'col30',OPT_08_LEADTIME::string, 'col31',OPT_08_SIMULATED_LEADTIME::string, 'col32',OVERRIDE_REASON_CODE::string,
    'col33',OVERRIDE_REASON_DESCRIPTION::string, 'col34',OVRRIDE_LEADTIME::string, 'col35',PART_STATUS_CODE::string,
    'col36',PERCENTAGE_LEADTIME::string, 'col37',PACKAGE_GROUP::string, 'col38',POINT_LEADTIME::string,
    'col39',PREVIOUS_TRANSACTION_BASED_ORDER_TRACKING_LEADTIME::string, 'col40',REGULAR_OVERRIDE_DATE::string, 'col41',REGULAR_OVERRIDE_REQUESTING_USERID::string,
    'col42',REGULAR_OVERRIDE_TO_DATE::string, 'col43',REGULAR_OVERRIDE_USERID::string,
    'col44',TRANSACTION_BASED_ORDER_TRACKING_LEADTIME::string, 'col45',TOP_PERFORMING_MEASURE_NAME::string, 'col46',TOP_PERFORMING_MEASURE_VALUE::string,
    'col47',TOP_PERFORMING_MEASURE_VALUE_REFINED::string, 'col48',TREND_LEADTIME::string, 'col49',SOURCE_UPDATE_DATE::string,
    'col50',WEIGHTED_GAP_LEADTIME::string, 'col51',BIW_INS_DTTM::string, 'col52',BIW_UPD_DTTM::string, 'col53',BIW_BATCH_ID::string)::string
    )::BINARY as BIW_MD5_KEY
FROM    
 {{ source('STG_PLAN_APS_OWNER', 'LT_HISTORY_DETAIL_ALL') }}
  WHERE 
    {% if var('is_backfill') %}
        BIW_UPD_DTTM >= '{{var('refresh_start_ts')}}'
		AND BIW_UPD_DTTM < '{{V_START_DTTM}}'
    {% else %}
        BIW_UPD_DTTM >= '{{V_LWM}}' 
		AND BIW_UPD_DTTM <= '{{V_HWM}}'
    {% endif %}
 QUALIFY( row_number() OVER (PARTITION BY PART_ID,MP_RUN_DT ORDER BY BIW_UPD_DTTM DESC)=1)

