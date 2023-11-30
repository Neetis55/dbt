/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_SALES_PRODUCT_PRICE_WORKSHEET
-- dbt build --full-refresh --select +MART_SALES_PRODUCT_PRICE_WORKSHEET+  --vars 'is_backfill: True'

Version     Date            Author           Description
-------     --------        -----------      ----------------------------------
1.0         09-NOV-2022      VINAYAKA CN    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['GPS_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{% if is_incremental() %}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_PRICE_WORKSHEET'-%}
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
         description = 'Building table PRODUCT_PRICE_WORKSHEET for MART_SALES'
        ,transient=false
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_PRICE_WORKSHEET'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
		,post_hook= [v_sql_upd_success_batch]
        )
}}

SELECT 
    GPS_KEY ,
    PRODUCT_ID ,
    STATUS ,
    IS_DELETE ,
    EFFECTIVE_FROM_DATE ,
    EFFECTIVE_TO_DATE ,
    PRODUCT_KEY ,
    DISTRIBUTOR_BOOK_PRICE_USD ,
    DISTRIBUTOR_BOOK_PRICE_EUR ,
    DISTRIBUTOR_BOOK_PRICE_JPY ,
    END_POINT_PRICE_USD ,
    FIELD_MINIMUM_QUANTITY ,
    MINIMUM_PACKAGE_QUANTITY ,
    MULTIPLIER ,
    REFERENCE_PRICE ,
    STRATEGIC_MINIMUM_PRICE ,
    WALK_AWAY_PRICE ,
    FRAME_NAME ,
    GPS_PRICE_CATEGORY ,
    GPS_PROGRAM_DESCRIPTION ,
    GPS_CATEGORY ,
    GPS_CATEGORY_DESCRIPTION ,
    GPS_MARKET ,
    GPS_MARKET_DESCRIPTION ,
    GPS_PROGRAM_CODE ,
    GPS_PROGRAM_CODE_DESCRIPTION ,
    MARGIN_LEVEL ,
    PRODUCT_FRAME_TYPE ,
    PRODUCT_FRAME_TYPE_DESCRIPTION ,
    IS_NCNR ,
    IS_PRICE_BOOK ,
    IS_QUOTE ,
    IS_STEP_PRICING ,
    STAR_LEVEL ,
    IS_FIRST_TIME_IN_PRICE_BOOK ,
    ON_TARGET_REMOVAL_DATE ,
    ON_TARGET_START_DATE ,
    PHASE_OUT_END_DATE ,
    PHASE_OUT_START_DATE ,
    REFERENCE_QUANTITY ,
    MINIMUM_ORDER_QUANTITY ,
    RESALE_PRICE ,
    MINIMUM_DOLLAR_VALUE ,
    COGS_PRICE ,
    SOURCE_SYSTEM, 
    BIW_INS_DTTM ,
    BIW_UPD_DTTM ,
    BIW_BATCH_ID
FROM 
    {{ref('MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST')}}  
    WHERE STATUS='Current'