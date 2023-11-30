/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST
-- dbt build --full-refresh --select +MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST+  --vars 'is_backfill: True'

Version     Date            Author           Description
-------     --------        -----------      ----------------------------------
1.0         09-NOV-2022      KALI D    Initial Version
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
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{%- set V_TARGET_TABLE_NAME = env_var('DBT_EDW_DB') ~ env_var('DBT_DEP_ENV') ~'.MART_SALES.PRODUCT_PRICE_WORKSHEET_HIST' -%}
{%- set V_STAGE_TABLE_NAME = env_var('DBT_EDL_DB') ~ env_var('DBT_DEP_ENV') ~'.ETL_MART_SALES.PRODUCT_PRICE_WORKSHEET_HIST' -%}
{%- set V_POST_UPDATE -%}
 UPDATE {{V_TARGET_TABLE_NAME}} T1
    SET STATUS ='Expired',
        EFFECTIVE_TO_DATE = T2.EFFECTIVE_FROM_DATE - INTERVAL '1 DAY',
        BIW_UPD_DTTM =  T2.BIW_UPD_DTTM,
        BIW_BATCH_ID = T2.BIW_BATCH_ID
        FROM (SELECT 
                PRODUCT_ID, 
                EFFECTIVE_FROM_DATE,
                BIW_UPD_DTTM,
                BIW_BATCH_ID
            FROM {{V_STAGE_TABLE_NAME}}
            WHERE EFFECTIVE_TO_DATE='9999-12-31' 
            ) T2
            WHERE 
            T1.EFFECTIVE_TO_DATE = '9999-12-31' 
            AND T1.BIW_UPD_DTTM <> (SELECT MAX(BIW_UPD_DTTM) FROM {{V_TARGET_TABLE_NAME}})
            AND T1.PRODUCT_ID = T2.PRODUCT_ID
{% endset %}

{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building table PRODUCT_PRICE_WORKSHEET_HIST for MART_SALES'
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_PRICE_WORKSHEET_HIST'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
        ,merge_update_columns =  ['PRODUCT_ID', 'EFFECTIVE_FROM_DATE', 'EFFECTIVE_TO_DATE', 'STATUS', 'IS_DELETE', 'PRODUCT_KEY',
                                    'DISTRIBUTOR_BOOK_PRICE_USD', 'END_POINT_PRICE_USD', 'FIELD_MINIMUM_QUANTITY', 'MINIMUM_PACKAGE_QUANTITY', 'MULTIPLIER',
                                    'REFERENCE_PRICE', 'STRATEGIC_MINIMUM_PRICE', 'WALK_AWAY_PRICE', 'FRAME_NAME', 'GPS_PRICE_CATEGORY',
                                    'GPS_PROGRAM_DESCRIPTION', 'SOURCE_SYSTEM', 'GPS_CATEGORY', 'GPS_CATEGORY_DESCRIPTION', 'GPS_MARKET', 'GPS_MARKET_DESCRIPTION',
                                    'GPS_PROGRAM_CODE', 'GPS_PROGRAM_CODE_DESCRIPTION', 'MARGIN_LEVEL', 'PRODUCT_FRAME_TYPE',
                                    'PRODUCT_FRAME_TYPE_DESCRIPTION', 'IS_NCNR', 'IS_PRICE_BOOK', 'IS_QUOTE', 'IS_STEP_PRICING', 'STAR_LEVEL',
                                    'IS_FIRST_TIME_IN_PRICE_BOOK', 'ON_TARGET_REMOVAL_DATE', 'ON_TARGET_START_DATE', 'PHASE_OUT_END_DATE', 'PHASE_OUT_START_DATE',
                                    'REFERENCE_QUANTITY', 'MINIMUM_ORDER_QUANTITY', 'RESALE_PRICE', 'MINIMUM_DOLLAR_VALUE', 'COGS_PRICE',
                                    'DISTRIBUTOR_BOOK_PRICE_EUR', 'DISTRIBUTOR_BOOK_PRICE_JPY', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY'] 
		,post_hook= [v_sql_upd_success_batch]
        )
}}

  SELECT 
        stg.GPS_KEY, 
        stg.PRODUCT_ID, 
        stg.EFFECTIVE_FROM_DATE,
        STG.EFFECTIVE_TO_DATE, 
        stg.STATUS, 
        stg.IS_DELETE, 
        stg.PRODUCT_KEY, 
        stg.DISTRIBUTOR_BOOK_PRICE_USD, 
        stg.DISTRIBUTOR_BOOK_PRICE_EUR, 
        stg.DISTRIBUTOR_BOOK_PRICE_JPY, 
        stg.END_POINT_PRICE_USD, 
        stg.FIELD_MINIMUM_QUANTITY, 
        stg.MINIMUM_PACKAGE_QUANTITY, 
        stg.MULTIPLIER, 
        stg.REFERENCE_PRICE, 
        stg.STRATEGIC_MINIMUM_PRICE, 
        stg.WALK_AWAY_PRICE, 
        stg.FRAME_NAME, 
        stg.GPS_PRICE_CATEGORY, 
        stg.GPS_PROGRAM_DESCRIPTION, 
        stg.GPS_CATEGORY, 
        stg.GPS_CATEGORY_DESCRIPTION, 
        stg.GPS_MARKET, 
        stg.GPS_MARKET_DESCRIPTION, 
        stg.GPS_PROGRAM_CODE, 
        stg.GPS_PROGRAM_CODE_DESCRIPTION, 
        stg.MARGIN_LEVEL, 
        stg.PRODUCT_FRAME_TYPE, 
        stg.PRODUCT_FRAME_TYPE_DESCRIPTION, 
        stg.IS_NCNR, 
        stg.IS_PRICE_BOOK, 
        stg.IS_QUOTE, 
        stg.IS_STEP_PRICING, 
        stg.STAR_LEVEL, 
        stg.IS_FIRST_TIME_IN_PRICE_BOOK, 
        stg.ON_TARGET_REMOVAL_DATE, 
        stg.ON_TARGET_START_DATE, 
        stg.PHASE_OUT_END_DATE, 
        stg.PHASE_OUT_START_DATE, 
        stg.REFERENCE_QUANTITY, 
        stg.MINIMUM_ORDER_QUANTITY, 
        stg.RESALE_PRICE, 
        stg.MINIMUM_DOLLAR_VALUE, 
        stg.COGS_PRICE, 
        stg.SOURCE_SYSTEM, 
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
        {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
        stg.BIW_MD5_KEY
    FROM 
        {{ref('ETL_MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST')}} STG
        {% if is_incremental() %}
            LEFT JOIN {{ this }} TGT
            on STG.GPS_KEY= TGT.GPS_KEY
            WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
        {% endif %}