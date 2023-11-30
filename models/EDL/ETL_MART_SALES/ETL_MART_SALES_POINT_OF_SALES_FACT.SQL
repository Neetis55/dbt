/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_POINT_OF_SALES_FACT

Version     Date            Author             Description
-------     --------        -----------        ----------------------------------
1.0         17/01/2023      Sruthi Kasbe       Billing Load for POS model
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['TRANSACTION_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
/*--DBT Variable used during development*/
{% if is_incremental() %}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list = edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
/*--DBT Variable*/
/*--SELECT {{v_all_column_list}}*/
/*--SELECT {{v_update_column_list}}*/
/*--SELECT {{v_md5_column_list}}*/
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_POINT_OF_SALES_FACT'-%}
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
         description = 'Building ETL view POS_FACT for Sales Mart'
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='POINT_OF_SALES_FACT'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT
    TRANSACTION_KEY,
    FISCAL_SHIP_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,
    END_CUSTOMER_KEY,
    DIRECT_CORPORATION_KEY,
    INDIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,
    MARKET_PRODUCT_NUMBER_KEY,
    INTERNAL_PART_NUMBER_KEY,
    TRANSACTION_ID,
    FISCAL_SHIP_DATE,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,
    END_CUSTOMER_CODE,
    DIRECT_CORPORATION_CODE,
    INDIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    TRANSACTION_CODE,
    REPORTED_SHIP_DATE,
    REPORTED_BRANCH_ID,
    RESALE_QUANTITY,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME,
    DISTRIBUTOR_END_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_END_CUSTOMER_NAME,
    DISTRIBUTOR_INVOICE_NUMBER,
    DISTRIBUTOR_INVOICE_LINE_NUMBER,
    REGISTRATION_NUMBER,
    DEBIT_NUMBER,
    DISTRIBUTOR_PO_NUMBER,
    TRANSACTION_CURRENCY_ISO_CODE,
    RESALE_UNIT_PRICE,
    RESALE_UNIT_PRICE_USD,
    RESALE_NET_AMOUNT,
    RESALE_NET_AMOUNT_USD,
    RESALE_NET_AMOUNT_EUR,
    RESALE_NET_AMOUNT_JPY,
    DISTRIBUTOR_UNIT_PRICE,
    DISTRIBUTOR_UNIT_PRICE_USD,
    DISTRIBUTOR_COST_AMOUNT,
    DISTRIBUTOR_COST_AMOUNT_USD,
    DISTRIBUTOR_COST_AMOUNT_EUR,
    DISTRIBUTOR_COST_AMOUNT_JPY,
    SOURCE_DATA_FROM,
    ONSALES_CREATE_DATE,
    ONSALES_MODIFIED_DATE,
    IS_TRUE_POS,
    IS_SDM_TRUE_POS,
    IS_VAR,
    END_CORPORATION_DECODE,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY,
    BIW_LOGICAL_DELETE_FLAG
FROM {{ref('ETL_MART_SALES_POINT_OF_SALES_MODELN_FACT')}}
UNION ALL
SELECT
    TRANSACTION_KEY,
    BILLING_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,
    END_CUSTOMER_KEY,
    DIRECT_CORPORATION_KEY,
    INDIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,
    MARKET_PRODUCT_NUMBER_KEY,
    INTERNAL_PART_NUMBER_KEY,
    TRANSACTION_ID,
    PROCESS_DATE,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,
    END_CUSTOMER_CODE,
    DIRECT_CORPORATION_CODE,
    INDIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    TRANSACTION_CODE,
    REPORTED_SHIP_DATE,
    REPORTED_BRANCH_ID,
    BILLING_NET_QUANTITY,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME,
    DIST_END_CUSTOMER_CODE,
    DISTRIBUTOR_END_CUSTOMER_NAME,
    DISTRIBUTOR_INVOICE_NUMBER,
    DISTRIBUTOR_INVOICE_LINE_NUMBER,
    REGISTRATION_NUMBER,
    DEBIT_NUMBER,
    CUSTOMER_PO_NUMBER,
    TRANSACTION_CURRENCY_ISO_CODE,
    RESALE_UNIT_PRICE,
    RESALE_UNIT_PRICE_USD,
    RESALE_NET_AMOUNT,
    RESALE_NET_AMOUNT_USD,
    RESALE_NET_AMOUNT_EUR,
    RESALE_NET_AMOUNT_JPY,
    DISTRIBUTOR_UNIT_PRICE,
    DISTRIBUTOR_UNIT_PRICE_USD,
    DISTRIBUTOR_COST_AMOUNT,
    DISTRIBUTOR_COST_AMOUNT_USD,
    DISTRIBUTOR_COST_AMOUNT_EUR,
    DISTRIBUTOR_COST_AMOUNT_JPY,
    SOURCE_DATA_FROM,
    ONSALES_CREATE_DATE,
    ONSALES_MODIFIED_DATE,
    IS_TRUE_POS,
    IS_SDM_TRUE_POS,
    IS_VAR,
    END_CORPORATION_DECODE,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY,
    BIW_LOGICAL_DELETE_FLAG
FROM {{ref('ETL_MART_SALES_POINT_OF_SALES_BILLING_FACT')}}
