/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select MART_SALES_POINT_OF_SALES_FACT
--dbt run --select MART_SALES_POINT_OF_SALES_FACT
--dbt run --select MART_SALES_POINT_OF_SALES_FACT --vars 'is_one_time_load: True'

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         10/18/2022      Sruthi Kasbe    Updated customer columns and its order
---------------------------------------------------------------------------*/
-- depends_on: {{ ref('ETL_MART_SALES_POINT_OF_SALES_FACT') }}
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['TRANSACTION_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_POINT_OF_SALES_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_POINT_OF_SALES_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_POINT_OF_SALES_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{%- set V_EDL_DB = var('V_EDL_DEFAULT_DB') +env_var('DBT_DEP_ENV')+'.' -%}
{%- set V_EDW_DB = var('V_EDW_DEFAULT_DB') +env_var('DBT_DEP_ENV')+'.' -%}
--select {{V_EDL_DB}}
{%- set v_sql_update_tgt -%}
update {{V_EDW_DB}}MART_SALES.POINT_OF_SALES_FACT TGT
set    BIW_LOGICAL_DELETE_FLAG='Y',
BIW_INS_DTTM = current_timestamp()::TIMESTAMP_NTZ  
WHERE BIW_LOGICAL_DELETE_FLAG <> 'Y'
AND SOURCE_DATA_FROM ='POS'
AND TRANSACTION_ID IN
                        (WITH  INACTIVE_POS AS 
                              (
                                SELECT -- LATEST VALUE OF EACH TRANSACTION
                                  CUSTOM_FIELD3,
                                  CUSTOM_FIELD7,
                                  CUSTOM_FIELD16,
                                  REVERSAL_STATUS,
                                  CR_FLAG,
                                  STATUS
                                FROM {{V_EDL_DB}}STG_MODELN_PRICING_OWNER.POS_DATA
                                WHERE CUSTOM_FIELD3 IS NOT NULL
                                QUALIFY( ROW_NUMBER() OVER (PARTITION BY CUSTOM_FIELD3 ORDER BY BIW_UPD_DTTM DESC, OID DESC)=1)
                              ),
                              ADDRESS AS 
                              (
                                  SELECT 
                                      CODE,
                                      TYPE,
                                      ADDRESS5
                                  FROM {{V_EDL_DB}}STG_MODELN_PRICING_OWNER.ADDRESS
                                  WHERE ADDRESS5 IS NOT NULL 
                                  AND TYPE = 5
                                  QUALIFY( ROW_NUMBER() OVER (PARTITION BY ADDRESS5 ORDER BY  OID DESC, BIW_UPD_DTTM DESC )=1)
                              ),
                              ---------------------------- MODELN CUSTOMER ----------------
                              MODELN_CUSTOMER AS (
                                  SELECT
                                  OID AS CRM_CUSTOMER_OID,
                                  ACCOUNT_NUM AS CUSTOMER_CODE,
                                  CUSTOM_FIELD36 AS BUSINESS_CLASS_CODE,
                                  CUSTOM_FIELD3 as END_CORPORATION_CODE,
                                  PARENT_CUSTOMER_OID 
                                  FROM {{V_EDL_DB}}STG_MODELN_PRICING_OWNER.CUSTOMER
                                  QUALIFY( ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUM ORDER BY MODIFIED_DATE DESC, OID DESC)=1)
                              )
                        SELECT 
                        POS.CUSTOM_FIELD3
                        FROM INACTIVE_POS POS
                        LEFT JOIN ADDRESS  SHIP_ADDRESS 
                                ON POS.CUSTOM_FIELD7 || ':' || POS.CUSTOM_FIELD16 = SHIP_ADDRESS.ADDRESS5
                        LEFT JOIN MODELN_CUSTOMER AS SHIP_CUST
                            ON SHIP_ADDRESS.CODE = SHIP_CUST.CUSTOMER_CODE
                        WHERE REVERSAL_STATUS IS NOT NULL 
                        OR ( CR_FLAG <> 'Y' AND STATUS IS NULL)
                        OR (SHIP_CUST.BUSINESS_CLASS_CODE  IN ('DIST','CIPO','D3PL'))
                          )
;
{%endset%}  
{{
    config(
         description = 'Building table POS_FACT for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='POINT_OF_SALES_FACT'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,merge_update_columns= ['FISCAL_SHIP_DATE_KEY', 'DIRECT_CUSTOMER_KEY', 'INDIRECT_CUSTOMER_KEY', 'END_CUSTOMER_KEY',
                                'DIRECT_CORPORATION_KEY', 'INDIRECT_CORPORATION_KEY', 'END_CORPORATION_KEY', 'MARKET_PRODUCT_NUMBER_KEY', 'INTERNAL_PART_NUMBER_KEY',
                                'TRANSACTION_ID', 'FISCAL_SHIP_DATE', 'MARKET_PRODUCT_NUMBER', 'INTERNAL_PART_NUMBER', 'DIRECT_CUSTOMER_CODE',
                                'INDIRECT_CUSTOMER_CODE', 'END_CUSTOMER_CODE', 'DIRECT_CORPORATION_CODE', 'INDIRECT_CORPORATION_CODE',
                                'END_CORPORATION_CODE', 'TRANSACTION_CODE', 'REPORTED_SHIP_DATE', 'REPORTED_BRANCH_ID', 'RESALE_QUANTITY',
                                'DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER', 'DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME', 'DISTRIBUTOR_END_CUSTOMER_IDENTIFIER',
                                'DISTRIBUTOR_END_CUSTOMER_NAME', 'DISTRIBUTOR_INVOICE_NUMBER', 'DISTRIBUTOR_INVOICE_LINE_NUMBER', 'REGISTRATION_NUMBER',
                                'DEBIT_NUMBER', 'DISTRIBUTOR_PO_NUMBER', 'TRANSACTION_CURRENCY_ISO_CODE', 'RESALE_UNIT_PRICE',
                                'RESALE_UNIT_PRICE_USD', 'RESALE_NET_AMOUNT', 'RESALE_NET_AMOUNT_USD', 'RESALE_NET_AMOUNT_EUR', 'RESALE_NET_AMOUNT_JPY',
                                'DISTRIBUTOR_UNIT_PRICE', 'DISTRIBUTOR_UNIT_PRICE_USD', 'DISTRIBUTOR_COST_AMOUNT', 'DISTRIBUTOR_COST_AMOUNT_USD',
                                'DISTRIBUTOR_COST_AMOUNT_EUR', 'DISTRIBUTOR_COST_AMOUNT_JPY', 'SOURCE_DATA_FROM', 'ONSALES_CREATE_DATE', 'ONSALES_MODIFIED_DATE',
                                'IS_TRUE_POS', 'IS_SDM_TRUE_POS', 'IS_VAR', 'END_CORPORATION_DECODE', 'BIW_UPD_DTTM', 'BIW_BATCH_ID',
                                'BIW_MD5_KEY', 'BIW_LOGICAL_DELETE_FLAG'] 
        ,post_hook= [v_sql_update_tgt, v_sql_upd_success_batch]
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
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY,
    BIW_LOGICAL_DELETE_FLAG
FROM {{ref('ETL_MART_SALES_POINT_OF_SALES_FACT')}} 
{% if is_incremental() is false or var('is_one_time_load') %} 
    UNION ALL
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
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY,
    BIW_LOGICAL_DELETE_FLAG
FROM 
    {{ref('ETL_MART_SALES_POINT_OF_SALES_FACT_FCS_ONE_TIME_LOAD')}}
{% endif %}
