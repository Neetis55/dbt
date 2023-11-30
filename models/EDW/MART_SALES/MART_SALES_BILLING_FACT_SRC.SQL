/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select MART_SALES_BILLING_FACT_SRC --vars 'is_one_time_load: True'
--dbt run --full-refresh --select MART_SALES_BILLING_FACT_SRC
--dbt run --select MART_SALES_BILLING_FACT_SRC

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         10/18/2022      Sruthi Kasbe    Updated customer columns and its order
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['BILLING_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_BILLING_FACT_SRC') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_BILLING_FACT_SRC') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_BILLING_FACT_SRC'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table Billing_FACT for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='BILLING_FACT_SRC'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['BILLING_DATE_KEY', 'DIRECT_CUSTOMER_KEY', 'INDIRECT_CUSTOMER_KEY', 'END_CUSTOMER_KEY',
                                'DIST_END_CUSTOMER_KEY', 'DIRECT_CORPORATION_KEY', 'END_CORPORATION_KEY', 'MARKET_PRODUCT_NUMBER_KEY', 'INTERNAL_PART_NUMBER_KEY',
                                'PROCESS_DATE', 'DIRECT_CUSTOMER_CODE', 'INDIRECT_CUSTOMER_CODE', 'END_CUSTOMER_CODE', 'DIST_END_CUSTOMER_CODE',
                                'DIRECT_CORPORATION_CODE', 'END_CORPORATION_CODE', 'MARKET_PRODUCT_NUMBER', 'INTERNAL_PART_NUMBER', 'SOURCE_OF_SALE',
                                'REGION', 'SALES_ORDER_NUMBER', 'SALES_ITEM_NUMBER', 'SALES_DELIVERY_NUMBER',
                                'SALES_ORDER_LINE_ITEM_DELIVERY', 'ORDER_TYPE', 'HANDLING_CODE', 'CUSTOMER_PART_NUMBER', 'CUSTOMER_PA_NUMBER', 'REQUESTED_DATE',
                                'ORIGINAL_SCHEDULED_SHIP_DATE', 'SCHEDULED_SHIPPED_DATE', 'EARLIEST_POSSIBLE_SHIP_DATE', 'PLANNED_DELIVERY_DATE',
                                'RESCHEDULED_DELIVERY_DATE', 'REQUESTED_SHIP_DATE', 'ENTRY_DATE', 'CUSTOMER_OF_INTEREST', 'IS_NNCO', 'NNCO_AGREEMENT_TYPE',
                                'ODM_END_CORP_CODE', 'BILL_TO_COUNTRY', 'BILL_TO_CUST_CODE', 'SUB_CORP_CODE', 'SHIP_FROM_AREA', 'CUSTOMER_PO_NUMBER',
                                'QUOTE_ID', 'OPERATING_UNIT', 'ITEM_STATUS', 'IS_REVERSAL_REQUIRED', 'ACTIVITY', 'BOOKING_CATEGORY',
                                'BILL_SOURCE_CODE', 'BUSINESS_CLASS_CODE', 'CREDIT_REASON_CODE', 'CREDIT_CODE_DESCRIPTION', 'PRICE_SOURCE',
                                'GL_POSTED_DATE', 'TRANSACTION_DATE', 'CUSTOMER_TRANSACTION_ID', 'CUSTOMER_TRANSACTION_LINE_ID',
                                'OBR_TRANSACTION_ID', 'TRANSACTION_LINE_NUMBER', 'BILLING_DOCUMENT_NUMBER', 'ORIGINAL_DOCUMENT_NUMBER', 'IS_TRUE_BILL',
                                'GL_REVENUE_ACCOUNT', 'ACCRUAL_CATEGORY', 'JOURNAL_DESCRIPTION', 'LEDGER', 'DEBIT_ACCOUNT', 'CREDIT_ACCOUNT',
                                'ACCRUAL_RULE_SET', 'ACCRUAL_RULE_ID', 'ACCRUAL_COMMENTS', 'ACCRUAL_TYPE', 'ACCRUAL_FACTOR',
                                'ACCRUAL_TRANSACTION_DATE', 'IS_ACCRUAL_DSA', 'IS_ACCRUAL_DCD', 'IS_ACCRUAL_REB', 'IS_ACCRUAL_DSC', 'IS_ACCRUAL_REVERSAL',
                                'IS_QUANTITY_OVERRIDE', 'TRANSACTION_CURRENCY_ISO_CODE', 'CONTRACT_CURRENCY', 'BILLING_GROSS_QUANTITY',
                                'BILLING_NET_QUANTITY', 'UNIT_PRICE_USD', 'UNIT_PRICE_EUR', 'UNIT_PRICE_JPY', 'UNIT_PRICE_TRANSACTION_CURRENCY',
                                'GROSS_AMOUNT_USD', 'GROSS_AMOUNT_TRANSACTION_CURRENCY', 'GROSS_AMOUNT_EUR', 'GROSS_AMOUNT_JPY',
                                'GROSS_AMOUNT_CONTRACT_CURRENCY', 'BILLING_NET_AMOUNT_USD', 'BILLING_NET_AMOUNT_TRANSACTION_CURRENCY', 'BILLING_NET_AMOUNT_EUR',
                                'BILLING_NET_AMOUNT_JPY', 'BILLING_NET_AMOUNT_CONTRACT_CURRENCY', 'ACCRUAL_AMOUNT_USD',
                                'ACCRUAL_AMOUNT_TRANSACTION_CURRENCY', 'ACCRUAL_AMOUNT_CONTRACT_CURRENCY', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

SELECT
    BILLING_KEY, 
    BILLING_DATE_KEY, 
    DIRECT_CUSTOMER_KEY, 
    INDIRECT_CUSTOMER_KEY, 
    END_CUSTOMER_KEY, 
    DIST_END_CUSTOMER_KEY, 
    DIRECT_CORPORATION_KEY, 
    END_CORPORATION_KEY, 
    MARKET_PRODUCT_NUMBER_KEY, 
    INTERNAL_PART_NUMBER_KEY,  
    DIRECT_CUSTOMER_CODE, 
    INDIRECT_CUSTOMER_CODE, 
    END_CUSTOMER_CODE, 
    DIST_END_CUSTOMER_CODE, 
    DIRECT_CORPORATION_CODE, 
    END_CORPORATION_CODE, 
    MARKET_PRODUCT_NUMBER, 
    INTERNAL_PART_NUMBER, 
    PROCESS_DATE,
    SOURCE_OF_SALE, 
    REGION, 
    SALES_ORDER_NUMBER, 
    SALES_ITEM_NUMBER, 
    SALES_DELIVERY_NUMBER, 
    SALES_ORDER_LINE_ITEM_DELIVERY, 
    ORDER_TYPE, 
    HANDLING_CODE, 
    CUSTOMER_PART_NUMBER, 
    CUSTOMER_PA_NUMBER, 
    REQUESTED_DATE, 
    ORIGINAL_SCHEDULED_SHIP_DATE, 
    SCHEDULED_SHIPPED_DATE, 
    EARLIEST_POSSIBLE_SHIP_DATE, 
    PLANNED_DELIVERY_DATE, 
    RESCHEDULED_DELIVERY_DATE, 
    REQUESTED_SHIP_DATE, 
    ENTRY_DATE, 
    CUSTOMER_OF_INTEREST, 
    IS_NNCO, 
    NNCO_AGREEMENT_TYPE, 
    ODM_END_CORP_CODE, 
    BILL_TO_COUNTRY, 
    BILL_TO_CUST_CODE, 
    SUB_CORP_CODE, 
    SHIP_FROM_AREA, 
    CUSTOMER_PO_NUMBER, 
    QUOTE_ID, 
    OPERATING_UNIT, 
    ITEM_STATUS, 
    IS_REVERSAL_REQUIRED, 
    ACTIVITY, 
    BOOKING_CATEGORY, 
    BILL_SOURCE_CODE, 
    BUSINESS_CLASS_CODE, 
    CREDIT_REASON_CODE, 
    CREDIT_CODE_DESCRIPTION, 
    PRICE_SOURCE, 
    GL_POSTED_DATE, 
    TRANSACTION_DATE, 
    CUSTOMER_TRANSACTION_ID, 
    CUSTOMER_TRANSACTION_LINE_ID, 
    OBR_TRANSACTION_ID, 
    TRANSACTION_LINE_NUMBER, 
    BILLING_DOCUMENT_NUMBER, 
    ORIGINAL_DOCUMENT_NUMBER, 
    IS_TRUE_BILL, 
    GL_REVENUE_ACCOUNT, 
    ACCRUAL_CATEGORY, 
    JOURNAL_DESCRIPTION, 
    LEDGER, 
    DEBIT_ACCOUNT, 
    CREDIT_ACCOUNT, 
    ACCRUAL_RULE_SET, 
    ACCRUAL_RULE_ID, 
    ACCRUAL_COMMENTS, 
    ACCRUAL_TYPE, 
    ACCRUAL_FACTOR, 
    ACCRUAL_TRANSACTION_DATE, 
    IS_ACCRUAL_DSA, 
    IS_ACCRUAL_DCD, 
    IS_ACCRUAL_REB, 
    IS_ACCRUAL_DSC, 
    IS_ACCRUAL_REVERSAL, 
    IS_QUANTITY_OVERRIDE, 
    TRANSACTION_CURRENCY_ISO_CODE, 
    CONTRACT_CURRENCY, 
    BILLING_GROSS_QUANTITY, 
    BILLING_NET_QUANTITY, 
    UNIT_PRICE_USD, 
    UNIT_PRICE_EUR, 
    UNIT_PRICE_JPY, 
    UNIT_PRICE_TRANSACTION_CURRENCY, 
    GROSS_AMOUNT_USD, 
    GROSS_AMOUNT_TRANSACTION_CURRENCY, 
    GROSS_AMOUNT_EUR, 
    GROSS_AMOUNT_JPY, 
    GROSS_AMOUNT_CONTRACT_CURRENCY, 
    BILLING_NET_AMOUNT_USD, 
    BILLING_NET_AMOUNT_TRANSACTION_CURRENCY, 
    BILLING_NET_AMOUNT_EUR, 
    BILLING_NET_AMOUNT_JPY, 
    BILLING_NET_AMOUNT_CONTRACT_CURRENCY, 
    ACCRUAL_AMOUNT_USD, 
    ACCRUAL_AMOUNT_TRANSACTION_CURRENCY, 
    ACCRUAL_AMOUNT_CONTRACT_CURRENCY, 
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
    BIW_MD5_KEY
FROM {{ref('ETL_MART_SALES_BILLING_FACT_SRC')}}