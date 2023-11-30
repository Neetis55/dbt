/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select MART_SALES_SAMPLE_FACT
--dbt run --select MART_SALES_SAMPLE_FACT

Version     Date            Author               Description
-------     --------        -----------          ----------------------------------
1.0         12/28/2022      Sruthi Kasbe         Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SAMPLE_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_SAMPLE_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_SAMPLE_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{% endif %}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_SAMPLE_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{{
    config(
         description = 'Building table SAMPLE_FACT for sales mart '
        ,transient=true
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='SAMPLE_FACT'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,merge_update_columns=[ 'DATE_KEY', 'MARKET_PRODUCT_NUMBER_KEY', 'SAMPLE_LINE_ITEM_ID', 'ORDER_ID', 'LINE_ITEM_ID', 'SHIPMENT_ID', 'SAMPLE_CREATED_DATE',
                                'MARKET_PRODUCT_NUMBER', 'LINE_ITEM_CREATED_BY', 'LINE_ITEM_STATUS', 'LINE_ITEM_WORKFLOW_STATUS', 'LINE_ITEM_WORKFLOW_UPDATED_DATE',
                                'SAMPLE_CREATED_USER', 'SAMPLE_USER_COMPANY', 'LINE_ITEM_DESIGN_CUSTOMER_NAME', 'END_CUSTOMER_NAME', 'LINE_ITEM_DESCRIPTION',
                                'SAMPLE_CREATED_BY', 'SAMPLE_CREATED_BY_EMAIL', 'SAMPLE_USER_CHANNEL', 'SAMPLE_REQUESTED_QUANTITY', 'SOURCE_TYPE', 'CORPORATION_CODE',
                                'LINE_ITEM_APPROVER_USER_ID', 'LINE_ITEM_APPROVER_NAME', 'SECONDARY_LINE_ITEM_APPROVER_USER_ID', 'LINE_ITEM_SOURCE_OID',
                                'SAMPLE_SHIP_TO_CONTACT', 'SAMPLE_SHIP_TO_COMPANY', 'SAMPLE_SHIP_TO_ADDRESS1', 'SAMPLE_SHIP_TO_ADDRESS2', 'SAMPLE_SHIP_TO_ADDRESS3',
                                'SAMPLE_SHIP_TO_CITY', 'SAMPLE_SHIP_TO_COUNTRY', 'SAMPLE_SHIP_TO_CONTACT_PHONE', 'SAMPLE_SHIP_TO_CONTACT_EMAIL', 'SAMPLE_SHIP_TO_ZIP',
                                'LINE_ITEM_OPPORTUNITY_NUMBER', 'SAMPLE_SHIPMENT_ORDER_NUMBER', 'SALES_ORDER_ID', 'SAMPLE_CASSOP_LINE_ITEM', 'SAMPLE_SHIP_DATE',
                                'SAMPLE_SHIPMENT_TRACKING_NUMBER', 'SAMPLE_QUANTITY_ORDERED', 'SAMPLE_QUANTITY_SHIPPED', 'SAMPLE_PRICE_USD', 'BIW_UPD_DTTM',
                                'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,post_hook= [ v_sql_upd_success_batch]
        )
}}

SELECT 
	SAMPLE_KEY, 
    DATE_KEY,
    MARKET_PRODUCT_NUMBER_KEY, 
    SAMPLE_LINE_ITEM_ID, 
    ORDER_ID, 
    LINE_ITEM_ID, 
    SHIPMENT_ID, 
    SAMPLE_CREATED_DATE,
    MARKET_PRODUCT_NUMBER, 
    LINE_ITEM_CREATED_BY, 
    LINE_ITEM_STATUS, 
    LINE_ITEM_WORKFLOW_STATUS, 
    LINE_ITEM_WORKFLOW_UPDATED_DATE, 
    SAMPLE_CREATED_USER,  
    SAMPLE_USER_COMPANY, 
    LINE_ITEM_DESIGN_CUSTOMER_NAME, 
    END_CUSTOMER_NAME, 
    LINE_ITEM_DESCRIPTION, 
    SAMPLE_CREATED_BY, 
    SAMPLE_CREATED_BY_EMAIL, 
    SAMPLE_USER_CHANNEL, 
    SAMPLE_REQUESTED_QUANTITY, 
    SOURCE_TYPE, 
    CORPORATION_CODE, 
    LINE_ITEM_APPROVER_USER_ID, 
    LINE_ITEM_APPROVER_NAME, 
    SECONDARY_LINE_ITEM_APPROVER_USER_ID, 
    LINE_ITEM_SOURCE_OID, 
    SAMPLE_SHIP_TO_CONTACT, 
    SAMPLE_SHIP_TO_COMPANY, 
    SAMPLE_SHIP_TO_ADDRESS1, 
    SAMPLE_SHIP_TO_ADDRESS2, 
    SAMPLE_SHIP_TO_ADDRESS3, 
    SAMPLE_SHIP_TO_CITY, 
    SAMPLE_SHIP_TO_COUNTRY, 
    SAMPLE_SHIP_TO_CONTACT_PHONE, 
    SAMPLE_SHIP_TO_CONTACT_EMAIL, 
    SAMPLE_SHIP_TO_ZIP, 
    LINE_ITEM_OPPORTUNITY_NUMBER, 
    SAMPLE_SHIPMENT_ORDER_NUMBER, 
    SALES_ORDER_ID, 
    SAMPLE_CASSOP_LINE_ITEM, 
    SAMPLE_SHIP_DATE, 
    SAMPLE_SHIPMENT_TRACKING_NUMBER, 
    SAMPLE_QUANTITY_ORDERED, 
    SAMPLE_QUANTITY_SHIPPED, 
    SAMPLE_PRICE_USD, 
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
    BIW_MD5_KEY
    FROM {{ref('ETL_MART_SALES_SAMPLE_FACT')}}
