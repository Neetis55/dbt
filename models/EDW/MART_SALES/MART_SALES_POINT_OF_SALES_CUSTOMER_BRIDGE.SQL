/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select MART_SALES_POINT_OF_SALES_CUSTOMER_BRIDGE

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         10/18/2022      Sruthi Kasbe    Updated customer columns and its order
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['TRANSACTION_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_POINT_OF_SALES_CUSTOMER_BRIDGE') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_POINT_OF_SALES_CUSTOMER_BRIDGE') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_POINT_OF_SALES_CUSTOMER_BRIDGE'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{{
    config(
         description = 'Building table POS_CUSTOMER_BRIDGE for sales mart '
        ,transient=false
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='POINT_OF_SALES_CUSTOMER_BRIDGE'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

SELECT 
    TRANSACTION_KEY, 
    FISCAL_SHIP_DATE_KEY, 
    ADJUSTED_END_CORPORATION_KEY, 
    REFERENCE_CORPORATION_KEY, 
    MARKET_PRODUCT_NUMBER_KEY, 
    TRANSACTION_ID, 
    FISCAL_SHIP_DATE,  
    ADJUSTED_END_CORPORATION_CODE, 
    REFERENCE_CORPORATION_CODE, 
    MARKET_PRODUCT_NUMBER,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
FROM {{ref('ETL_MART_SALES_POINT_OF_SALES_CUSTOMER_BRIDGE')}}