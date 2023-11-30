/*---------------------------------------------------------------------------
Command to run model:
--dbt build --full-refresh --select MART_SALES_PRODUCT_CLASSIFICATION_FACT
--dbt build --select MART_SALES_PRODUCT_CLASSIFICATION_FACT
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         19 Dec 2022      VINAYAKA CN     INITIAL VERSION
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['MARKET_PRODUCT_NUMBER_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_CLASSIFICATION_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table PRODUCT_CLASSIFICATION_FACT dimension for SALES Mart'
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_CLASSIFICATION_FACT'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['PRODUCT_ID', 'FISCAL_WEEK_KEY', 'WEB_PART_NAME', 'TOTAL_RESALE_QUANTITY',
                                'TOTAL_RESALE_QUANTITY_POS', 'TOTAL_RESALE_QUANTITY_BILLING', 'TOTAL_COST_USD', 'TOTAL_RESALE_NET_USD',
                                'DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT', 'INDIRECT_CUSTOMER_COUNT', 'END_CUSTOMER_COUNT', 'DIRECT_REGION_COUNT', 'END_REGION_COUNT',
                                'END_CORPORATION_COUNT', 'END_CORPORATION_POS_COUNT', 'END_CORPORATION_BILLING_COUNT',
                                'COMPETITOR_REPLACEMENT_SIMILAR_COUNT', 'COMPETITOR_REPLACEMENT_DIRECT_COUNT', 'COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT',
                                'PRODUCT_CLASSIFICATION', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}


WITH STG_PRODUCT_CLASSIFICATION_FACT AS (
    SELECT 
        MARKET_PRODUCT_NUMBER_KEY
        ,PRODUCT_ID
        ,FISCAL_WEEK_KEY  
        ,WEB_PART_NAME
        ,TOTAL_RESALE_QUANTITY
        ,TOTAL_RESALE_QUANTITY_POS
        ,TOTAL_RESALE_QUANTITY_BILLING
        ,TOTAL_COST_USD
        ,TOTAL_RESALE_NET_USD
        ,DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT
        ,INDIRECT_CUSTOMER_COUNT
        ,END_CUSTOMER_COUNT
        ,DIRECT_REGION_COUNT
        ,END_REGION_COUNT
        ,END_CORPORATION_COUNT
        ,END_CORPORATION_POS_COUNT
        ,END_CORPORATION_BILLING_COUNT
        ,COMPETITOR_REPLACEMENT_SIMILAR_COUNT
        ,COMPETITOR_REPLACEMENT_DIRECT_COUNT
        ,COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT
        ,PRODUCT_CLASSIFICATION 
        ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
        ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
        ,{{V_BIW_BATCH_ID}}	 AS BIW_BATCH_ID
        ,BIW_MD5_KEY
    FROM 
        {{ref('ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT')}}
)

SELECT 
     STG.MARKET_PRODUCT_NUMBER_KEY
    ,STG.PRODUCT_ID
    ,STG.FISCAL_WEEK_KEY  
    ,STG.WEB_PART_NAME
    ,STG.TOTAL_RESALE_QUANTITY
    ,STG.TOTAL_RESALE_QUANTITY_POS
    ,STG.TOTAL_RESALE_QUANTITY_BILLING
    ,STG.TOTAL_COST_USD
    ,STG.TOTAL_RESALE_NET_USD
    ,STG.DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT
    ,STG.INDIRECT_CUSTOMER_COUNT
    ,STG.END_CUSTOMER_COUNT
    ,STG.DIRECT_REGION_COUNT
    ,STG.END_REGION_COUNT
    ,STG.END_CORPORATION_COUNT
    ,STG.END_CORPORATION_POS_COUNT
    ,STG.END_CORPORATION_BILLING_COUNT
    ,STG.COMPETITOR_REPLACEMENT_SIMILAR_COUNT
    ,STG.COMPETITOR_REPLACEMENT_DIRECT_COUNT
    ,STG.COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT
    ,STG.PRODUCT_CLASSIFICATION 
    ,STG.BIW_INS_DTTM 
    ,STG.BIW_UPD_DTTM 
    ,STG.BIW_BATCH_ID
    ,STG.BIW_MD5_KEY
FROM 
    STG_PRODUCT_CLASSIFICATION_FACT STG
    {% if is_incremental() %}
        LEFT JOIN {{ this }} TGT
            on STG.MARKET_PRODUCT_NUMBER_KEY= TGT.MARKET_PRODUCT_NUMBER_KEY
        WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
    {% endif %}  