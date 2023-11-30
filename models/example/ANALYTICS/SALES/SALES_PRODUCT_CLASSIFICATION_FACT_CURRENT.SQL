/*---------------------------------------------------------------------------
Command to run model:
dbt run --select PRODUCT_CLASSIFICATION_FACT_CURRENT

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         19-Dec-2022     Vinayaka CN       INITIAL VERSION
---------------------------------------------------------------------------*/

{{ config(
  description = 'PRODUCT_CLASSIFICATION_FACT_CURRENT view for Sales Analytics Team', 
  materialized = 'view', 
  schema = 'SALES', 
  tags = 'MART_SALES',
  alias = 'PRODUCT_CLASSIFICATION_FACT_CURRENT'
) }} 

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
    ,BIW_INS_DTTM 
    ,BIW_UPD_DTTM 
    ,BIW_BATCH_ID 
FROM 
  {{ref('MART_SALES_PRODUCT_CLASSIFICATION_FACT')}} 
where FISCAL_WEEK_KEY = (select max(FISCAL_WEEK_KEY) from  {{ref('MART_SALES_PRODUCT_CLASSIFICATION_FACT')}} )