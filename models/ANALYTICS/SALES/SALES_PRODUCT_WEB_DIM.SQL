/*---------------------------------------------------------------------------
Command to run model:
dbt run --select SALES_PRODUCT_WEB

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         24-NoV-2022     Vinayaka CN       INITIAL VERSION
---------------------------------------------------------------------------*/

{{ config(
  description = 'PRODUCT_WEB_FLAG view for Sales Analytics Team', 
  materialized = 'view', 
  schema = 'SALES', 
  tags = 'MART_SALES',
  alias = 'PRODUCT_WEB_DIM'
) }} 

SELECT
    PRODUCT_KEY,
    PRODUCT_ID ,
    IS_WEB_VISIBLE,
    WEB_PART_NAME,
    WEB_PART_GROUP,
    IS_LAUNCH_SALES_TIPS ,
    IS_LAUNCH_PRESENTATION_CUSTOMER ,
    IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,
    IS_DATA_SHEET_AVAILABLE ,
    BIW_INS_DTTM ,
    BIW_UPD_DTTM ,
    BIW_BATCH_ID 
FROM 
  {{ref('MART_SALES_PRODUCT')}} 
