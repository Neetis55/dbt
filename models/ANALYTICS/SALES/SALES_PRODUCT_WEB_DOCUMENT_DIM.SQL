/*---------------------------------------------------------------------------
Command to run model:
dbt run --select SALES_PRODUCT_WEB_DOCUMENT_DIM

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         06-Dec-2022     Vinayaka CN       INITIAL VERSION
---------------------------------------------------------------------------*/

{{ config(
  description = 'PRODUCT_WEB_DOCUMENT view for Sales Analytics Team', 
  materialized = 'view', 
  schema = 'SALES', 
  tags = 'MART_SALES',
  alias = 'PRODUCT_WEB_DOCUMENT_DIM'
) }} 

SELECT
    PRODUCT_WEB_DOCUMENT_KEY,
    PRODUCT_ID ,
    BASE_PART ,
    DOCUMENT_NAME ,
    DOCUMENT_FILE_NAME ,
    DOCUMENT_TYPE ,
    BIW_INS_DTTM ,
    BIW_UPD_DTTM ,
    BIW_BATCH_ID 
FROM 
  {{ref('MART_SALES_PRODUCT_WEB_DOCUMENT')}} 
