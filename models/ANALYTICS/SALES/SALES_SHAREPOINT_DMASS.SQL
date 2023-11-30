{{ config(
  description = 'SHAREPOINT_DMASS view for Sales Analytics Team', 
  materialized = 'view', 
  schema = 'SALES', 
  tags = 'MART_SALES',
  alias = 'SHAREPOINT_DMASS'
) }} 

SELECT
    ID,
    PAL2 AS PAL2_CODE,
    PAL3 AS PAL3_CODE,
    DMASS_PRODUCT_GROUP,
    DMASS_SPEC1,
    DMASS_SPEC2,
    CREATED AS DMASS_CREATED_DATE,
    CREATED_BY AS DMASS_CREATED_BY,
    MODIFIED AS DMASS_MODIFIED_DATE,
    MODIFIED_BY AS DMASS_MODIFIED_BY,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID
FROM {{source ('STG_SHAREPOINT_MDL','DMASS_MAPPING')}}