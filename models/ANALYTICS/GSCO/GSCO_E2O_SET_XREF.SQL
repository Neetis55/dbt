{{ config(
  description = 'E2open view for E2O_SET_XREF table', 
  materialized = 'view', 
  schema = 'GSCO', 
  tags = ['E2OPEN'],
  alias = 'E2O_SET_XREF'
) }} 
SELECT 
    SUPPLIER_ENTITY_NAME,
    SUPPLIER_ENTITY_DESCRIPTION,
    SUPPLIER,
    ENTITY_TYPE,
    SYSTEM_ENTITY_KEY,
    CREATED_ON_UDT,
    CREATED_BY,
    UPDATED_ON_UDT,
    UPDATED_BY,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID
FROM {{ref('MART_E2OPEN_E2O_SET_XREF')}}
