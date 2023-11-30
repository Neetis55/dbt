{{ config(
  description = 'OM_EP_LINEDOWN_ORDER_INFO view for Sales Analytics Team', 
  materialized = 'view', 
  schema = 'SALES', 
  tags = 'MART_OUTSYS_SALES',
  alias = 'OM_EP_LINEDOWN_ORDER_INFO'
) }} 


select 
    LINEDOWN_ORDER_RULE_KEY,
    LindownId,
    PONumber,
    POINumber,
    BIW_INS_DTTM ,
    BIW_UPD_DTTM ,
    BIW_BATCH_ID,
    BIW_MD5_KEY  
    FROM {{ref('MART_SALES_OM_EP_LINEDOWN_ORDER_INFO') }}