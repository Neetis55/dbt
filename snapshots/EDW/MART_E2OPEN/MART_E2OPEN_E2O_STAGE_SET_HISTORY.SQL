{% snapshot MART_E2OPEN_E2O_STAGE_SET_HISTORY %}
{{
    config(
       transient=false
      ,target_schema='MART_E2OPEN'
      ,alias='E2O_STAGE_SET_HISTORY'
      ,unique_key = 'STAGE_SET_KEY'
      ,tags =['E2OPEN']
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns =  ['STAGE_SET',  'LOCATION',  'COUNTRY_CODE',  'STAGE_SET_TYPE',  'STAGE_SET_DESCRIPTION',  'MFG_PROCESS', 
									'VALID_QUANTITY_LOCATION',  'UOM_IN_QUANTITY1',  'UOM_OUT_QUANTITY1',  'UOM_IN_QUANTITY2',  'UOM_OUT_QUANTITY2',  'BIN_LEVEL',  'BU_OWNER', 
									'AREA_NAME',  'MEMORY_TAG',  'MEMORY_TAG_VALUE',  'IN_TRANSIT_FLAG',  'SHIPPING_FLAG',  'STORE_FLAG',  'MEMORY_FLAG',  'HOLD_FLAG',
									'DIE_BANK_FLAG',  'WAFER_BANK_FLAG',  'FUTURE_HOLD_FLAG',  'SORT_ORDER',  'CREATED_ON_UDT',  'CREATED_BY', 
									'UPDATED_ON_UDT',  'UPDATED_BY',  'BIW_UPD_DTTM',  'BIW_BATCH_ID',  'BIW_MD5_KEY' ] 
    )
}}

--dbt docs requested to do select *
select * from {{ ref('MART_E2OPEN_E2O_STAGE_SET') }} 

{% endsnapshot %}