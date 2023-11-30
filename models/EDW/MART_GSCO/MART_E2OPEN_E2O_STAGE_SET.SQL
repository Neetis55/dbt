--dbt run --full-refresh --select MART_E2OPEN_E2O_STAGE_SET
--dbt run --select MART_E2OPEN_E2O_STAGE_SET
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['STAGE_SET_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_E2OPEN_E2O_STAGE_SET') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_E2OPEN_E2O_STAGE_SET') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_E2OPEN_E2O_STAGE_SET'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_E2OPEN_E2O_STAGE_SET'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building table E2O_STAGE_SET dimension for E2OPEN MART '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_E2OPEN'
        ,alias='E2O_STAGE_SET'
        ,unique_key= v_pk_list
    	,merge_update_columns =  ['STAGE_SET',  'LOCATION',  'COUNTRY_CODE',  'STAGE_SET_TYPE',  'STAGE_SET_DESCRIPTION',  'MFG_PROCESS', 
									'VALID_QUANTITY_LOCATION',  'UOM_IN_QUANTITY1',  'UOM_OUT_QUANTITY1',  'UOM_IN_QUANTITY2',  'UOM_OUT_QUANTITY2',  'BIN_LEVEL',  'BU_OWNER', 
									'AREA_NAME',  'MEMORY_TAG',  'MEMORY_TAG_VALUE',  'IN_TRANSIT_FLAG',  'SHIPPING_FLAG',  'STORE_FLAG',  'MEMORY_FLAG',  'HOLD_FLAG',
									'DIE_BANK_FLAG',  'WAFER_BANK_FLAG',  'FUTURE_HOLD_FLAG',  'SORT_ORDER',  'CREATED_ON_UDT',  'CREATED_BY', 
									'UPDATED_ON_UDT',  'UPDATED_BY',  'BIW_UPD_DTTM',  'BIW_BATCH_ID',  'BIW_MD5_KEY' ]  

		,tags =['E2OPEN']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH STAGE_SET AS 
(
SELECT
	MD5(STAGE_SET) AS STAGE_SET_KEY,
	STAGE_SET, 
	LOCATION, 
	COUNTRY_CODE, 
	STAGE_SET_TYPE, 
	STAGE_SET_DESCRIPTION, 
	MFG_PROCESS, 
	VALID_QUANTITY_LOCATION, 
	UOM_IN_QUANTITY1, 
	UOM_OUT_QUANTITY1, 
	UOM_IN_QUANTITY2, 
	UOM_OUT_QUANTITY2, 
	BIN_LEVEL, 
	BU_OWNER, 
	AREA_NAME, 
	MEMORY_TAG, 
	MEMORY_TAG_VALUE, 
	IN_TRANSIT_FLAG, 
	SHIPPING_FLAG, 
	STORE_FLAG, 
	MEMORY_FLAG, 
	HOLD_FLAG, 
	DIE_BANK_FLAG, 
	WAFER_BANK_FLAG, 
	FUTURE_HOLD_FLAG, 
	SORT_ORDER, 
	CREATED_ON_UDT, 
	CREATED_BY, 
	UPDATED_ON_UDT, 
	UPDATED_BY, 
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
	'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
	{{V_BIW_BATCH_ID}} as BIW_BATCH_ID   ,
	BIW_MD5_KEY
FROM
	{{ref('ETL_MART_E2OPEN_E2O_STAGE_SET')}}
{% if is_incremental() %}
	WHERE BIW_UPD_DTTM >= '{{V_LWM}}'
	AND BIW_UPD_DTTM < '{{V_HWM}}'
{% endif %}
)
SELECT
	STG.STAGE_SET_KEY,
	stg.STAGE_SET, 
	stg.LOCATION, 
	stg.COUNTRY_CODE, 
	stg.STAGE_SET_TYPE, 
	stg.STAGE_SET_DESCRIPTION, 
	stg.MFG_PROCESS, 
	stg.VALID_QUANTITY_LOCATION, 
	stg.UOM_IN_QUANTITY1, 
	stg.UOM_OUT_QUANTITY1, 
	stg.UOM_IN_QUANTITY2, 
	stg.UOM_OUT_QUANTITY2, 
	stg.BIN_LEVEL, 
	stg.BU_OWNER, 
	stg.AREA_NAME, 
	stg.MEMORY_TAG, 
	stg.MEMORY_TAG_VALUE, 
	stg.IN_TRANSIT_FLAG, 
	stg.SHIPPING_FLAG, 
	stg.STORE_FLAG, 
	stg.MEMORY_FLAG, 
	stg.HOLD_FLAG, 
	stg.DIE_BANK_FLAG, 
	stg.WAFER_BANK_FLAG, 
	stg.FUTURE_HOLD_FLAG, 
	stg.SORT_ORDER, 
	stg.CREATED_ON_UDT, 
	stg.CREATED_BY, 
	stg.UPDATED_ON_UDT, 
	stg.UPDATED_BY, 
	stg.BIW_INS_DTTM, 
	stg.BIW_UPD_DTTM, 
	stg.BIW_BATCH_ID, 
	stg.BIW_MD5_KEY

FROM
	STAGE_SET STG
{% if is_incremental() %}
LEFT JOIN {{ this }} TGT
ON
	STG.STAGE_SET_KEY = TGT.STAGE_SET_KEY
WHERE
	TGT.BIW_MD5_KEY <> STG.BIW_MD5_KEY
	OR TGT.BIW_MD5_KEY IS NULL
{% endif %}