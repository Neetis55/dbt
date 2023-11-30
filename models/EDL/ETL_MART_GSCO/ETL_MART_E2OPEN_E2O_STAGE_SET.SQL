--dbt run --full-refresh --select ETL_MART_E2OPEN_E2O_STAGE_SET
-- dbt run --select ETL_MART_E2OPEN_E2O_STAGE_SET
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['STAGE_SET']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_E2OPEN_E2O_STAGE_SET'-%}
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
         description = 'Building table E2O_LOT_TXN_DTL for MART_E2OPEN'
        ,transient=true
        ,materialized='incremental'
        ,schema ='ETL_MART_E2OPEN'
        ,alias='E2O_STAGE_SET'
		,unique_key= v_pk_list
        ,tags =['E2OPEN']
		,merge_update_columns = ['LOCATION', 'COUNTRY_CODE', 'STAGE_SET_TYPE', 'STAGE_SET_DESCRIPTION', 'MFG_PROCESS', 'VALID_QUANTITY_LOCATION', 'UOM_IN_QUANTITY1', 'UOM_OUT_QUANTITY1', 
                                'UOM_IN_QUANTITY2', 'UOM_OUT_QUANTITY2', 'BIN_LEVEL', 'BU_OWNER', 'AREA_NAME', 'MEMORY_TAG', 'MEMORY_TAG_VALUE', 'IN_TRANSIT_FLAG', 'SHIPPING_FLAG', 'STORE_FLAG', 
                                'MEMORY_FLAG', 'HOLD_FLAG', 'DIE_BANK_FLAG', 'WAFER_BANK_FLAG', 'FUTURE_HOLD_FLAG', 'SORT_ORDER', 'CREATED_ON_UDT', 'CREATED_BY', 'UPDATED_ON_UDT', 'UPDATED_BY', 
                                'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']					
		,post_hook= [v_sql_upd_success_batch]	
        )
}}
WITH 
STG_STAGE_SET AS
(
SELECT 
    STAGE_SET,
    LOCATION,
    COUNTRY_CODE,
    STAGE_SET_TYPE,
    STAGE_SET_DESCRIPTION,
    MFG_PROCESS,
    VALID_QTY_LOCATION AS VALID_QUANTITY_LOCATION,
    UOM_IN_QTY1 AS UOM_IN_QUANTITY1,
    UOM_OUT_QTY1 AS UOM_OUT_QUANTITY1,
    UOM_IN_QTY2 AS UOM_IN_QUANTITY2,
    UOM_OUT_QTY2 AS UOM_OUT_QUANTITY2,
    BIN_LEVEL,
    BU_OWNER,
    AREA_NAME,
    MEMORY_TAG,
    MEMORY_TAG_VALUE,
    COALESCE(TRY_TO_BOOLEAN(IN_TRANSIT_FLAG),'FALSE')::BOOLEAN AS IN_TRANSIT_FLAG,
    COALESCE(TRY_TO_BOOLEAN(SHIPPING_FLAG),'FALSE')::BOOLEAN AS SHIPPING_FLAG,
    COALESCE(TRY_TO_BOOLEAN(STORE_FLAG),'FALSE')::BOOLEAN AS STORE_FLAG,
    COALESCE(TRY_TO_BOOLEAN(MEMORY_FLAG),'FALSE')::BOOLEAN AS MEMORY_FLAG,
    COALESCE(TRY_TO_BOOLEAN(HOLD_FLAG),'FALSE')::BOOLEAN AS HOLD_FLAG,
    COALESCE(TRY_TO_BOOLEAN(DIE_BANK_FLAG),'FALSE')::BOOLEAN AS DIE_BANK_FLAG,
    COALESCE(TRY_TO_BOOLEAN(WAFER_BANK_FLAG),'FALSE')::BOOLEAN AS WAFER_BANK_FLAG,
    COALESCE(TRY_TO_BOOLEAN(FUTURE_HOLD_FLAG),'FALSE')::BOOLEAN AS FUTURE_HOLD_FLAG,
    SORT_ORDER,
    TO_TIMESTAMP_TZ (SUBSTR (CREATED_ON, 1, LENGTH (CREATED_ON) - 3) || ' +00:00','MM/DD/YYYY HH24:MI:SS TZH:TZM') CREATED_ON_UDT,
    CREATED_BY,
    TO_TIMESTAMP_TZ (SUBSTR (UPDATED_ON, 1, LENGTH (UPDATED_ON) - 3) || ' +00:00','MM/DD/YYYY HH24:MI:SS TZH:TZM') UPDATED_ON_UDT,
    UPDATED_BY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
	'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
	{{V_BIW_BATCH_ID}} as BIW_BATCH_ID   ,
    md5(object_construct ('col1',LOCATION, 'col2',COUNTRY_CODE, 'col3',STAGE_SET_TYPE, 'col4',STAGE_SET_DESCRIPTION, 'col5',MFG_PROCESS, 'col6',VALID_QUANTITY_LOCATION, 
    'col7',UOM_IN_QUANTITY1, 'col8',UOM_OUT_QUANTITY1, 'col9',UOM_IN_QUANTITY2, 'col10',UOM_OUT_QUANTITY2, 'col11',BIN_LEVEL, 'col12',BU_OWNER, 'col13',AREA_NAME, 'col14',MEMORY_TAG, 
    'col15',MEMORY_TAG_VALUE, 'col16',IN_TRANSIT_FLAG, 'col17',SHIPPING_FLAG, 'col18',STORE_FLAG, 'col19',MEMORY_FLAG, 'col20',HOLD_FLAG, 'col21',DIE_BANK_FLAG, 
    'col22',WAFER_BANK_FLAG, 'col23',FUTURE_HOLD_FLAG, 'col24',SORT_ORDER, 'col25',CREATED_ON_UDT, 'col26',CREATED_BY, 'col27',UPDATED_ON_UDT, 'col28',UPDATED_BY)::string ) as BIW_MD5_KEY
FROM 
{{source('STG_E2OPEN','STG_STAGE_SET')}}
{% if is_incremental() %}
    WHERE BIW_UPD_DTTM >= '{{V_LWM}}'
    AND BIW_UPD_DTTM < '{{V_HWM}}'
{% endif %}
QUALIFY (
    ROW_NUMBER() OVER (PARTITION BY STAGE_SET ORDER BY BIW_UPD_DTTM DESC)=1 -- LATEST ON EACH PK
    )
)
SELECT
STG.*
FROM STG_STAGE_SET STG
--- ADD IF EXISTING ON UPDATE
{% if is_incremental() %}
    LEFT JOIN {{ this }} TGT
    on STG.STAGE_SET= TGT.STAGE_SET
    WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}