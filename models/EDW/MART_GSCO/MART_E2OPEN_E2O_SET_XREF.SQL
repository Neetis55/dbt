--dbt run --full-refresh --select MART_E2OPEN_E2O_SET_XREF
--dbt run --select MART_E2OPEN_E2O_SET_XREF
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SUPPLIER_ENTITY_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_E2OPEN_E2O_SET_XREF') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_E2OPEN_E2O_SET_XREF') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_E2OPEN_E2O_SET_XREF'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_E2OPEN_E2O_SET_XREF'-%}
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
         description = 'Building table E2O_SET_XREF dimension for E2OPEN MART '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_E2OPEN'
        ,alias='E2O_SET_XREF'
        ,unique_key= v_pk_list
    	,merge_update_columns =  ['SUPPLIER', 'ENTITY_TYPE', 'SUPPLIER_ENTITY_DESCRIPTION', 'SYSTEM_ENTITY_KEY', 'CREATED_ON_UDT', 
                                'CREATED_BY', 'UPDATED_ON_UDT', 'UPDATED_BY','BIW_UPD_DTTM', 'BIW_BATCH_ID','BIW_MD5_KEY']
		,tags =['E2OPEN']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH STG_SET_XREF AS 
(
SELECT
	MD5(SUPPLIER_ENTITY_NAME) AS SUPPLIER_ENTITY_KEY,
    SUPPLIER_ENTITY_NAME,
    SUPPLIER_ENTITY_DESCRIPTION,
    SUPPLIER,
    ENTITY_TYPE,
    SYSTEM_ENTITY_KEY,
    CREATED_ON_UDT,
    CREATED_BY,
    UPDATED_ON_UDT,
    UPDATED_BY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
	'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
	{{V_BIW_BATCH_ID}} as BIW_BATCH_ID   ,
	BIW_MD5_KEY
FROM
	{{ref('ETL_MART_E2OPEN_E2O_SET_XREF')}}
)
SELECT
	STG.*

FROM
	STG_SET_XREF STG
{% if is_incremental() %}
LEFT JOIN {{ this }} TGT
ON
	STG.SUPPLIER_ENTITY_KEY = TGT.SUPPLIER_ENTITY_KEY
WHERE
	TGT.BIW_MD5_KEY <> STG.BIW_MD5_KEY
	OR TGT.BIW_MD5_KEY IS NULL
{% endif %}