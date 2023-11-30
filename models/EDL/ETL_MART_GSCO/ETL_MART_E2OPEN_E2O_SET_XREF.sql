--dbt run --full-refresh --select ETL_MART_E2OPEN_E2O_SET_XREF
-- dbt run --select ETL_MART_E2OPEN_E2O_SET_XREF
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SUPPLIER_ENTITY_NAME']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_E2OPEN_E2O_SET_XREF'-%}
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
         description = 'Building ETL table E2O_SET_XREF for for MART_E2OPEN'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_E2OPEN'
        ,alias='E2O_SET_XREF'
		,tags =['E2OPEN']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}


SELECT 
	SUPPLIER  ,
	ENTITY_TYPE  ,
	SUPPLIER_ENTITY_NAME  ,
	SUPPLIER_ENTITY_DESCRIPTION  ,
	SYSTEM_ENTITY_KEY  ,
    CREATED_ON , 
	CREATED_BY  ,
    UPDATED_ON , 
	UPDATED_BY  ,
	TO_TIMESTAMP_TZ(SUBSTR(STG_SET_XREF.CREATED_ON,1,19)  || ' +00:00','MM/DD/YYYY HH24:MI:SS TZH:TZM') AS CREATED_ON_UDT ,
	TO_TIMESTAMP_TZ(SUBSTR(STG_SET_XREF.UPDATED_ON,1,19)  || ' +00:00','MM/DD/YYYY HH24:MI:SS TZH:TZM') AS UPDATED_ON_UDT ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SUPPLIER, 'col2',ENTITY_TYPE, 'col3',SUPPLIER_ENTITY_DESCRIPTION,
    'col4',SYSTEM_ENTITY_KEY, 'col5',CREATED_ON, 'col6',CREATED_BY, 'col7',UPDATED_ON,
    'col8',UPDATED_BY)::string ) as BIW_MD5_KEY
FROM 
  {{ source('STG_E2OPEN', 'STG_SET_XREF') }}
WHERE
BIW_UPD_DTTM >= '{{V_LWM}}'
AND BIW_UPD_DTTM < '{{V_HWM}}'
QUALIFY( ROW_NUMBER() OVER (PARTITION BY SUPPLIER_ENTITY_NAME ORDER BY BIW_UPD_DTTM DESC) =1)
