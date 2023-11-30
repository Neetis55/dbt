--dbt run --full-refresh --select MART_E2OPEN_E2O_LOT_TRANSACTION_DETAIL_FACT
-- dbt run --select MART_E2OPEN_E2O_LOT_TRANSACTION_DETAIL_FACT

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LOT_TRANSACTION_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_E2OPEN_E2O_LOT_TRANSACTION_DETAIL_FACT'-%}
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
         description = 'Building table E2O_LOT dimension for E2OPEN MART '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_E2OPEN'
        ,alias='E2O_LOT_TRANSACTION_DETAIL_FACT'
        ,unique_key= v_pk_list
    	,merge_update_columns =  ['SYSTEM_LOT_NUMBER', 'LOT_TRANSACTION_ID', 'LOT_TRANSACTION_DATE_KEY', 'LOCATION_KEY',
                                'PART_KEY', 'STAGE_SET_KEY', 'SYSTEM_TRANSACTION_UDT', 'MES_LOT_NAME', 'LOT_STATE', 'BOH_GOOD_WAFER_QUANTITY',
                                'EOH_GOOD_WAFER_QUANTITY', 'GOOD_WAFER_QUANTITY', 'BOH_REJECT_WAFER_QUANTITY', 'EOH_REJECT_WAFER_QUANTITY',
                                'REJECT_WAFER_QUANTITY', 'BOH_TOTAL_WAFER_QUANTITY', 'EOH_TOTAL_WAFER_QUANTITY', 'BOH_GOOD_DIE_QUANTITY',
                                'EOH_GOOD_DIE_QUANTITY', 'GOOD_DIE_QUANTITY', 'BOH_REJECT_DIE_QUANTITY', 'EOH_REJECT_DIE_QUANTITY', 'REJECT_DIE_QUANTITY',
                                'BOH_TOTAL_DIE_QUANTITY', 'EOH_TOTAL_DIE_QUANTITY', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY'] 
		,tags =['E2OPEN']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}
WITH STG_LOT_DTL AS
(
    SELECT 
    MD5(SYSTEM_LOT_NUMBER||'--'||LOT_TRANSACTION_ID) AS LOT_TRANSACTION_KEY
    ,SYSTEM_LOT_NUMBER
    ,LOT_TRANSACTION_ID
    ,REPLACE(SUBSTRING(CONVERT_TIMEZONE('America/Phoenix',SYSTEM_TRANSACTION_UDT),1,10),'-')::number AS LOT_TRANSACTION_DATE_KEY
    ,MD5(LOCATION) AS  LOCATION_KEY
    ,MD5(PART_ID ) AS PART_KEY
    ,MD5(STAGE_SET) AS STAGE_SET_KEY
    ,SYSTEM_TRANSACTION_UDT
    ,MES_LOT_NAME
    ,LOT_STATE
    ,BOH_GOOD_WAFER_QUANTITY
    ,EOH_GOOD_WAFER_QUANTITY
    ,GOOD_WAFER_QUANTITY
    ,BOH_REJECT_WAFER_QUANTITY
    ,EOH_REJECT_WAFER_QUANTITY
    ,REJECT_WAFER_QUANTITY
    ,BOH_TOTAL_WAFER_QUANTITY
    ,EOH_TOTAL_WAFER_QUANTITY
    ,BOH_GOOD_DIE_QUANTITY
    ,EOH_GOOD_DIE_QUANTITY
    ,GOOD_DIE_QUANTITY
    ,BOH_REJECT_DIE_QUANTITY
    ,EOH_REJECT_DIE_QUANTITY
    ,REJECT_DIE_QUANTITY
    ,BOH_TOTAL_DIE_QUANTITY
    ,EOH_TOTAL_DIE_QUANTITY
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
    ,{{V_BIW_BATCH_ID}} as BIW_BATCH_ID  
    ,md5(object_construct ('col1',SYSTEM_LOT_NUMBER, 'col2',LOT_TRANSACTION_ID, 'col3',LOT_TRANSACTION_DATE_KEY,
    'col4',LOCATION_KEY, 'col5',PART_KEY, 'col6',STAGE_SET_KEY, 'col7',SYSTEM_TRANSACTION_UDT, 'col8',MES_LOT_NAME, 'col9',LOT_STATE,
    'col10',BOH_GOOD_WAFER_QUANTITY, 'col11',EOH_GOOD_WAFER_QUANTITY, 'col12',GOOD_WAFER_QUANTITY, 'col13',BOH_REJECT_WAFER_QUANTITY,
    'col14',EOH_REJECT_WAFER_QUANTITY, 'col15',REJECT_WAFER_QUANTITY, 'col16',BOH_TOTAL_WAFER_QUANTITY, 'col17',EOH_TOTAL_WAFER_QUANTITY,
    'col18',BOH_GOOD_DIE_QUANTITY, 'col19',EOH_GOOD_DIE_QUANTITY, 'col20',GOOD_DIE_QUANTITY, 'col21',BOH_REJECT_DIE_QUANTITY,
    'col22',EOH_REJECT_DIE_QUANTITY, 'col23',REJECT_DIE_QUANTITY, 'col24',BOH_TOTAL_DIE_QUANTITY, 'col25',EOH_TOTAL_DIE_QUANTITY)::string ) as BIW_MD5_KEY
    FROM 
    {{ref('ETL_MART_E2OPEN_E2O_LOT_TXN_DTL_DRVD')}}
    {% if is_incremental() %}
    WHERE BIW_UPD_DTTM >= '{{V_LWM}}'
    AND BIW_UPD_DTTM < '{{V_HWM}}'
    {% endif %}
)
SELECT 
STG.* 
FROM STG_LOT_DTL STG 
{% if is_incremental() %}
    LEFT JOIN {{ this }} TGT
    on STG.LOT_TRANSACTION_KEY= TGT.LOT_TRANSACTION_KEY
    WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}