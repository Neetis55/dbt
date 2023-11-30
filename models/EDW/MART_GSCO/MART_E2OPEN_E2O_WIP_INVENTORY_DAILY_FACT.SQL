--dbt run --full-refresh --select MART_E2OPEN_E2O_WIP_INVENTORY_DAILY_FACT
--dbt run --select MART_E2OPEN_E2O_WIP_INVENTORY_DAILY_FACT
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SYSTEM_LOT_NUMBER','SNAPSHOT_DATE_KEY' ]-%}
{###%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%###}
{###%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%###}
{###%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%###}
--DBT Variable
--SELECT {{v_update_column_list}}
--select {{v_md5_column_list}}


{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_E2OPEN_E2O_WIP_INVENTORY_DAILY_FACT'-%}
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
        ,alias='E2O_WIP_INVENTORY_DAILY_FACT'
        ,unique_key= v_pk_list
    	,merge_update_columns =  ['LOT_KEY', 'LOCATION_KEY', 'PART_KEY', 'STAGE_SET_KEY', 'TOTAL_WAFER_QUANTITY',
                                'GOOD_WAFER_QUANTITY', 'REJECT_WAFER_QUANTITY', 'TOTAL_DIE_QUANTITY', 'GOOD_DIE_QUANTITY', 'REJECT_DIE_QUANTITY',
                                'LOT_HOLD_TRANSACTION_TIME_AGING_DAY', 'AGING_DAY', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
		,tags =['E2OPEN']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH STG_DAILY_LOT AS 
(SELECT
    SYSTEM_LOT_NUMBER,
    REPLACE(SUBSTRING(CURRENT_DATE,1,10),'-')::number SNAPSHOT_DATE_KEY,
    MD5(SYSTEM_LOT_NUMBER) AS  LOT_KEY,
    MD5(LOCATION) AS LOCATION_KEY,
    MD5(PART_ID) AS PART_KEY,
    MD5(STAGE_SET) AS STAGE_SET_KEY,
    EOH_TOTAL_WAFER_QUANTITY  AS TOTAL_WAFER_QUANTITY,
    EOH_GOOD_WAFER_QUANTITY AS GOOD_WAFER_QUANTITY,
    EOH_REJECT_WAFER_QUANTITY AS REJECT_WAFER_QUANTITY,
    EOH_TOTAL_DIE_QUANTITY AS TOTAL_DIE_QUANTITY,
    EOH_GOOD_DIE_QUANTITY AS GOOD_DIE_QUANTITY,
    EOH_REJECT_DIE_QUANTITY AS REJECT_DIE_QUANTITY,
    DATEDIFF(DAY,convert_timezone('America/Phoenix', LOT_HOLD_TRANSACTION_UDT),CURRENT_DATE) AS LOT_HOLD_TRANSACTION_TIME_AGING_DAY,
    DATEDIFF(DAY,convert_timezone('America/Phoenix', MOST_RECENT_MOVE_TRANSACTION_UDT),CURRENT_DATE) AS AGING_DAY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID   ,
    md5(object_construct ('col1',LOT_KEY, 'col2',LOCATION_KEY, 'col3',PART_KEY, 'col4',STAGE_SET_KEY,
    'col5',TOTAL_WAFER_QUANTITY, 'col6',GOOD_WAFER_QUANTITY, 'col7',REJECT_WAFER_QUANTITY, 'col8',TOTAL_DIE_QUANTITY,
    'col9',GOOD_DIE_QUANTITY, 'col10',REJECT_DIE_QUANTITY, 'col11',LOT_HOLD_TRANSACTION_TIME_AGING_DAY,
    'col12',AGING_DAY)::string ) as BIW_MD5_KEY
FROM {{ref('ETL_MART_E2OPEN_E2O_LOT_DTL')}}
WHERE IS_ACTIVE
)
SELECT 
    STG.*
 FROM STG_DAILY_LOT STG
{% if is_incremental() %}
    LEFT JOIN {{ this }} TGT
    on STG.SYSTEM_LOT_NUMBER= TGT.SYSTEM_LOT_NUMBER
    AND STG.SNAPSHOT_DATE_KEY = TGT.SNAPSHOT_DATE_KEY
    WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}
