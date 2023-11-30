/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT
-- dbt build --full-refresh --select MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022      KALI DANDAPANI     Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['WORKING_CAPITAL_WEEK_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'),'STG.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'-%}
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
         description = 'Building WEEKLY SNAPSHOT table FOR EFK MERGER PROJECT'
        ,transient=false   
        ,materialized='incremental'
        ,schema ='MART_GSCO'
        ,alias='INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'
        ,tags =['GSCO_EFK']
        ,unique_key= v_pk_list
        ,merge_update_columns =['SNAPSHOT_WEEK_KEY', 'TRANSACTION_ID', 'LOT_NUMBER', 'CATEGORY', 'CATEGORY_LEVEL_2',
                                'CATEGORY_LEVEL_3', 'IS_CONSIGNED', 'SUPPLIER', 'TRANSACTION_QUANTITY', 'SUBINVENTORY_CODE', 'INVENTORY_ITEM_ID',
                                'LAST_MOVE_ORDER', 'ONHAND_QUANTITY', 'EXPIRY_DATE', 'LINE_NUMBER', 'UOM', 'DESCRIPTION', 'ORG_CODE', 'PART_NUMBER',
                                'MOVE_ORDER_LINE_ID', 'INVENTORY_AMOUNT', 'DATE_RECEIVED', 'ITEM_COST', 'CONSIGNED_AMOUNT', 'OWNED_AMOUNT',
                                'TRANSACTION_YEAR', 'QTR_OF_YEAR', 'TRANSACTION_MONTH', 'WEEK_OF_YEAR', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    STG.WORKING_CAPITAL_WEEK_KEY, 
    STG.SNAPSHOT_WEEK_KEY, 
    STG.TRANSACTION_ID, 
    STG.LOT_NUMBER, 
    STG.CATEGORY, 
    STG.CATEGORY_LEVEL_2, 
    STG.CATEGORY_LEVEL_3, 
    STG.IS_CONSIGNED, 
    STG.SUPPLIER, 
    STG.TRANSACTION_QUANTITY, 
    STG.SUBINVENTORY_CODE, 
    STG.INVENTORY_ITEM_ID, 
    STG.LAST_MOVE_ORDER, 
    STG.ONHAND_QUANTITY, 
    STG.EXPIRY_DATE, 
    STG.LINE_NUMBER, 
    STG.UOM, 
    STG.DESCRIPTION, 
    STG.ORG_CODE, 
    STG.PART_NUMBER, 
    STG.MOVE_ORDER_LINE_ID, 
    STG.INVENTORY_AMOUNT, 
    STG.DATE_RECEIVED, 
    STG.ITEM_COST, 
    STG.CONSIGNED_AMOUNT,
    STG.OWNED_AMOUNT,
    STG.TRANSACTION_YEAR,
    STG.QTR_OF_YEAR,
    STG.TRANSACTION_MONTH,
    STG.WEEK_OF_YEAR,        
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    STG.BIW_MD5_KEY
FROM 
    {{ref ('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT')}}  STG
   {% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.WORKING_CAPITAL_WEEK_KEY= TGT.WORKING_CAPITAL_WEEK_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}