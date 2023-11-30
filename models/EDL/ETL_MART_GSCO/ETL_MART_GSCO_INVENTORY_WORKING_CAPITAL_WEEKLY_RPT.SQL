/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT
-- dbt build --full-refresh --select ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT+

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022      KALI DANDAPANI     Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['WORKING_CAPITAL_WEEK_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'-%}
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
         description = 'Building ETL table INVENTORY_WORKING_CAPITAL_WEEKLY_RPT for EFK MERGER PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_GSCO'
        ,alias='INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'
        ,tags =['GSCO_EFK']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH FISCAL_WEEK AS 
(
    SELECT 
        DISTINCT FISCAL_WEEK_KEY
    FROM 
    {{source('MART','DATE')}}
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() )::DATE
)

,WORKING_CAPITAL AS (
    SELECT 
        TRANSACTION_TEMP_ID AS TRANSACTION_ID
        ,LOT_NUMBER
        ,CATEGORY
        ,CATEGORY_LEVEL_2
        ,CATEGORY_LEVEL_3
        ,CONSIGNED_FLAG::BOOLEAN AS IS_CONSIGNED
        ,SUPPLIER
        ,TRANSACTION_QUANTITY
        ,SUBINVENTORY_CODE
        ,INVENTORY_ITEM_ID
        ,LAST_MOVE_ORDER
        ,ONHAND_QUANTITY
        ,EXPIRY_DATE
        ,LINE_NUMBER
        ,UOM
        ,DESCRIPTION
        ,ORG_CODE
        ,PART_NUMBER
        ,MOVE_ORDER_LINE_ID
        ,INVENTORY_AMOUNT
        ,DATE_RECEIVED
        ,ITEM_COST
        ,CONSIGNED_AMOUNT
        ,OWNED_AMOUNT
        ,TRX_YEAR AS TRANSACTION_YEAR
        ,QTR_OF_YEAR
        ,TRX_MONTH AS TRANSACTION_MONTH
        ,WEEK_OF_YEAR
    FROM 
    {{source ('STG_EBS_APPS','XXON_INV_WORKING_CAPITAL_V')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID,LOT_NUMBER ORDER BY BIW_UPD_DTTM DESC) =1)
)
SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',TRANSACTION_ID::STRING
                            ,'COL3',LOT_NUMBER::STRING
                         )::STRING 
        )::BINARY AS WORKING_CAPITAL_WEEK_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
    TRANSACTION_ID
    ,LOT_NUMBER
    ,CATEGORY
    ,CATEGORY_LEVEL_2
    ,CATEGORY_LEVEL_3
    ,IS_CONSIGNED
    ,SUPPLIER
    ,TRANSACTION_QUANTITY
    ,SUBINVENTORY_CODE
    ,INVENTORY_ITEM_ID
    ,LAST_MOVE_ORDER
    ,ONHAND_QUANTITY
    ,EXPIRY_DATE
    ,LINE_NUMBER
    ,UOM
    ,DESCRIPTION
    ,ORG_CODE
    ,PART_NUMBER
    ,MOVE_ORDER_LINE_ID
    ,INVENTORY_AMOUNT
    ,DATE_RECEIVED
    ,ITEM_COST
    ,CONSIGNED_AMOUNT
    ,OWNED_AMOUNT
    ,TRANSACTION_YEAR
    ,QTR_OF_YEAR
    ,TRANSACTION_MONTH
    ,WEEK_OF_YEAR    
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
    ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID
    ,md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',TRANSACTION_ID::string,
    'col3',LOT_NUMBER::string, 'col4',CATEGORY::string, 'col5',CATEGORY_LEVEL_2::string, 'col6',CATEGORY_LEVEL_3::string,
    'col7',IS_CONSIGNED::string, 'col8',SUPPLIER::string, 'col9',TRANSACTION_QUANTITY::string, 'col10',SUBINVENTORY_CODE::string,
    'col11',INVENTORY_ITEM_ID::string, 'col12',LAST_MOVE_ORDER::string, 'col13',ONHAND_QUANTITY::string, 'col14',EXPIRY_DATE::string,
    'col15',LINE_NUMBER::string, 'col16',UOM::string, 'col17',DESCRIPTION::string, 'col18',ORG_CODE::string,
    'col19',PART_NUMBER::string, 'col20',MOVE_ORDER_LINE_ID::string, 'col21',INVENTORY_AMOUNT::string,
    'col22',DATE_RECEIVED::string, 'col23',ITEM_COST::string, 'col24',CONSIGNED_AMOUNT::string, 'col25',OWNED_AMOUNT::string,
    'col26',TRANSACTION_YEAR::string, 'col27',QTR_OF_YEAR::string, 'col28',TRANSACTION_MONTH::string, 'col29',WEEK_OF_YEAR::string)::string
    )::BINARY as BIW_MD5_KEY   
FROM     WORKING_CAPITAL  
    CROSS JOIN FISCAL_WEEK FSC_WK