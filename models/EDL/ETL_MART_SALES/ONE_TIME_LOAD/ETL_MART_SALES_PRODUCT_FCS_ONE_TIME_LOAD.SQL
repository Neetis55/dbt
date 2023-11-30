--dbt run --select ETL_MART_SALES_PRODUCT_FCS_ONE_TIME_LOAD
--dbt run --full-refresh --select ETL_MART_SALES_PRODUCT_FCS_ONE_TIME_LOAD 
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT_FCS_ONE_TIME_LOAD'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'loading Fairchild one time to product'
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='PRODUCT_FCS_ONE_TIME_LOAD'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

WITH FAIRCHILD AS 
(
    SELECT
         PRODUCT_ID,
         NPD_BUCKET,
         'FAIRCHILD' AS PRODUCT_SOURCE,
         WEB_PART_GROUP,
         CASE WHEN WEB_VISIBLE_FLAG = 1 THEN 'Y' ELSE 'N' END AS WEB_VISIBLE_FLAG
    FROM 
        {{ source('STG_SDM_PRESENTATION', 'DIM_FCSPRODUCTS') }}
)

SELECT 
        md5(PRODUCT_ID) as PRODUCT_KEY,
        PRODUCT_ID,
        PRODUCT_SOURCE,
        NPD_BUCKET,
        coalesce(try_to_boolean(Web_Visible_Flag),'FALSE')::BOOLEAN  AS IS_WEB_VISIBLE,
        NULL AS WEB_PART_NAME,
        WEB_PART_GROUP,
        NULL AS MOQ_QUANTITY,
        NULL AS PART_OID_AK_ID,
        NULL AS CREATED_DATE,
        NULL AS IS_LAUNCH_SALES_TIPS ,
        NULL AS IS_LAUNCH_PRESENTATION_CUSTOMER ,
        NULL AS IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,
        NULL AS IS_DATA_SHEET_AVAILABLE ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
         {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
         md5(object_construct ('col1',PRODUCT_ID::string, 'col2',PRODUCT_SOURCE::string, 'col3',NPD_BUCKET::string--, 'col4',IS_WEB_VISIBLE::string
                        ,'col4', WEB_PART_NAME, 'col5',WEB_PART_GROUP::string, 'col6',MOQ_QUANTITY::string, 'col7',PART_OID_AK_ID::string, 'col8',CREATED_DATE::string)::string ) as BIW_MD5_KEY

 FROM FAIRCHILD
