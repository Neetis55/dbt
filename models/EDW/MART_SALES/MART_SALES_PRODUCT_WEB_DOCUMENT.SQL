/*---------------------------------------------------------------------------
Command to run model:
--dbt build --full-refresh --select MART_SALES_PRODUCT_WEB_DOCUMENT
--dbt build --select MART_SALES_PRODUCT_WEB_DOCUMENT
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         06 Dec 2022      VINAYAKA CN     INITIAL VERSION
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_WEB_DOCUMENT_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_PRODUCT_WEB_DOCUMENT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_PRODUCT_WEB_DOCUMENT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_WEB_DOCUMENT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table PRODUCT_WEB_DOCUMENT dimension for SALES Mart'
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_WEB_DOCUMENT'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['PRODUCT_ID','BASE_PART','DOCUMENT_NAME','DOCUMENT_FILE_NAME','DOCUMENT_TYPE','BIW_UPD_DTTM', 'BIW_BATCH_ID','BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

WITH STG_PRODUCT_WEB_DOCUMENT AS (
    SELECT 
        PRODUCT_WEB_DOCUMENT_KEY,
        PRODUCT_ID ,
        BASE_PART ,
        DOCUMENT_NAME ,
        DOCUMENT_FILE_NAME ,
        DOCUMENT_TYPE ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
        {{V_BIW_BATCH_ID}}	 AS BIW_BATCH_ID,
        BIW_MD5_KEY
    FROM 
        {{ref('ETL_MART_SALES_PRODUCT_WEB_DOCUMENT')}}
)

SELECT 
    STG.PRODUCT_WEB_DOCUMENT_KEY,
    STG.PRODUCT_ID ,
    STG.BASE_PART ,
    STG.DOCUMENT_NAME ,
    STG.DOCUMENT_FILE_NAME ,
    STG.DOCUMENT_TYPE ,
    STG.BIW_INS_DTTM ,
    STG.BIW_UPD_DTTM ,
    STG.BIW_BATCH_ID,
    STG.BIW_MD5_KEY
FROM 
    STG_PRODUCT_WEB_DOCUMENT STG
    {% if is_incremental() %}
        LEFT JOIN {{ this }} TGT
            on STG.PRODUCT_WEB_DOCUMENT_KEY= TGT.PRODUCT_WEB_DOCUMENT_KEY
        WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
    {% endif %}      