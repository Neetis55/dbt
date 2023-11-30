--dbt run --full-refresh --select MART_SALES_PRODUCT_NPD_BUCKET_REF
--dbt run --select MART_SALES_PRODUCT_NPD_BUCKET_REF
/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select MART_SALES_PRODUCT_NPD_BUCKET_REF
--dbt build --select MART_SALES_PRODUCT_NPD_BUCKET_REF+

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         10/14/2022      Kali D           Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list =  ['NPD_BUCKET' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT_NPD_BUCKET_REF'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{{
    config(
         description = 'Reference table to capture NPD Bucket List'
        ,transient=false
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='PRODUCT_NPD_BUCKET_REF'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,post_hook= [ v_sql_upd_success_batch]
        )
}}
--- List of Buckets are defined in dbt code
{% set NPD_BUCKET1 = [["New Request" , "5 years old and less"],["0 to 3 years" , "5 years old and less"],["3 to 5 years" , "5 years old and less"],["5 to 7 years" , "Greater than 5 years but less than 10 years"] ,["7 to 10 years" , "Greater than 5 years but less than 10 years"] ,["Greater than 10 years" , "Greater than 10 years old"]] %}

WITH NPD_BUCKET AS 
(
    {% for V_LIST in NPD_BUCKET1 %}
    SELECT '{{V_LIST[0]}}' AS NPD_BUCKET, '{{V_LIST[1]}}' NPD_GROUP
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)
SELECT 
    NPD_BUCKET,
    NPD_GROUP,
    current_user AS   BIW_CREATED_BY ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_CREATED_DTTM ,
    current_user AS  BIW_UPD_BY ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
FROM NPD_BUCKET