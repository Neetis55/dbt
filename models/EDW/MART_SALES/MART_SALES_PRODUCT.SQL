/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select MART_SALES_PRODUCT
--dbt run --select MART_SALES_PRODUCT
--dbt run --select MART_SALES_PRODUCT --vars 'is_one_time_load: True' 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.1         24 Nov 2022      VINAYAKA CN        Added product attributes like IS_LAUNCH_SALES_TIPS ,IS_LAUNCH_PRESENTATION_CUSTOMER ,IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,IS_DATA_SHEET_AVAILABLE
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_PRODUCT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_PRODUCT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_PRODUCT'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_PRODUCT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}


{{
    config(
         description = 'Building table PRODUCT dimension for Mart Sales'
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='PRODUCT'
        ,unique_key= v_pk_list
        ,merge_update_columns =['PRODUCT_ID','PRODUCT_SOURCE','IS_WEB_VISIBLE','WEB_PART_NAME','WEB_PART_GROUP',
                                 'MOQ_QUANTITY','PART_OID_AK_ID','CREATED_DATE','IS_LAUNCH_SALES_TIPS' ,'IS_LAUNCH_PRESENTATION_CUSTOMER'
                                 ,'IS_LAUNCH_PRESENTATION_DISTRIBUTOR','IS_DATA_SHEET_AVAILABLE','BIW_UPD_DTTM', 'BIW_BATCH_ID','BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}
WITH STG_PRODUCT AS 
(
SELECT 
    md5(PRODUCT_ID) as PRODUCT_KEY,
    PRODUCT_ID, 
    PRODUCT_SOURCE,  
    MOQ_QUANTITY,
    PART_OID_AK_ID,
    CREATED_DATE,
    IS_WEB_VISIBLE,
    WEB_PART_NAME,
    WEB_PART_GROUP,
    IS_LAUNCH_SALES_TIPS ,
    IS_LAUNCH_PRESENTATION_CUSTOMER ,
    IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,
    IS_DATA_SHEET_AVAILABLE ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
{{ref('ETL_MART_SALES_PRODUCT')}}
)
SELECT 
    STG.PRODUCT_KEY,
    STG.PRODUCT_ID, 
    STG.PRODUCT_SOURCE,  
    STG.MOQ_QUANTITY,
    STG.PART_OID_AK_ID,
    STG.CREATED_DATE,
    STG.IS_WEB_VISIBLE,
    STG.WEB_PART_NAME,
    STG.WEB_PART_GROUP, 
    STG.IS_LAUNCH_SALES_TIPS ,
    STG.IS_LAUNCH_PRESENTATION_CUSTOMER ,
    STG.IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,
    STG.IS_DATA_SHEET_AVAILABLE ,
    STG.BIW_INS_DTTM, 
    STG.BIW_UPD_DTTM, 
    STG.BIW_BATCH_ID, 
    STG.BIW_MD5_KEY
FROM STG_PRODUCT STG
{% if is_incremental() %}
LEFT JOIN {{ this }} TGT
  on STG.PRODUCT_KEY= TGT.PRODUCT_KEY
WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY 
      OR TGT.BIW_MD5_KEY IS NULL
{% endif %}
{% if not is_incremental() or var('is_one_time_load') %} 
    UNION ALL
	    SELECT 
        PRODUCT_KEY, 
        PRODUCT_ID, 
        PRODUCT_SOURCE, 
        MOQ_QUANTITY, 
        PART_OID_AK_ID, 
        CREATED_DATE, 
        IS_WEB_VISIBLE, 
        WEB_PART_NAME,
        WEB_PART_GROUP,
        IS_LAUNCH_SALES_TIPS ,
        IS_LAUNCH_PRESENTATION_CUSTOMER ,
        IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,
        IS_DATA_SHEET_AVAILABLE ,
       '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
       '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
        {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID, 
        BIW_MD5_KEY
        FROM 
    {{ref('ETL_MART_SALES_PRODUCT_FCS_ONE_TIME_LOAD')}}
    where PRODUCT_KEY NOT IN (SELECT PRODUCT_KEY FROM STG_PRODUCT) 
{% endif %}