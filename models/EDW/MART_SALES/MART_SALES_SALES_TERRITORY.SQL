--dbt run --full-refresh --select MART_SALES_SALES_TERRITORY
--dbt run --select MART_SALES_SALES_TERRITORY
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SALES_TERRITORY_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_SALES_TERRITORY') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_SALES_TERRITORY') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_SALES_TERRITORY'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--SELECT {{v_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_SALES_TERRITORY'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{{
    config(
         description = 'Building table SALES_TERRITORY dimension for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='SALES_TERRITORY'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['SALES_ORGANIZATION_CODE', 'SALES_ORGANIZATION_DESCRIPTION', 'SALES_REP_FIRM_NAME', 'SALES_OFFICE_CODE', 'SALES_OFFICE_NAME'
                            , 'SALES_AREA_NAME', 'SALES_SUB_AREA_NAME', 'SALES_REGION_CODE', 'SALES_REGION_NAME'
                            , 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}


WITH SALES_DATA as
( select 	 
    md5(SALES_ORGANIZATION_CODE) AS SALES_TERRITORY_KEY,
	  SALES_ORGANIZATION_CODE,
    SALES_ORGANIZATION_DESCRIPTION,
    SALES_REP_FIRM_NAME,
    SALES_OFFICE_CODE,
    SALES_OFFICE_NAME,
    SALES_AREA_NAME,
    SALES_SUB_AREA_NAME,
    SALES_REGION_CODE,
    SALES_REGION_NAME,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY
    from	
{{ ref('ETL_MART_SALES_SALES_TERRITORY') }}
  )
  select
    STG.SALES_TERRITORY_KEY,
    STG.SALES_ORGANIZATION_CODE,
    STG.SALES_ORGANIZATION_DESCRIPTION,
    STG.SALES_REP_FIRM_NAME,
    STG.SALES_OFFICE_CODE,
    STG.SALES_OFFICE_NAME,
    STG.SALES_AREA_NAME,
    STG.SALES_SUB_AREA_NAME,
    STG.SALES_REGION_CODE,
    STG.SALES_REGION_NAME,
    STG.BIW_INS_DTTM ,
    STG.BIW_UPD_DTTM ,
    STG.BIW_BATCH_ID,  
    STG.BIW_MD5_KEY
  from
  SALES_DATA STG
   {% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.SALES_TERRITORY_KEY= TGT.SALES_TERRITORY_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
    {% endif %}