--dbt run --full-refresh --select MART_SALES_CORPORATION
--dbt run --select MART_SALES_CORPORATION
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['CORPORATION_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_CORPORATION') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_CORPORATION') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_CORPORATION'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--SELECT {{v_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_CORPORATION'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table CORPORATION dimension for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='CORPORATION'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['CORPORATION_CODE', 'CORPORATION_DESCRIPTION', 'ERP_CUST_ACCOUNT_ID', 'CORPORATION_CATEGORY_CODE'
                            , 'CORPORATION_CATEGORY_DESCRIPTION', 'ERP_STATUS_CODE', 'HOME_REGION', 'IS_MARKET_MAKER', 'CLASS'
                            , 'URL', 'ACCOUNT_TYPE', 'IS_CUSTOMER_ITEM_CONTROL_REQUIRED', 'BARCODE_LABEL_CODE', 'VENDOR_CODE'
                            , 'COLLECTOR_NAME', 'TAX_PRINTING_DESCRIPTION', 'GROUPING_RULE_CODE', 'HAS_CREDIT_CHECK', 'HAS_CREDIT_HOLD'
                            , 'CONTRACT_NAME', 'GAM_USER_ID', 'GAM_FIRST_NAME', 'GAM_LAST_NAME', 'GAM_EMAIL', 'GAM_PHONE'
                            , 'GAM_CONTACT_LAST_UPDATE_BY', 'GAM_CONTACT_CREATED_BY', 'GSM_USER_ID', 'GSM_FIRST_NAME', 'GSM_LAST_NAME'
                            , 'GSM_EMAIL', 'GSM_PHONE', 'GSM_CONTACT_LAST_UPDATE_BY', 'GSM_CONTACT_CREATED_BY'
                            , 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH CORP AS 
(
  select
    md5(CORPORATION_CODE) as CORPORATION_KEY,
    CORPORATION_CODE,
    CORPORATION_DESCRIPTION,
    ERP_CUST_ACCOUNT_ID,
    CORPORATION_CATEGORY_CODE,
    CORPORATION_CATEGORY_DESCRIPTION,
    ERP_STATUS_CODE,
    HOME_REGION,
    IS_MARKET_MAKER,
    CLASS,
    URL,
    ACCOUNT_TYPE,
    IS_CUSTOMER_ITEM_CONTROL_REQUIRED,
    BARCODE_LABEL_CODE,
    VENDOR_CODE,
    COLLECTOR_NAME,
    TAX_PRINTING_DESCRIPTION,
    GROUPING_RULE_CODE,
    HAS_CREDIT_CHECK,
    HAS_CREDIT_HOLD,
    CONTRACT_NAME,
    GAM_USER_ID,
    GAM_FIRST_NAME,
    GAM_LAST_NAME,
    GAM_EMAIL,
    GAM_PHONE,
    GAM_CONTACT_LAST_UPDATE_BY,
    GAM_CONTACT_CREATED_BY,
    GSM_USER_ID,
    GSM_FIRST_NAME,
    GSM_LAST_NAME,
    GSM_EMAIL,
    GSM_PHONE,
    GSM_CONTACT_LAST_UPDATE_BY,
    GSM_CONTACT_CREATED_BY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
    FROM {{ref('ETL_MART_SALES_CORPORATION')}}
)
SELECT 
    STG.CORPORATION_KEY,
    STG.CORPORATION_CODE,
    STG.CORPORATION_DESCRIPTION,
    STG.ERP_CUST_ACCOUNT_ID,
    STG.CORPORATION_CATEGORY_CODE,
    STG.CORPORATION_CATEGORY_DESCRIPTION,
    STG.ERP_STATUS_CODE,
    STG.HOME_REGION,
    STG.IS_MARKET_MAKER,
    STG.CLASS,
    STG.URL,
    STG.ACCOUNT_TYPE,
    STG.IS_CUSTOMER_ITEM_CONTROL_REQUIRED,
    STG.BARCODE_LABEL_CODE,
    STG.VENDOR_CODE,
    STG.COLLECTOR_NAME,
    STG.TAX_PRINTING_DESCRIPTION,
    STG.GROUPING_RULE_CODE,
    STG.HAS_CREDIT_CHECK,
    STG.HAS_CREDIT_HOLD,
    STG.CONTRACT_NAME,
    STG.GAM_USER_ID,
    STG.GAM_FIRST_NAME,
    STG.GAM_LAST_NAME,
    STG.GAM_EMAIL,
    STG.GAM_PHONE,
    STG.GAM_CONTACT_LAST_UPDATE_BY,
    STG.GAM_CONTACT_CREATED_BY,
    STG.GSM_USER_ID,
    STG.GSM_FIRST_NAME,
    STG.GSM_LAST_NAME,
    STG.GSM_EMAIL,
    STG.GSM_PHONE,
    STG.GSM_CONTACT_LAST_UPDATE_BY,
    STG.GSM_CONTACT_CREATED_BY,
    STG.BIW_INS_DTTM,
    STG.BIW_UPD_DTTM,
    STG.BIW_BATCH_ID,
    STG.BIW_MD5_KEY 
    FROM CORP STG
{% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.CORPORATION_KEY= TGT.CORPORATION_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
    {% endif %}