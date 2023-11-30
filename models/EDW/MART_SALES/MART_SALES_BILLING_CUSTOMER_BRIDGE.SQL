/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select MART_SALES_BILLING_CUSTOMER_BRIDGE
--dbt run --select MART_SALES_BILLING_CUSTOMER_BRIDGE

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         10/18/2022      Sruthi Kasbe    Updated customer columns and its order
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['BILLING_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_BILLING_CUSTOMER_BRIDGE') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_BILLING_CUSTOMER_BRIDGE') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}

--DBT Variable
--SELECT {{v_md5_column_list}}
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_BILLING_CUSTOMER_BRIDGE'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table BILLING_CUSTOMER_BRIDGE for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='BILLING_CUSTOMER_BRIDGE'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,merge_update_columns = ['DIRECT_CUSTOMER_KEY', 'INDIRECT_CUSTOMER_KEY', 'END_CUSTOMER_KEY', 'DIRECT_CORPORATION_KEY','INDIRECT_CORPORATION_KEY','END_CORPORATION_KEY'
                                ,'DIRECT_CUSTOMER_CODE','INDIRECT_CUSTOMER_CODE', 'END_CUSTOMER_CODE', 'DIRECT_CORPORATION_CODE','INDIRECT_CORPORATION_CODE','END_CORPORATION_CODE'
                                , 'ADJUSTED_END_CORPORATION_KEY','REFERENCE_CORPORATION_KEY' ,'ADJUSTED_END_CORPORATION_CODE', 'REFERENCE_CORPORATION_CODE'
                                ,'END_CORPORATION_DEFINITION', 'PROCESS_DATE','BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

select
    BILLING_KEY,
    BILLING_DATE_KEY,
    PROCESS_DATE,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,
    END_CUSTOMER_KEY,
    DIRECT_CORPORATION_KEY,
    INDIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,
    ADJUSTED_END_CORPORATION_KEY,
    REFERENCE_CORPORATION_KEY,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,
    END_CUSTOMER_CODE,
    DIRECT_CORPORATION_CODE,
    INDIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    ADJUSTED_END_CORPORATION_CODE,
    REFERENCE_CORPORATION_CODE,
    END_CORPORATION_DEFINITION,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
	'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
	{{V_BIW_BATCH_ID}} as BIW_BATCH_ID   ,
    BIW_MD5_KEY 
FROM {{ref('ETL_MART_SALES_BILLING_CUSTOMER_BRIDGE')}} 
