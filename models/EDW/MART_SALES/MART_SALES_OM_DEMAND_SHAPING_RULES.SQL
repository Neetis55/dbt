/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select MART_SALES_OM_DEMAND_SHAPING_RULES
--dbt run --full-refresh --select MART_SALES_OM_DEMAND_SHAPING_RULES

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         09 NOV 2022     VINAY SUBRAMANIAN   Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['DEMAND_SHAPING_RULE_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_OM_DEMAND_SHAPING_RULES') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_OM_DEMAND_SHAPING_RULES') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_OM_DEMAND_SHAPING_RULES'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--SELECT {{v_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_OM_DEMAND_SHAPING_RULES'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table OM_DEMAND_SHAPING_RULES for sales mart '
        ,transient=false
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='OM_DEMAND_SHAPING_RULES'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['RULE_NAME', 'LEVEL_NAME', 'LEVEL_VALUE1', 'RULE_VALUE', 'RULE_ACTIVE_START_DATE',
                                 'RULE_ACTIVE_END_DATE', 'RULE_CREATION_DATETIME', 'RULE_CREATED_BY', 'RULE_LAST_UPDATED_DATETIME', 'RULE_LAST_UPDATED_BY',
                                 'LEVEL_VALUE2', 'LEVEL_VALUE3', 'IS_RULE_ACTIVE', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY'] 

        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

  SELECT
    DEMAND_SHAPING_RULE_KEY,
    RULE_NAME,
    LEVEL_NAME,
    LEVEL_VALUE1,
    RULE_VALUE,
    RULE_ACTIVE_START_DATE,
    RULE_ACTIVE_END_DATE,
    RULE_CREATION_DATETIME,
    RULE_CREATED_BY,
    RULE_LAST_UPDATED_DATETIME,
    RULE_LAST_UPDATED_BY,
    LEVEL_VALUE2,
    LEVEL_VALUE3,
    IS_RULE_ACTIVE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
    FROM {{ref('ETL_MART_SALES_OM_DEMAND_SHAPING_RULES')}}
