/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_BACKLOG_CUSTOMER_BRIDGE
--dbt run --full-refresh --select ETL_MART_SALES_BACKLOG_CUSTOMER_BRIDGE

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         09/26/2022      Vinayaka CN     Initial Version
2.0         NOV-01-2022     KALI D          To improve the performance of model, 
                                            looping the model to execute with smaller data set instead of full 5 YEARS
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['BACKLOG_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_BACKLOG_CUSTOMER_BRIDGE'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{%- set v_sql_trunc_stage = " truncate table if exists ETL_MART_SALES.BACKLOG_CUSTOMER_BRIDGE_TBL " -%}
{% if var('is_backfill') %}
        {%- set V_LWM =  var('refresh_start_ts')-%}
{% endif %}

{################# SNOWFLAKE OBJECT CONFIGURATION #################}

{{
    config(
         description = 'Building ETL table BACKLOG_CUSTOMER_BRIDGE for sales mart'
        ,materialized= 'view'
        ,schema ='ETL_MART_SALES'
        ,alias='BACKLOG_CUSTOMER_BRIDGE'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}
{################# Purge ETL table #################}
{%- if execute -%}
    {%- set results = run_query(v_sql_trunc_stage) -%}
{%- endif -%}

{################# GET ROLLING 5 YEARS DAYS COUNT #################}
{%- set v_no_of_days= 0 -%}
{%- set v_no_of_days_per_loop = 20 -%}
{%- set v_buffer_days = 30 -%}
{%- set query -%}
    select datediff(day,TRUNC(CURRENT_DATE,'YEAR')- INTERVAL '5 YEAR' , current_date) as no_of_days
{%- endset -%}
{%- if execute -%}
    {%- set results = run_query(query) -%}
    {%- set v_no_of_days = results.columns[0].values()[0] |int -%}
{%- endif -%}

{################# LOOP THE MACRO TO RUN FOR SMALLER CHUNKS #################}
{% for  no_of_back_date in range (0, v_no_of_days + v_buffer_days , v_no_of_days_per_loop ) %}
    {%-set v_start_date = 'current_date - ' ~ no_of_back_date ~ ' - ' ~ v_no_of_days_per_loop -%}
    {%-set v_end_date = 'current_date - ' ~ no_of_back_date -%}
    {{edw_backlog_bridge_loop_insert_etl(v_start_date,v_end_date, V_LWM, V_HWM , V_START_DTTM ,V_BIW_BATCH_ID  ) }}
 {%- endfor -%}

 {################# WORK AROUND BUILDING VIEW TO FAKE DBT  #################}
 SELECT
    BACKLOG_KEY
    ,SNAPSHOT_DATE_KEY
    ,DIRECT_CUSTOMER_KEY
    ,INDIRECT_CUSTOMER_KEY
    ,DIRECT_CORPORATION_KEY
    ,INDIRECT_CORPORATION_KEY
    ,END_CORPORATION_KEY
    ,ADJUSTED_END_CORPORATION_KEY
    ,REFERENCE_CORPORATION_KEY
    ,DIRECT_CUSTOMER_CODE
    ,DIRECT_CORPORATION_CODE
    ,INDIRECT_CUSTOMER_CODE
    ,INDIRECT_CORPORATION_CODE
    ,END_CORPORATION_CODE
    ,ADJUSTED_END_CORPORATION_CODE
    ,REFERENCE_CORPORATION_CODE
    ,END_CORPORATION_DEFINITION
    ,PROCESS_DATE
    ,BIW_INS_DTTM
    ,BIW_UPD_DTTM
    ,BIW_BATCH_ID
    ,BIW_MD5_KEY
 FROM 
 ETL_MART_SALES.BACKLOG_CUSTOMER_BRIDGE_TBL
