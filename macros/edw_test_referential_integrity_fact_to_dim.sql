/*---------------------------------------------------------------------------
Below macro is build:
1. To identify whether we have any FK values in the fact table and missing in the dimension table
2. Capture the missing value in the audit table 
3. Insert the misisng PK value in the dimension table

Currently severity of the test is set to warning as we are doing insert into the dimension.

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         10/18/2022      Kali D     Initial Version
---------------------------------------------------------------------------*/

{% test edw_test_referential_integrity_fact_to_dim (model,column_name,param_source_table_name,param_source_column_name,param_source_column_hash_name,param_target_condition="1=1", param_source_condition="1=1")  %}
 {## NO NEED TO RUN BATCH PROCESS FOR DOC GENERATE##}
{% if execute and flags.WHICH not in ( 'generate','rpc') %}

  {################# Step1 Batch control insert and update SQL #################}
  {%- set v_dbt_job_name = 'DBT_'~model~'.'~column_name~'_RI_CHECK_AGAINST_'~param_source_table_name-%}
  {%- set v_watermark = edw_batch_control(v_dbt_job_name,'NA','NA' ,'NA','NA' ) -%}
  {%- set V_LWM = v_watermark[0] -%}
  {%- set V_HWM = v_watermark[1] -%}
  {%- set V_START_DTTM = v_watermark[2] -%}
  {%- set V_BIW_BATCH_ID = v_watermark[3] -%}
  {%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
  {%- set v_table_name = var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV')+'.'+'UTILITY_DBT.DBT_REFERENTIAL_INTEGRITY_CHECK' -%}

  {{ config(severity = 'warn') }}

  {################# Step 2 Create table if not exists #################}
  {%- set v_create_table -%} 
      CREATE TABLE IF NOT EXISTS {{v_table_name}}
      (
      TARGET_TABLE_MODEL_NAME VARCHAR(255) NOT NULL,
      TARGET_TABLE_COLUMN_NAME VARCHAR(255) NOT NULL,
      SOURCE_TABLE_MODEL_NAME VARCHAR(255) NOT NULL,
      SOURCE_TABLE_COLUMN_NAME VARCHAR(255) NOT NULL,
      TARGET_TABLE_VALUE STRING NOT NULL,
      BIW_INS_DTTM	TIMESTAMP_NTZ(6),
      BIW_UPD_DTTM	TIMESTAMP_NTZ(6),
      BIW_BATCH_ID	NUMBER(38,0)
      )
  {%endset%}
    {################# Overriding the where clause #################}
  {% if var('is_full_ri_test') %}
        {%- set where_condition = "1=1"  -%}
    {% else %}
        {%- set where_condition = param_target_condition -%}
  {% endif %}
  {################# Step 3 Identify missing value and capture in the table #################}
  {%- set v_identify_missing_fk %}
  begin ;
  insert into {{v_table_name}}
  with left_table as (
    select
      {{column_name}} as id
    from {{model}}
    where {{column_name}} is not null
      and {{where_condition}}
  ),
  right_table as (
    select
      {{param_source_column_name}} as id
    from {{param_source_table_name}}
    where {{param_source_column_name}} is not null
  ),
  exceptions as (
    select distinct
      '{{model}}' as TARGET_TABLE_MODEL_NAME,
      '{{column_name}}' as TARGET_TABLE_COLUMN_NAME,
      '{{param_source_table_name}}' as  SOURCE_TABLE_MODEL_NAME ,
      '{{param_source_column_name}}' as  SOURCE_TABLE_COLUMN_NAME ,
      left_table.id as TARGET_TABLE_VALUE,
      '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
      '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
      {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID
    from left_table
    left join right_table
          on left_table.id = right_table.id
    where right_table.id is null
  )
  select * from exceptions;

  {%endset%}
  {################# Step 4 Insert missing value into source table #################}
  {%- set v_insert_missing_fk %}
  merge into {{param_source_table_name}}  TGT
  using 
      (
      select distinct
          TARGET_TABLE_VALUE, 
          md5(TARGET_TABLE_VALUE) AS HASH_KEY,
          BIW_INS_DTTM, 
          BIW_UPD_DTTM, 
          BIW_BATCH_ID
      from {{v_table_name}}
          where TARGET_TABLE_MODEL_NAME= '{{model}}'
          and   TARGET_TABLE_COLUMN_NAME ='{{column_name}}'
          and SOURCE_TABLE_MODEL_NAME ='{{param_source_table_name}}'
          and SOURCE_TABLE_COLUMN_NAME ='{{param_source_column_name}}'
          and BIW_BATCH_ID = {{V_BIW_BATCH_ID}}
          and TARGET_TABLE_VALUE not in ( select {{param_source_column_name}} from {{param_source_table_name}}  )
      ) STG
       ON TGT.{{param_source_column_name}}  = STG. TARGET_TABLE_VALUE
       WHEN NOT MATCHED THEN 
       INSERT ( 
            {{param_source_column_name}},
            {{param_source_column_hash_name}}, 
            BIW_INS_DTTM, 
            BIW_UPD_DTTM, 
            BIW_BATCH_ID
            )
       VALUES (   
          STG.TARGET_TABLE_VALUE, 
          STG.HASH_KEY,
          STG.BIW_INS_DTTM, 
          STG.BIW_UPD_DTTM, 
          STG.BIW_BATCH_ID)
        ;
  commit;
  {%endset%}


  {% do run_query(v_create_table) %}
  {% do run_query(v_identify_missing_fk) %}
  {% do run_query(v_insert_missing_fk) %}
  {% do run_query(v_sql_upd_success_batch) %}
  select TARGET_TABLE_MODEL_NAME
  from {{v_table_name}}
      where TARGET_TABLE_MODEL_NAME= '{{model}}'
      and   TARGET_TABLE_COLUMN_NAME ='{{column_name}}'
      and SOURCE_TABLE_MODEL_NAME ='{{param_source_table_name}}'
      and SOURCE_TABLE_COLUMN_NAME ='{{param_source_column_name}}'
      and BIW_BATCH_ID = {{V_BIW_BATCH_ID}}
{% endif %}
{% endtest %}
