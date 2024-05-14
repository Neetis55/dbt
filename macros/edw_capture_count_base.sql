/*---------------------------------------------------------------------------
Below macro is build to capture the rows impacted because of current execution,

input model name : database.schema.table
output: stores rows impacted into table

Note: currently capturing count on EDL and EDW layer only

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         March-31-2023      Kali D     Initial Version
---------------------------------------------------------------------------*/

{% macro edw_capture_count_base (model)  %}
 {## NO NEED TO RUN BATCH PROCESS FOR DOC GENERATE##}
    {% if execute and flags.WHICH not in ( 'generate','rpc') %}
    {################# Step1 Batch control insert and update SQL #################}
            {#%- set v_dbt_job_name = 'DBT_'~model~'.CAPTURE_COUNT'-%}
            {%- set v_watermark = edw_batch_control(v_dbt_job_name,'NA','NA' ,'NA','NA' ) -%}
            {%- set V_LWM = v_watermark[0] -%}
            {%- set V_HWM = v_watermark[1] -%}
            {%- set V_START_DTTM = v_watermark[2] -%}
            {%- set V_BIW_BATCH_ID = v_watermark[3] -%}
            {%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%#}
            {%- set v_table_name = var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV')+'.'+'UTILITY_DBT.DBT_BIW_MODEL_EXECUTION_LOG' -%}


            {################# Step 2 Create table if not exists #################}
            {%- set v_create_table -%}
                CREATE TABLE IF NOT EXISTS {{v_table_name}}
                (
                TABLE_DATABASE VARCHAR(255) NOT NULL,
                TABLE_SCHEMA VARCHAR(255) NOT NULL,
                TABLE_NAME  VARCHAR(255) NOT NULL,
                MODEL_EXECUTION_DTTM TIMESTAMP_NTZ(6) NOT NULL,
                MODEL_BIW_BATCH_ID NUMBER(38,0)  NOT NULL,
                ROW_IMPACTED NUMBER(38,0),
                BIW_INS_DTTM TIMESTAMP_NTZ(6),
                BIW_UPD_DTTM TIMESTAMP_NTZ(6),
                BIW_BATCH_ID NUMBER(38,0)
                )
            {%endset%}
            {################# Step 3 Get the count and capture into table #################}
            {%- set v_db_name, v_schema_name , v_object_name = (model|string).split('.') -%}
            {################# Check whether model has required columns to identify the count #################}
            {%- set v_pre_column_check %}
                    select 
                        count(*) column_count
                    from
                        {{v_db_name}}.INFORMATION_SCHEMA.COLUMNS C
                        join {{v_db_name}}.INFORMATION_SCHEMA.tables t
                            on c.table_catalog=t.table_catalog
                            and c.table_schema=t.table_schema
                            and c.table_name=t.table_name
                        WHERE t.TABLE_TYPE ='BASE TABLE'
                        AND C.TABLE_SCHEMA =  '{{v_schema_name}}'
                            AND C.TABLE_NAME = '{{v_object_name}}'
                            AND C.COLUMN_NAME IN ('BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY')
            {%endset%}

            {%- set v_row_count_capture %}
                    insert into {{v_table_name}}
                    select
                        '{{v_db_name}}' as TABLE_DATABASE,
                        '{{v_schema_name}}'  as  TABLE_SCHEMA,
                        '{{v_object_name}}' as  TABLE_NAME,
                        max(BIW_UPD_DTTM) AS MODEL_EXECUTION_DTTM,
                        max(BIW_BATCH_ID) AS MODEL_BIW_BATCH_ID ,
                        COUNT(*) AS ROW_IMPACTED ,
                        max(BIW_UPD_DTTM)::TIMESTAMP_NTZ AS BIW_INS_DTTM ,
                        max(BIW_UPD_DTTM)::TIMESTAMP_NTZ AS BIW_UPD_DTTM ,
                        max(BIW_BATCH_ID)::NUMBER(38,0) AS BIW_BATCH_ID
                    from {{model}} m
                    WHERE BIW_BATCH_ID = (SELECT MAX(BIW_BATCH_ID) FROM {{model}} WHERE BIW_MD5_KEY IS NOT NULL)
                    AND BIW_BATCH_ID NOT IN ( SELECT BIW_BATCH_ID FROM {{v_table_name}})
                    group by 1,2,3
            {%endset%}
            {################# Step 4 Execute commands if model is from EDL or EDW Zone#################}
            {%- if ('EDW' in model|upper or 'EDL' in model|upper) -%}
                    {%- set sql_results = run_query(v_pre_column_check) -%}
                    {%- set v_column_count = sql_results.columns[0].values()[0] -%}
                    {%- if v_column_count == 3-%}
                        {% do run_query(v_create_table) %}
                        {% do run_query(v_row_count_capture) %}
                    {% endif %}   
            {% endif %}         
    {% endif %}

{% endmacro %}