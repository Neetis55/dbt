{% macro edw_process_info_insert(param_job_name,param_schema_name,param_table_name,param_tag,param_load_type) -%}
    {%- set v_sql_get -%} 
        select nvl(count(*),0) row_count
        from {{source('UTILITY', 'EDW_PROCESS_INFO')}} 
        where PROCESS_NAME = '{{param_job_name}}'
    {%endset%}

    {%- set v_subj_area = param_tag|join(',') -%}

    {%- if  'incremental' in param_load_type -%}
            {%- set v_load_type = 'INC' -%}
        {%- else -%}   
            {%- set v_load_type = 'FULL' -%}
    {%- endif -%}

    {% set v_schema_zone = param_schema_name.split('_') %}
    {% set v_zone = v_schema_zone[0] %}

    {%- set v_insert_sql -%} 
        insert into UTILITY.EDW_PROCESS_INFO (
            PROCESS_ID,PROCESS_NAME,SUBJECT_AREA,
            LAYER_NAME,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,
            LOAD_TYPE,EDW_INS_DTTM,EDW_UPD_DTTM)
        SELECT 
            utility.EDW_PROCESS_INFO_SEQ.nextval PROCESS_ID , 
            '{{param_job_name}}' PROCESS_NAME , 
            '{{v_subj_area}}' SUBJECT_AREA ,
            '{{v_zone}}' LAYER_NAME ,
            '{{param_schema_name}}' TARGET_SCHEMA_NAME ,
            '{{param_table_name}}' TARGET_TABLE_NAME,
            '{{v_load_type}}' LOAD_TYPE ,
            CURRENT_TIMESTAMP EDW_INS_DTTM ,
            CURRENT_TIMESTAMP EDW_UPD_DTTM 
    {%endset%}

    {%- if execute -%}
        {%- set results = run_query(v_sql_get) -%}
        {%- set row_count = results.columns[0].values()[0] -%}
    {%- endif -%}

    {%- if row_count==0-%}
        {% do run_query(v_insert_sql) %}
    {%- endif -%}

{% endmacro %}

--select {{edw_process_info_insert('DBT_ETL_MART_E2OPEN_E2O_LOT_DTeL')}}