{% macro edw_batch_control_start (param_job_name,param_schema_name,param_table_name,param_tag,param_load_type)  %}

    {{edw_process_info_insert(param_job_name,param_schema_name,param_table_name,param_tag,param_load_type)}}

    {% set query %}
        CALL UTILITY.EDW_BATCH_QUEUE_PROC('{{ param_job_name }}' ,current_timestamp)
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
        CALL UTILITY.EDW_BATCH_RUNNING_PROC('{{ param_job_name }}')
    {% endset %}
    {% do run_query(query) %}

    {%- set query -%}
        SELECT NVL(MAX(BATCH_ID),-1001) 
        FROM     {{source('UTILITY', 'EDW_PROCESS_BATCH_CTL')}}     CTL  
        JOIN     {{source('UTILITY', 'EDW_PROCESS_INFO')}}     INFO 	
        ON CTL.PROCESS_ID = INFO.PROCESS_ID             
        AND INFO.PROCESS_NAME= '{{param_job_name}}'            
        AND CTL.BATCH_STATUS='R' 
    {%- endset -%}

    {# -- Prevent querying of db in parsing mode #}
    {%- if execute -%}
        {%- set results = run_query(query) -%}
        {%- set v_batch_id = results.columns[0].values()[0] -%}
    {%- endif -%}
    
    {{ return(v_batch_id) }}

{% endmacro %}