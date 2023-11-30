{% macro edw_batch_control(param_job_name,param_schema_name,param_table_name,param_tag,param_load_type) %}

    {%- set V_LWM = '' -%}
    {%- set V_HWM = '' -%}
    {%- set V_START_DTTM = '' -%}
        
 {## NO NEED TO RUN BATCH PROCESS FOR DOC GENERATE##}
    {% if execute and flags.WHICH  in ( 'generate','rpc') %}
        {{ return([V_LWM, V_HWM, V_START_DTTM,v_batch_id]) }}
 
    {%- else -%}
        {## Step 1 call the batch start process, which check for process info and return the batch id ##}
        {%- set v_batch_id = edw_batch_control_start(param_job_name,param_schema_name,param_table_name,param_tag,param_load_type)-%}
        {%- call statement('get_edw_watermark', fetch_result=true) %}
            SELECT
                LWM_DTTM,
                HWM_DTTM,
                START_DTTM
            FROM
                {{source('UTILITY', 'EDW_PROCESS_BATCH_CTL')}}  
            WHERE BATCH_ID = {{ v_batch_id }}      
        {%- endcall -%}
        {%- set value_list = load_result('get_edw_watermark') -%}
        {%- set default = [] -%}
        {%- if value_list and value_list['data'] -%}
        -- TODO: could probably find a more elegant way to do this
            {%- set V_LWM = value_list['data'][0][0]|string -%}
            {%- set V_HWM = value_list['data'][0][1]|string -%}
            {%- set V_START_DTTM = value_list['data'][0][2]|string -%}
            {{ return([V_LWM, V_HWM, V_START_DTTM,v_batch_id]) }}
        {%- else -%}
            {{ return(default) }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}