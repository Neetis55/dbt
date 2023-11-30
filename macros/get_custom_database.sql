{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- set V_EDL_DB = var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV') -%}
    {%- set V_EDW_DB = var('V_EDW_DEFAULT_DB')+env_var('DBT_DEP_ENV') -%}
    {%- set V_ANALYTICS_DB = var('V_ANALYTICS_DEFAULT_DB')+env_var('DBT_DEP_ENV') -%}


    {%- if  'EDL' in node.path|upper -%}
            {%- set default_database = V_EDL_DB -%}
        {%- elif 'EDW' in node.path|upper-%}   
            {%- set default_database = V_EDW_DB -%}
        {%- elif 'ANALYTICS' in node.path|upper-%}
            {%- set default_database = V_ANALYTICS_DB -%}
        {%- else -%}  
            {%- set default_database = target.database -%}
    {%- endif -%}
    {%- if custom_database_name is none -%}
            {{ default_database }}
    {%- else -%}
        {{ custom_database_name | trim }}
    {%- endif -%}

{%- endmacro %}
