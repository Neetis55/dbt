{% macro edw_get_column_list_with_prefix(param_object_name,param_prefix) -%}
        {%- set column_list_quoted = [] -%}
        {%- set table_desc =  get_columns_in_relation(param_object_name) -%}
        
        {% for column in table_desc %}
                {{ column_list_quoted.append(param_prefix+column.name) }}
        {% endfor %}
        {%- set dest_cols_list = column_list_quoted | join(', ') -%}
    {{ return(dest_cols_list) }}
{% endmacro %}
