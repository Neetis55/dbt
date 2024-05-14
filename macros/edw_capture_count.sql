/*---------------------------------------------------------------------------
Below macro is build:
1. Loop the process to capture the record count impact by model execution

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         March-31-2023      Kali D     Initial Version
---------------------------------------------------------------------------*/



{################# Macro to call count capture macro #################}
{% macro edw_capture_count (models)  %}
    {% if var is iterable  %}
        {% for  model in models %}
            {{edw_capture_count_base (model)}}
        {%- endfor -%}
    {%- else -%} -- check for empty model
        {{edw_capture_count_base (models[0])}}
    {%- endif -%}
{% endmacro %}
