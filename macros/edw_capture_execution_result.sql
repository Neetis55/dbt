{################# Macro to take run end results and prase to get macro #################}
{% macro edw_capture_execution_result(results) -%}
    {% set m_models = [] %}
    {% for result in results  %}
        {% if result.node.resource_type == "model" %}
            {% do m_models.append(result.node.database~'.'~result.node.schema~'.'~result.node.alias) %}
        {% endif %}
    {% endfor %}
    {% if m_models|length > 1  %}
        {{edw_capture_count (m_models)}}
    {% endif %}
    ---Update batch control on failed jobs
    {{edw_update_batch_control_on_failure()}}
   -- dummy return
    {{ return('select 1 as dummy') }}
{% endmacro %}