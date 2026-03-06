{% macro coalesce_trim(value, fallback) %}
  coalesce(trim({{ value }}), {{ fallback }})
{% endmacro %}