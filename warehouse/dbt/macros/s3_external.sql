{% macro s3_raw_path(source_name) -%}
  's3://{{ env_var("LAKE_BUCKET") }}/raw/{{ source_name }}/'
{%- endmacro %}
