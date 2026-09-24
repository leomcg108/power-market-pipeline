{#
    Schema names per target:
    - prod uses the custom schema as is, for example power_staging.
    - Every other target adds its name as a suffix, for example power_staging_dev,
      so dev and CI runs never touch prod tables.
    Models without a custom schema fall back to the target's default schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}_{{ target.name }}
    {%- endif -%}
{%- endmacro %}
