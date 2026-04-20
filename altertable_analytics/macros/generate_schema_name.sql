{#
    Per-target schema naming:
      - prod: uses the custom schema name verbatim (staging, marts_ingestion, ...)
      - dev/ci: prefixes the default schema so each developer/PR gets isolated schemas
    Ref: docs/superpowers/specs/2026-04-20-altertable-analytics-dbt-project-design.md §4.5
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
