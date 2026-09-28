{#- Same checks as dbt-duckdb's validate_ducklake_restrictions, worded for Altertable users. -#}
{%- macro validate_ducklake_restrictions(config, target_relation, errors) -%}
  {%- if config.get('merge_returning_columns') -%}
    {%- do errors.append("Altertable MERGE restrictions: merge_returning_columns is not supported.") -%}
  {%- endif -%}

  {%- set merge_clauses = config.get('merge_clauses') or {} -%}
  {%- set configured_clauses = merge_clauses.get('when_matched', []) + merge_clauses.get('when_not_matched', []) -%}
  {%- set update_or_delete = configured_clauses | selectattr('action', 'defined') | selectattr('action', 'in', ['update', 'delete']) | list -%}
  {%- if update_or_delete | length > 1 -%}
    {%- do errors.append("Altertable MERGE restrictions: merge_clauses can contain only a single UPDATE or DELETE action across when_matched and when_not_matched. Found " ~ update_or_delete | length ~ " UPDATE/DELETE actions.") -%}
  {%- endif -%}
{%- endmacro -%}
