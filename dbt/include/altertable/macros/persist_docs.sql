
{#
  The logic in this file is adapted from dbt-postgres, since DuckDB matches
  the Postgres relation/column commenting model as of 0.10.1
#}

{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they do not use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro duckdb_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}

{% macro altertable__alter_relation_comment(relation, comment) -%}
  {% set escaped_comment = duckdb_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }}
{%- endmacro %}


{#
  default__persist_docs assigns this macro's return value to run_query().
  We run each COMMENT ON COLUMN via run_query inside the macro so Flight SQL gets
  one statement per round-trip; the outer run_query then receives an empty string and skips.
#}
{% macro altertable__alter_column_comment(relation, column_dict) -%}
  {# Altertable does not support COMMENT ON COLUMN for views (relation-level COMMENT ON VIEW is supported). #}
  {%- if relation.is_view and column_dict | length > 0 -%}
    {%- do exceptions.raise_compiler_error(
      "The Altertable engine does not support COMMENT ON COLUMN for views (relation-level comments are supported). "
      ~ "Disable column persistence for this model with persist_docs: {columns: false}, "
      ~ "or remove column description entries from its schema YAML. "
      ~ "Relation: " ~ relation.render()
    ) -%}
  {%- endif -%}
  {%- set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list -%}
  {%- set existing_lower_map = {} -%}
  {%- for col in existing_columns -%}
    {% do existing_lower_map.update({col|lower: col}) %}
  {%- endfor -%}
  {%- for column_name in column_dict -%}
    {%- set actual_name = existing_lower_map.get(column_name|lower, none) -%}
    {%- if actual_name is not none -%}
      {%- set col_comment = column_dict[column_name]['description'] -%}
      {%- set escaped_comment = duckdb_escape_comment(col_comment) -%}
      {%- set col_ref = adapter.quote(actual_name) if column_dict[column_name]['quote'] else actual_name -%}
      {%- set _comment_sql -%}
comment on column {{ relation }}.{{ col_ref }} is {{ escaped_comment }}
      {%- endset -%}
      {% do run_query(_comment_sql) %}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}
