{% macro altertable__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}
{% endmacro %}

{# DuckLake does not support DROP ... CASCADE:
   https://ducklake.select/docs/stable/duckdb/unsupported_features.html
   https://github.com/duckdb/ducklake/discussions/1057
   Override the leaf drop macros to omit CASCADE so
   dbt drop-emitting paths stay compatible with DuckLake, including
   adapter.drop_relation (which resolves to drop_table/view/...) and direct
   get_drop_sql calls used by relation replacement and cleanup flows. #}
{% macro altertable__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }}
  {%- endcall -%}
{% endmacro %}

{% macro altertable__drop_table(relation) -%}
  drop table if exists {{ relation.render() }}
{%- endmacro %}

{% macro altertable__drop_view(relation) -%}
  drop view if exists {{ relation.render() }}
{%- endmacro %}

{% macro altertable__drop_materialized_view(relation) -%}
  drop materialized view if exists {{ relation.render() }}
{%- endmacro %}

{% macro altertable__list_schemas(database) -%}
  {% set sql %}
    select schema_name
    from information_schema.schemata
    {% if database is not none %}
    where lower(catalog_name) = '{{ database | lower | trim('"') }}'
    {% endif %}
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro altertable__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from information_schema.schemata
        where lower(schema_name) = '{{ schema | lower }}'
        and lower(catalog_name) = '{{ information_schema.database | lower }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{#
  Flight SQL accepts a single statement per round-trip, so when a table needs more
  than a CREATE TABLE AS (enforced contract, partitioning or sort order),
  the preliminary statements run here and only the final INSERT is returned for the
  materialization's main statement.
#}
{% macro altertable__create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {%- if language != 'sql' -%}
    {% do exceptions.raise_compiler_error(
      "Python models are not supported on Altertable; got language '" ~ language ~ "'"
    ) %}
  {%- endif -%}

  {%- set contract_config = config.get('contract') -%}
  {%- set enforce_contract = contract_config.enforced and not temporary -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(compiled_code) }}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set target = relation.include(database=(not temporary), schema=(not temporary)) -%}

  {%- if not enforce_contract and not partitioned_by and not sorted_by -%}
    {{ sql_header if sql_header is not none }}
    create {% if temporary: -%}temporary{%- endif %} table {{ target }} as (
      {{ compiled_code }}
    );
  {%- else -%}
    {% call statement('create_empty_table', auto_begin=true) -%}
      {{ sql_header if sql_header is not none }}
      {% if enforce_contract -%}
        create table {{ target }} {{ get_table_columns_and_constraints() }}
      {%- else -%}
        create {% if temporary: -%}temporary{%- endif %} table {{ target }} as (
          select * from ({{ compiled_code }}) as model_subq limit 0
        )
      {%- endif %}
    {%- endcall %}
    {% if partitioned_by %}
      {% call statement('set_partitioned_by', auto_begin=true) -%}
        {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
      {%- endcall %}
    {% endif %}
    {% if sorted_by %}
      {% call statement('set_sorted_by', auto_begin=true) -%}
        {{ duckdb__alter_table_set_sorted_by(relation, sorted_by) }}
      {%- endcall %}
    {% endif %}
    {% if enforce_contract -%}
      insert into {{ target }} {{ get_column_names() }} (
        {{ get_select_subquery(compiled_code) }}
      );
    {%- else -%}
      insert into {{ target }} select * from ({{ compiled_code }}) as model_subq;
    {%- endif %}
  {%- endif -%}
{% endmacro %}

{% macro altertable__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          cast(null as integer) as character_maximum_length,
          cast(null as integer) as numeric_precision,
          cast(null as integer) as numeric_scale
      from duckdb_columns()
      where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and lower(schema_name) = lower('{{ relation.schema }}')
      {% endif %}
      {% if relation.database %}
      and lower(database_name) = lower('{{ relation.database }}')
      {% endif %}
      order by column_index
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro altertable__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      table_name as name,
      schema_name as schema,
      'table' as type
    from duckdb_tables()
    where lower(schema_name) = '{{ schema_relation.schema | lower }}'
    and lower(database_name) = '{{ schema_relation.database | lower }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      view_name as name,
      schema_name as schema,
      'view' as type
    from duckdb_views()
    where lower(schema_name) = '{{ schema_relation.schema | lower }}'
    and lower(database_name) = '{{ schema_relation.database | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro altertable__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
{% endmacro %}

{#
  dbt's default__get_delete_insert_merge_sql returns DELETE and INSERT as one
  semicolon-separated string, which Flight SQL rejects because it accepts a single
  statement per round-trip. We execute the DELETE as an auto-begin statement and
  return only the INSERT for the materialization's main statement. Both statements
  stay identical to dbt's and share the materialization transaction.
#}
{% macro altertable__get_incremental_delete_insert_sql(arg_dict) %}
  {%- set target = arg_dict["target_relation"] -%}
  {%- set source = arg_dict["temp_relation"] -%}
  {%- set unique_key = arg_dict["unique_key"] -%}
  {%- set incremental_predicates = arg_dict["incremental_predicates"] -%}
  {%- set dest_cols_csv = get_quoted_csv(arg_dict["dest_columns"] | map(attribute="name")) -%}

  {%- if unique_key -%}
    {%- if unique_key is string -%}
      {%- set unique_key = [unique_key] -%}
    {%- endif -%}
    {%- set unique_key_str = unique_key | join(", ") -%}

    {%- set delete_sql -%}
      delete from {{ target }} as DBT_INTERNAL_DEST
      where ({{ unique_key_str }}) in (
        select distinct {{ unique_key_str }}
        from {{ source }} as DBT_INTERNAL_SOURCE
      )
      {%- for predicate in incremental_predicates or [] %}
      and {{ predicate }}
      {%- endfor %}
    {%- endset -%}
    {% call statement("delete_incremental_rows", auto_begin=true) -%}
      {{ delete_sql }}
    {%- endcall %}
  {%- endif %}

  insert into {{ target }} ({{ dest_cols_csv }})
  (
    select {{ dest_cols_csv }}
    from {{ source }}
  )
{% endmacro %}
