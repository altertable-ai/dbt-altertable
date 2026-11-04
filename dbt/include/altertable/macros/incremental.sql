{#
  dbt-duckdb's incremental materialization, for dbt runs that share one long-lived Altertable
  session:
  - The temp relation is always dropped: a temporary table would otherwise hold the model's
    whole result in the session until the run ends, once per model and per microbatch batch.
  - SQL models stage a temporary view when the strategy reads the staged rows in a single
    statement, so the model streams into the target; see altertable__incremental_tmp_relation_type.
#}
{% materialization incremental, adapter="altertable", supported_languages=['sql', 'python'] -%}

  {%- set language = model['language'] -%}
  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_strategy = config.get('incremental_strategy') or 'default' -%}
  {%- set partitioned_by = none -%}
  {%- set sorted_by = none -%}
  {%- if existing_relation is none or full_refresh_mode -%}
    {%- set partitioned_by = duckdb__get_partitioned_by(target_relation, false) -%}
    {%- set sorted_by = duckdb__get_sorted_by(target_relation, false) -%}
  {%- endif -%}
  {%- set skip_auto_begin = (partitioned_by or sorted_by) and adapter.is_ducklake(target_relation) -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% set to_drop = [] %}
  {%- set tmp_relation_type = altertable__incremental_tmp_relation_type(
      incremental_strategy, unique_key, language, target_relation, compiled_code) -%}
  {%- set stage_as_view = tmp_relation_type == 'view' -%}
  {%- set temp_relation = make_temp_relation(target_relation).incorporate(type=tmp_relation_type) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, intermediate_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
    {% set need_swap = true %}
  {% else %}
    {% if language == 'python' %}
      {% set build_python = create_table_as(True, temp_relation, compiled_code, language, partitioned_by=none, sorted_by=none) %}
      {% call statement("pre", language=language) %}
        {{- build_python }}
      {% endcall %}
    {% elif stage_as_view %}
      {% do run_query(altertable__create_temporary_view_as(temp_relation, compiled_code)) %}
    {% else %} {# SQL #}
      {% do run_query(create_table_as(True, temp_relation, compiled_code, language, partitioned_by=none, sorted_by=none)) %}
    {% endif %}
    {% do to_drop.append(temp_relation) %}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}
    {% set language = "sql" %}

  {% endif %}

  {% call statement("main", language=language, auto_begin=not skip_auto_begin) %}
      {{- build_sql }}
  {% endcall %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{#
  Same choice as dbt-snowflake's tmp_relation_type: a view is evaluated by every statement that
  reads it, so it is only the default when the strategy reads the staged rows once.
  - append, merge, and delete+insert without a unique_key run a single INSERT or MERGE.
  - delete+insert with a unique_key reads the rows for its DELETE and again for its INSERT.
  - microbatch deletes the batch before its INSERT, which only changes the rows of a model
    naming its own table.
  The tmp_relation_type config overrides the choice, except where a view would change results.
#}
{% macro altertable__incremental_tmp_relation_type(strategy, unique_key, language, target_relation, compiled_code) -%}
  {%- set tmp_relation_type = config.get('tmp_relation_type') -%}
  {%- if tmp_relation_type not in [none, 'table', 'view'] -%}
    {% do exceptions.raise_compiler_error(
      "tmp_relation_type must be 'table' or 'view', but '" ~ tmp_relation_type ~ "' was specified."
    ) %}
  {%- endif -%}

  {%- if language != 'sql' -%}
    {%- if tmp_relation_type == 'view' -%}
      {% do exceptions.raise_compiler_error(
        "Python models only support 'table' for tmp_relation_type."
      ) %}
    {%- endif -%}
    {{ return('table') }}
  {%- endif -%}

  {%- set deletes_by_key = strategy in ['default', 'delete+insert'] and unique_key -%}
  {%- if deletes_by_key and tmp_relation_type == 'view' -%}
    {% do exceptions.raise_compiler_error(
      "The `" ~ strategy ~ "` strategy with a `unique_key` only supports 'table' for "
      ~ "tmp_relation_type, because its DELETE and INSERT must read the same rows."
    ) %}
  {%- endif -%}

  {%- if tmp_relation_type is not none -%}
    {{ return(tmp_relation_type) }}
  {%- elif deletes_by_key -%}
    {{ return('table') }}
  {%- elif strategy == 'microbatch' -%}
    {{ return('table' if target_relation.identifier | lower in compiled_code | lower else 'view') }}
  {%- elif strategy in ['append', 'merge', 'default', 'delete+insert'] -%}
    {{ return('view') }}
  {%- else -%}
    {{ return('table') }}
  {%- endif -%}
{%- endmacro %}

{% macro altertable__create_temporary_view_as(relation, compiled_code) -%}
  {%- if config.get('contract').enforced -%}
    {{ get_assert_columns_equivalent(compiled_code) }}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}
  create temporary view {{ relation }} as (
    {{ compiled_code }}
  )
{%- endmacro %}
