{#
  Same batch semantics as duckdb__get_incremental_microbatch_sql, but the DELETE runs
  as its own statement because Flight SQL accepts a single statement per round-trip.
#}
{% macro altertable__get_incremental_microbatch_sql(arg_dict) -%}
  {%- set event_time = config.get('event_time') -%}
  {%- if not event_time -%}
    {{ exceptions.raise_compiler_error("microbatch incremental strategy requires an 'event_time' model config") }}
  {%- endif -%}
  {%- if config.get('unique_key') -%}
    {{ exceptions.raise_compiler_error("microbatch incremental strategy does not support 'unique_key'. Microbatch runs delete+insert per batch based on 'event_time'. Remove 'unique_key' or use incremental_strategy='merge'.") }}
  {%- endif -%}

  {%- set batch_ctx = model.get('batch') -%}
  {%- set batch_start = batch_ctx.get('event_time_start') if batch_ctx else none -%}
  {%- set batch_end = batch_ctx.get('event_time_end') if batch_ctx else none -%}
  {%- if not (batch_start and batch_end) -%}
    {{ exceptions.raise_compiler_error("microbatch incremental strategy requires 'batch.event_time_start' and 'batch.event_time_end' to be set in the context") }}
  {%- endif -%}

  {%- set target = arg_dict["target_relation"] -%}
  {%- set source = arg_dict["temp_relation"] -%}
  {%- set incremental_predicates = normalize_incremental_predicates(arg_dict.get("incremental_predicates")) -%}
  {%- set dest_cols_csv = get_quoted_csv(arg_dict["dest_columns"] | map(attribute="name")) -%}

  {%- set batch_predicate -%}
    {{ event_time }} >= '{{ batch_start }}'
    and {{ event_time }} < '{{ batch_end }}'
  {%- endset -%}

  {% call statement("delete_microbatch_rows", auto_begin=true) -%}
    delete from {{ target }}
    where {{ batch_predicate }}
    {%- for predicate in incremental_predicates %}
    and ({{ predicate }})
    {%- endfor %}
  {%- endcall %}

  insert into {{ target }} ({{ dest_cols_csv }})
  select {{ dest_cols_csv }}
  from {{ source }}
  where {{ batch_predicate }}
{%- endmacro %}
