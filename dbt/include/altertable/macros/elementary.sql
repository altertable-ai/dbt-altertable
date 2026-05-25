{#
  Elementary's `default__get_delete_and_insert_queries` returns a single SQL
  string wrapping `delete` and `insert` in `begin transaction; ...; commit;`.
  Flight SQL's `CommandStatementQuery` is single-statement, so that payload
  fails server-side with `Cannot prepare multiple statements at once!` and
  blocks every dbt run that uses Elementary's `on-run-end` artifacts upload.

  This dispatch override mirrors `duckdb__get_delete_and_insert_queries`:
  emit `delete` and `insert` as separate queries with no transaction wrapping.
  Altertable's `begin/commit/rollback` are no-ops on the connection manager,
  so no explicit `adapter.commit()` is needed afterwards.
#}
{% macro altertable__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            delete from {{ relation }}
            where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}
