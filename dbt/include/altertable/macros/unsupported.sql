{#
  Features Altertable cannot provide. They fail loudly instead of
  silently producing a model that differs from what the project asked for.
#}

{% macro altertable__apply_grants(relation, grant_config, should_revoke=True) %}
  {% if grant_config %}
    {% do exceptions.raise_compiler_error(
      "Grants are not supported on Altertable; remove the `grants` config from " ~ relation
    ) %}
  {% endif %}
{% endmacro %}

{% macro altertable__get_create_index_sql(relation, index_dict) -%}
  {% do adapter.parse_index(index_dict) %}
{%- endmacro %}

{#- Called on every table rebuild; Altertable tables never have indexes to drop. -#}
{% macro drop_indexes_on_relation(relation) -%}
{%- endmacro %}

{% materialization external, adapter='altertable' %}
  {% do exceptions.raise_compiler_error(
    "The 'external' materialization is not supported on Altertable; use 'table' instead"
  ) %}
{% endmaterialization %}

{% materialization table_function, adapter='altertable' %}
  {% do exceptions.raise_compiler_error(
    "The 'table_function' materialization is not supported on Altertable; use 'view' or 'table' instead"
  ) %}
{% endmaterialization %}
