{% materialization incremental, adapter='altertable' -%}
  {% set result = dbt.materialization_incremental_default() %}
  {% set temp_relation = make_temp_relation(this.incorporate(type='table')) %}

  {% do adapter.drop_relation(temp_relation) %}
  {% do adapter.commit() %}

  {{ return(result) }}
{%- endmaterialization %}
