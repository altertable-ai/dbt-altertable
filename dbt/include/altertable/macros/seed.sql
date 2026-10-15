
{% macro altertable__get_binding_char() %}
  {{ return('?') }}
{% endmacro %}

{% macro altertable__load_csv_rows(model, agate_table) %}
    {% if config.get('fast', false) %}
        {% set seed_file_path = adapter.get_seed_file_path(model) %}
        {% set delimiter = config.get('delimiter', ',') %}
        {% set sql %}
          COPY {{ this.render() }} FROM '{{ seed_file_path }}' (FORMAT CSV, HEADER TRUE, DELIMITER '{{ delimiter }}')
        {% endset %}
        {% do adapter.add_query(sql, abridge_sql_log=True) %}
        {{ return(sql) }}
    {% endif %}

    {{ return(dbt.default__load_csv_rows(model, agate_table)) }}
{% endmacro %}
