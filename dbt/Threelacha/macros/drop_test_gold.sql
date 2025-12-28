{% macro drop_test_gold() %}
    {% set sql %}
        DROP TABLE IF EXISTS team3_gold.test_gold
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}
