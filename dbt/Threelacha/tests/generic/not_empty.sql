{% test not_empty(model) %}
    select *
    from {{ model }}
    limit 1
{% endtest %}
