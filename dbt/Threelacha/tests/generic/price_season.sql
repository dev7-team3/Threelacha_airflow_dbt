{% test price_season(model, present_month, season_month) %}

-- 0 row를 반환하면 pass, 1 row 이상 반환하면 fail
SELECT *
FROM {{ model }}
WHERE {{ present_month }} != {{ season_month }}

{% endtest %}
