{% test price_top3(model, country_col, rank_col) %}

-- 같은 country + ranking 조합이 두 번 이상 있으면 실패
SELECT
    {{ country_col }},
    {{ rank_col }},
    COUNT(*) AS cnt
FROM {{ model }}
GROUP BY 1, 2
HAVING COUNT(*) > 1

{% endtest %}
