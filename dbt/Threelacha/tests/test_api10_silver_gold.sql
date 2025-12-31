-- silver 와 첫번째 ephemeral의 count가 같은지 test
-- 0 row를 반환하면 pass, 1 row 이상 반환하면 fail
WITH silver as (
    SELECT COUNT(*) cnt
    FROM {{ source('silver', 'api10') }}
    WHERE dt = (
        SELECT MAX(dt)
        FROM {{ source('silver', 'api10') }}
    )
),
gold as (
    SELECT COUNT(*) cnt
    FROM {{ ref('stg_daily_product_price') }}
)
SELECT 1
FROM silver, gold
WHERE silver.cnt != gold.cnt
