{{ config(materialized='ephemeral') }}

WITH ranked_data AS (
    SELECT 
        *,
        -- dt 기준 내림차순
        RANK() OVER (ORDER BY dt DESC) as rnk
    FROM {{ source('silver', 'api10') }}
)
SELECT
    -- 산물 정보
    product_no,
    category_nm,
    item_nm,
    kind_nm,
    product_cls_unit,

    -- 지역정보
    country_cd,
    country_nm,

    -- 일자/가격 정보
    res_dt,
    base_dt,
    base_pr,

    prev_1d_dt,
    prev_1d_pr,
    direction_tp AS prev_1d_dir_tp,
    CAST(direction_pct AS DOUBLE) AS prev_1d_dir_pct,

    prev_1m_dt,
    prev_1m_pr,

    prev_1y_dt,
    prev_1y_pr,

    dt
FROM ranked_data
WHERE rnk = 1
