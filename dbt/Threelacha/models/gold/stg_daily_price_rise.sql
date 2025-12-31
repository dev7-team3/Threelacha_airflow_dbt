{{ config(materialized='ephemeral') }}

SELECT
    -- 산물 정보
    product_no,
    category_nm,
    item_nm,
    kind_nm,
    product_cls_unit,

    -- 지역 정보
    country_cd,
    country_nm,

    -- 일자/가격 정보
    res_dt,
    base_dt,
    base_pr,
    prev_1d_dt,
    prev_1d_pr,

    -- 등락율/순위
    prev_1d_dir_pct,
    ROW_NUMBER() OVER (PARTITION BY country_cd ORDER BY prev_1d_dir_pct DESC) as ranking
FROM {{ ref('stg_daily_product_price') }}
WHERE prev_1d_dir_tp = '1'
