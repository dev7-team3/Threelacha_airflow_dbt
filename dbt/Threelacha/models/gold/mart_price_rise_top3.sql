{{ config(
    materialized = 'table',
    format = 'PARQUET',
    location = 's3a://team3-batch/gold/mart_price_rise_top3/',
) }}

SELECT
    -- 산물 정보
    item_nm,
    kind_nm,
    product_cls_unit,

    -- 지역 정보
    country_cd,
    country_nm,

    -- 가격 정보
    res_dt,
    base_dt,
    base_pr,
    prev_1d_dt,
    prev_1d_pr,
    prev_1d_dir_pct,

    -- 순위
    ranking
FROM {{ ref('stg_daily_price_rise') }}
WHERE ranking <= 3
