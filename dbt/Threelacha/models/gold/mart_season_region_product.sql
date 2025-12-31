{{ config(
    materialized = 'table',
    format = 'PARQUET',
    location = 's3a://team3-batch/gold/mart_season_region_product/',
) }}

SELECT
    -- 산물 정보
    product_no,
    category_nm,
    item_nm,
    kind_nm,

    -- 지역 정보
    country_nm,
    latitude,
    longitude,

    -- 일자/가격 정보
    base_dt,
    base_pr,
    prev_1y_dt,
    prev_1y_pr,
    present_month,
    season,
    season_month
FROM {{ ref('stg_season_region_all_product') }}
where season is not null
