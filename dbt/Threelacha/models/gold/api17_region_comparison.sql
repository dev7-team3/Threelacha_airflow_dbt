{{ config(
    materialized='table',
    format='PARQUET',
    location='s3://team3-batch/gold/api17_region_comparison/'
) }}

SELECT 
    res_dt,
    CAST(year AS INTEGER) AS year,
    CAST(month AS INTEGER) AS month,
    week_of_year,
    weekday_nm,
    weekend_yn,
    country_cd,
    country_nm,
    category_nm,
    item_nm,
    kind_nm,
    rank_nm,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as record_count,
    COUNT(DISTINCT market_nm) as market_count,
    CAST(current_timestamp AS VARCHAR) as created_at
FROM {{ source('silver', 'api17') }}
WHERE price IS NOT NULL
    AND country_nm IS NOT NULL
GROUP BY 
    res_dt, year, month, week_of_year, weekday_nm, weekend_yn,
    country_cd, country_nm,
    category_nm, item_nm, kind_nm, rank_nm