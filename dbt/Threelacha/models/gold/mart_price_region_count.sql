{{ config(
    materialized = 'table',
    format = 'PARQUET',
    location = 's3a://team3-batch/gold/mart_price_region_count/',
) }}

SELECT
    country_nm,
    SUM(CASE WHEN prev_1d_dir_tp = '1' THEN 1 ELSE 0 END) AS rise_count,
    SUM(CASE WHEN prev_1d_dir_tp = '0' THEN 1 ELSE 0 END) AS drop_count,
    SUM(CASE WHEN prev_1d_dir_tp = '2' THEN 1 ELSE 0 END) AS keep_count
FROM {{ ref('stg_daily_product_price') }}
GROUP BY country_nm