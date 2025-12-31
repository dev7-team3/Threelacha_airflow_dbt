{{ config(
    materialized='table',
    file_format='parquet',
    write_compression='snappy',
    location='s3://team3-batch/gold/mart_eco_price_statistics_by_category/',
    partitioned_by=['year', 'month']
) }}

SELECT 
    res_dt,
    item_cd,
    item_nm,
    category_nm,
    market_category,
    COUNT(*) as record_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    CAST(current_timestamp AS VARCHAR) AS created_at,
    year,
    month
FROM {{ ref('stg_eco_data_with_market_category') }}
WHERE price IS NOT NULL
GROUP BY 
    res_dt,
    item_cd,
    item_nm,
    category_nm,
    market_category,
    year,
    month
