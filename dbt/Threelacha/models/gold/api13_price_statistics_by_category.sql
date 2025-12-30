{{ config(
    materialized='table',
    file_format='parquet',
    write_compression='snappy',
    location='s3://team3-batch/gold/api13_price_statistics_by_category/',
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
    year,
    month,
    CAST(current_timestamp AS VARCHAR) AS created_at
FROM {{ ref('api13_with_market_category') }}
WHERE price IS NOT NULL
GROUP BY 
    res_dt,
    item_cd,
    item_nm,
    market_category,
    year,
    month
ORDER BY 
    res_dt DESC,
    item_nm,
    avg_price ASC

