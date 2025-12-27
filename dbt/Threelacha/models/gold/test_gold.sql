{{ config(
    materialized='table',
    format='PARQUET',
    location='s3a://team3-batch/gold/test_gold/'
) }}

SELECT 
    res_dt,
    year,
    month,
    product_cls_nm,
    item_nm,
    COUNT(*) as record_count,
    CAST(NOW() AS VARCHAR) as created_at
FROM {{ source('silver', 'api1') }}
WHERE year = 2025 AND month = 12
GROUP BY res_dt, year, month, product_cls_nm, item_nm
LIMIT 100