{{ config(
    materialized='table',
    format='PARQUET',
    location='s3://team3-batch/gold/team3_gold/test_gold/'
) }}

SELECT 
    res_dt,
    year,
    month,
    product_cls_nm,
    item_nm,
    COUNT(*) AS record_count,
    CAST(current_timestamp AS VARCHAR) AS created_at
FROM {{ source('silver', 'api1') }}
WHERE year = '2025'
    AND month = '12'
GROUP BY
    res_dt,
    year,
    month,
    product_cls_nm,
    item_nm
LIMIT 100
