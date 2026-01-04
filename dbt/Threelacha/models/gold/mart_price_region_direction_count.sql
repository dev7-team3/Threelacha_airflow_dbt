{{ config(
    materialized = 'table',
    format = 'PARQUET',
    location = 's3a://team3-batch/gold/mart_price_region_direction_count/',
) }}

SELECT
    item_nm, kind_nm, prev_1d_dir_tp,
    count(prev_1d_dir_tp) as direction_count
FROM {{ ref('stg_daily_product_price') }}
where prev_1d_dir_tp is not null
GROUP BY 1, 2, 3
order by 1, 2, 3