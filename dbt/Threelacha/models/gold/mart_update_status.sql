{{ config(
    materialized = 'table',
    format = 'PARQUET',
    location = 's3a://team3-batch/gold/mart_update_status/',
) }}

SELECT
    count(*) as row_count,
    count(distinct country_nm) as country_count,
    max(res_dt) as latest_date
FROM {{ ref('stg_daily_product_price') }}