{{ config(
    materialized='table',
    format='PARQUET',
    location='s3a://team3-batch/gold/api10_price_comparison/',
    partitioned_by=['dt']
) }}

SELECT
    -- 산물 정보
    product_no, category_nm, item_nm, kind_nm, product_cls_unit,
    -- 지역정보
    country_cd,
    country_nm,
    -- 일자/가격 정보
    res_dt,
    dt,
    base_dt, base_pr,
    prev_1d_dt, prev_1d_pr,
    direction_tp as prev_1d_dir_tp,
    CAST(direction_pct as DOUBLE) as prev_1d_dir_pct,
    prev_1m_dt, prev_1m_pr,
    prev_1y_dt,prev_1y_pr
FROM {{ source('silver', 'api10') }}
WHERE dt = (SELECT max(dt) FROM {{ source('silver', 'api10') }})