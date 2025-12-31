{{ config(materialized='ephemeral') }}

WITH region_product AS (
    SELECT
        -- 산물 정보
        pc.product_no,
        pc.category_nm,
        pc.item_nm,
        pc.kind_nm,

        -- 지역 정보
        pc.country_nm,
        cc.latitude,
        cc.longitude,

        -- 일자/가격 정보
        pc.base_dt,
        pc.base_pr,
        pc.prev_1y_dt,
        pc.prev_1y_pr,
        CAST(substr(pc.dt, 5, 2) AS INTEGER) AS present_month
    FROM {{ ref('stg_daily_product_price') }} as pc
    LEFT JOIN {{ ref('country_coordinates') }} cc
    ON pc.country_cd = cc.country_cd
)
SELECT rp.*,
    si.season,
    si.month as season_month
FROM region_product rp
LEFT JOIN {{ ref('seasonal_info') }} si
ON rp.product_no = si.product_no
AND rp.present_month = si.month
