{{ config(
    materialized='ephemeral'
) }}

-- VIEW를 생성하지 않고 임시로 사용하는 ephemeral 모델
-- 실제 VIEW나 테이블을 생성하지 않고, 참조하는 쿼리에 인라인으로 포함됨
-- market_category 컬럼 추가 용도
SELECT 
    product_cls_cd,
    product_cls_nm,
    category_cd,
    category_nm,
    item_cd,
    item_nm,
    kind_cd,
    kind_nm,
    product_cls_unit,
    rank_cd,
    rank_nm,
    country_cd,
    market_nm,
    price,
    res_dt,
    week_of_year,
    weekday_num,
    weekday_nm,
    weekend_yn,
    year,
    month,
    CASE 
        WHEN market_nm LIKE '%-%' THEN 
            TRIM(SPLIT_PART(market_nm, '-', 2))
        ELSE 
            market_nm
    END as market_category
FROM {{ source('silver', 'api13') }}

