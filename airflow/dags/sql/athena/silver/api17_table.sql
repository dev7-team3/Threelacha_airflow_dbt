CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database }}.{{ params.table }} (
    res_dt DATE COMMENT '응답일자',
    week_of_year INT COMMENT '연도기준주차',
    weekday_num INT COMMENT '요일번호',
    weekday_nm STRING COMMENT '요일명',
    weekend_yn BOOLEAN COMMENT '주말여부',
    product_cls_cd STRING COMMENT '산물구분코드',
    product_cls_nm STRING COMMENT '산물구분명',
    product_cls_unit STRING COMMENT '산물구분단위',
    product_no DOUBLE COMMENT '산물번호',
    category_cd STRING COMMENT '부류코드',
    category_nm STRING COMMENT '부류명',
    country_cd STRING COMMENT '지역코드',
    country_nm STRING COMMENT '지역명',
    market_nm STRING COMMENT '시장명',
    item_nm STRING COMMENT '품목명',
    item_cd STRING COMMENT '품목코드',
    kind_nm STRING COMMENT '품종명',
    kind_cd STRING COMMENT '품종코드',
    rank_nm STRING COMMENT '등급명',
    rank_cd STRING COMMENT '등급코드',
    price DOUBLE COMMENT '가격'
)
COMMENT 'KAMIS Silver 레이어 데이터'
PARTITIONED BY (
    year STRING COMMENT '연도',
    month STRING COMMENT '월'
)
STORED AS PARQUET
LOCATION '{{ params.location }}'
TBLPROPERTIES ('parquet.compress'='SNAPPY');