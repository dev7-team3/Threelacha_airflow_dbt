CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database }}.{{ params.table }} (
    product_no DOUBLE COMMENT '산물번호',
    product_cls_cd STRING COMMENT '산물구분코드',
    product_cls_nm STRING COMMENT '산물구분명',
    product_cls_unit STRING COMMENT '산물구분단위',
    category_cd STRING COMMENT '부류코드',
    category_nm STRING COMMENT '부류명',
    item_cd STRING COMMENT '품목코드',
    item_nm STRING COMMENT '품목명',
    kind_cd STRING COMMENT '품종코드',
    kind_nm STRING COMMENT '품종명',
    base_dt STRING COMMENT '조회일자',
    base_pr DOUBLE COMMENT '조회가격',
    prev_1d_dt STRING COMMENT '1일전일자',
    prev_1d_pr DOUBLE COMMENT '1일전가격',
    direction_tp STRING COMMENT '등락유형',
    direction_pct STRING COMMENT '등락률',
    prev_1m_dt STRING COMMENT '1개월전일자',
    prev_1m_pr DOUBLE COMMENT '1개월전가격',
    prev_1y_dt STRING COMMENT '1년전일자',
    prev_1y_pr DOUBLE COMMENT '1년전가격',
    country_cd STRING COMMENT '지역코드',
    country_nm STRING COMMENT '지역명',
    res_dt DATE COMMENT '응답일자',
    week_of_year INT COMMENT '연도기준주차',
    weekday_num INT COMMENT '요일번호',
    weekday_nm STRING COMMENT '요일명',
    weekend_yn BOOLEAN COMMENT '주말여부'
)
COMMENT 'KAMIS Silver 레이어 데이터'
PARTITIONED BY (
    year STRING COMMENT '연도',
    month STRING COMMENT '월',
    dt STRING COMMENT '일'
)
STORED AS PARQUET
LOCATION 's3://team3-batch/silver/api-10/main'
TBLPROPERTIES ('parquet.compress'='SNAPPY');