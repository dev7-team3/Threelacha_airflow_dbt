"""
Silver API1 테이블을 Hive Metastore에 등록하는 DAG

필수 사전 작업:
- Airflow Connection: trino_conn (Trino 연결)
- MinIO에 silver/api-1/ 데이터 존재

실행 방법:
1. 최초 실행: 테이블 생성
2. 스키마 변경 시: 테이블 재생성 (DROP → CREATE)
3. 데이터 추가 시: 파티션 동기화만 실행
"""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag
import pendulum


@dag(
    dag_id="schema_register_silver_api13",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # 수동 실행 (또는 Silver Transform 완료 후 트리거)
    catchup=False,
    default_args={
        "owner": "dahye",
    },
    tags=["KAMIS", "api-13", "silver", "metastore", "schema"],
    description="Silver API13 테이블을 Hive Metastore에 등록 및 파티션 동기화",
)
def register_silver_api13_metastore():
    """
    Silver API13 테이블 Metastore 등록

    처리 흐름:
    1. Silver 스키마 생성
    2. 기존 테이블 삭제 (존재 시)
    3. 새 테이블 생성 (컬럼 코멘트 포함)
    4. 파티션 동기화
    """
    # 1. Silver 스키마 생성
    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id="trino_conn",
        sql="""
        CREATE SCHEMA IF NOT EXISTS hive.silver
        WITH (location = 's3a://team3-batch/silver/')
        """,
    )

    # 2. 기존 테이블 삭제 (재생성을 위해)
    drop_existing_table = SQLExecuteQueryOperator(
        task_id="drop_existing_table",
        conn_id="trino_conn",
        sql="""
        DROP TABLE IF EXISTS hive.silver.api13
        """,
    )

    # 3. Silver API13 테이블 생성 (컬럼 코멘트 포함)
    create_api13_table = SQLExecuteQueryOperator(
        task_id="create_api13_table",
        conn_id="trino_conn",
        sql="""
        CREATE TABLE hive.silver.api13 (
            -- 상품 분류
            product_cls_cd VARCHAR COMMENT '산물구분코드',
            product_cls_nm VARCHAR COMMENT '산물구분명',

            -- 카테고리
            category_cd VARCHAR COMMENT '부류코드',
            category_nm VARCHAR COMMENT '부류명',

            -- 품목 / 품종 / 등급
            item_cd VARCHAR COMMENT '품목코드',
            item_nm VARCHAR COMMENT '품목명',
            kind_cd VARCHAR COMMENT '품종코드',
            kind_nm VARCHAR COMMENT '품종명',
            product_cls_unit VARCHAR COMMENT '산물구분단위',
            rank_cd VARCHAR COMMENT '등급코드',
            rank_nm VARCHAR COMMENT '등급명',

            -- 지역 / 시장
            country_cd VARCHAR COMMENT '지역코드',
            market_nm VARCHAR COMMENT '시장명',

            -- 가격 정보
            price DOUBLE COMMENT '가격',

            -- 응답 / 파생 날짜 정보
            res_dt VARCHAR COMMENT '응답일자',
            week_of_year INTEGER COMMENT '연도기준주차',
            weekday_num INTEGER COMMENT '요일번호',
            weekday_nm VARCHAR COMMENT '요일명',
            weekend_yn BOOLEAN COMMENT '주말여부',

            -- 파티션 컬럼
            year INTEGER COMMENT '연도',
            month INTEGER COMMENT '월'
        )
        COMMENT 'KAMIS API13 Silver 레이어'
        WITH (
            format = 'PARQUET',
            external_location = 's3a://team3-batch/silver/api-13/',
            partitioned_by = ARRAY['year', 'month']
        )
        """,
    )

    # 4. 파티션 동기화 (S3의 실제 파티션을 Metastore에 자동 반영)
    sync_partitions = SQLExecuteQueryOperator(
        task_id="sync_partitions",
        conn_id="trino_conn",
        sql="""
        CALL hive.system.sync_partition_metadata(
            schema_name => 'silver',
            table_name => 'api13',
            mode => 'ADD'
        )
        """,
    )

    # 5. 테이블 정보 확인 (검증)
    verify_table = SQLExecuteQueryOperator(
        task_id="verify_table",
        conn_id="trino_conn",
        sql="""
        -- 테이블 구조 확인
        DESCRIBE hive.silver.api13;
        -- 파티션 목록 확인
        SELECT * FROM hive.silver."api13$partitions" ORDER BY year, month;
        """,
    )

    # 6. Gold 스키마 생성 (dbt 사용을 위해)
    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="trino_conn",
        sql="""
        CREATE SCHEMA IF NOT EXISTS hive.gold
        WITH (location = 's3a://team3-batch/gold/')
        """,
    )

    # Task 의존성
    (
        create_silver_schema
        >> drop_existing_table
        >> create_api13_table
        >> sync_partitions
        >> verify_table
        >> create_gold_schema
    )


register_silver_api13_metastore()
