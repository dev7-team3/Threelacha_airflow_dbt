import logging

from connection_utils import get_athena_config, get_query_engine_conn_id
import pendulum

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sdk import dag, task

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="schema_register_silver_api1_athena",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "jungeun_park",
        "retries": 0,
    },
    tags=["KAMIS", "api-1", "silver", "athena", "schema"],
    description="Silver API1 테이블을 Athena(Glue)에 등록 및 파티션 동기화",
)
def register_silver_api1_athena():
    """
    Athena용 Silver API1 테이블 등록 프로세스
    처리 흐름:
    1. team3_silver 데이터베이스 생성
    2. api1 테이블 생성 및 파티션 동기화
    3. team3_gold 데이터베이스 생성 (dbt 사용을 위해)
    """

    conn_id = get_query_engine_conn_id()
    _, workgroup = get_athena_config(conn_id)

    @task
    def log_start():
        """시작 로깅"""
        logger.info("=" * 80)
        logger.info("🔧 Athena 스키마 등록 시작")
        logger.info(f"   - Connection: {conn_id}")
        logger.info(f"   - WorkGroup: {workgroup}")
        logger.info("=" * 80)

    start_log = log_start()

    # 1. Silver 데이터베이스 생성
    create_silver_schema = AthenaOperator(
        task_id="create_silver_schema",
        aws_conn_id=conn_id,
        query="CREATE DATABASE IF NOT EXISTS team3_silver COMMENT 'Silver layer for team3'",
        database="default",
        workgroup=workgroup,
        output_location="s3://team3-batch/gold/athena-results/",
    )

    # 2. 기존 테이블 삭제
    drop_existing_table = AthenaOperator(
        task_id="drop_existing_table",
        aws_conn_id=conn_id,
        query="DROP TABLE IF EXISTS team3_silver.api1",
        database="team3_silver",
        workgroup=workgroup,
        output_location="s3://team3-batch/gold/athena-results/",
    )

    # 3. Silver API1 테이블 생성
    create_api1_table = AthenaOperator(
        task_id="create_api1_table",
        aws_conn_id=conn_id,
        query="""
        CREATE EXTERNAL TABLE IF NOT EXISTS team3_silver.api1 (
            res_dt DATE,
            week_of_year INT,
            weekday_num INT,
            weekday_nm STRING,
            weekend_yn BOOLEAN,
            product_cls_cd STRING,
            product_cls_nm STRING,
            category_cd STRING,
            category_nm STRING,
            country_cd STRING,
            country_nm STRING,
            item_nm STRING,
            item_cd STRING,
            kind_nm STRING,
            kind_cd STRING,
            rank_nm STRING,
            rank_cd STRING,
            unit STRING,
            base_dt STRING,
            base_pr DOUBLE,
            prev_1d_dt STRING,
            prev_1d_pr DOUBLE,
            prev_1w_dt STRING,
            prev_1w_pr DOUBLE,
            prev_2w_dt STRING,
            prev_2w_pr DOUBLE,
            prev_1m_dt STRING,
            prev_1m_pr DOUBLE,
            prev_1y_dt STRING,
            prev_1y_pr DOUBLE,
            avg_tp STRING,
            avg_pr DOUBLE
        )
        COMMENT 'KAMIS API1 Silver 레이어'
        PARTITIONED BY (year STRING, month STRING)
        STORED AS PARQUET
        LOCATION 's3://team3-batch/silver/api-1/'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        database="team3_silver",
        workgroup=workgroup,
        output_location="s3://team3-batch/gold/athena-results/",
    )

    # 4. 파티션 동기화
    sync_partitions = AthenaOperator(
        task_id="sync_partitions",
        aws_conn_id=conn_id,
        query="MSCK REPAIR TABLE team3_silver.api1",
        database="team3_silver",
        workgroup=workgroup,
        output_location="s3://team3-batch/gold/athena-results/",
    )

    # 5. 테이블 검증
    verify_table = AthenaOperator(
        task_id="verify_table",
        aws_conn_id=conn_id,
        query="SELECT COUNT(*) as total_count, COUNT(DISTINCT year) as year_count, COUNT(DISTINCT month) as month_count FROM api1",
        database="team3_silver",
        workgroup=workgroup,
        output_location="s3://team3-batch/gold/athena-results/",
    )

    # 6. Gold 데이터베이스 생성 (dbt 사용을 위해)
    create_gold_schema = AthenaOperator(
        task_id="create_gold_schema",
        aws_conn_id=conn_id,
        query="CREATE DATABASE IF NOT EXISTS team3_gold COMMENT 'Gold layer for team3' LOCATION 's3://team3-batch/gold/'",
        database="default",
        workgroup=workgroup,
        output_location="s3://team3-batch/gold/athena-results/",
    )

    @task
    def log_completion():
        """완료 로깅"""
        logger.info("=" * 80)
        logger.info("✅ Athena 스키마 등록 완료!")
        logger.info("=" * 80)
        logger.info("📋 생성된 리소스:")
        logger.info("   - Database: team3_silver")
        logger.info("     - Table: api1")
        logger.info("     - Location: s3://team3-batch/silver/api-1/")
        logger.info("   - Database: team3_gold")
        logger.info("     - Location: s3://team3-batch/gold/")
        logger.info("=" * 80)
        logger.info("🎯 다음 단계:")
        logger.info("   1. Athena 콘솔에서 테이블 확인")
        logger.info("   2. dbt 모델 개발 및 실행")
        logger.info("=" * 80)

    completion_log = log_completion()

    # Task 의존성 설정
    (
        start_log
        >> create_silver_schema
        >> drop_existing_table
        >> create_api1_table
        >> sync_partitions
        >> verify_table
        >> create_gold_schema
        >> completion_log
    )


register_silver_api1_athena()
