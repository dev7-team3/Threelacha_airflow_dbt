import logging
from pathlib import Path

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sdk import TaskGroup, dag
from connection_utils import get_athena_config, get_query_engine_conn_id
import pendulum

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 기본 설정
ATHENA_OUTPUT = "s3://team3-batch/gold/athena-results/"
DAGS_DIR = Path(__file__).resolve().parent
SQL_DIR = DAGS_DIR / "sql" / "athena" / "silver"

# 테이블 스펙 정의
TABLE_SPECS = [
    {"table": "api1", "sql": "api1_table.sql"},
    {"table": "api10", "sql": "api10_table.sql"},
    {"table": "api13", "sql": "api13_table.sql"},
    {"table": "api17", "sql": "api17_table.sql"},
]


@dag(
    dag_id="schema_register_silver_all_athena",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={
        "owner": "jungeun_park",
    },
    tags=["KAMIS", "silver", "athena", "schema"],
    description="Athena Silver Layer 스키마 등록 DAG",
)
def setup_athena_base_dag():
    """
    Athena Glue Catalog에 Silver Layer 테이블을 등록하는 DAG
    - Silver/Gold 데이터베이스 생성
    - 테이블별 DROP/CREATE/REPAIR 실행
    """

    logger.info(f"Setting up Athena schema for {len(TABLE_SPECS)} tables")

    # 1. Connection / Athena Config 추출
    conn_id = get_query_engine_conn_id()
    _, workgroup = get_athena_config(conn_id)

    # 2. Database 생성 (Silver / Gold)
    create_silver_db = AthenaOperator(
        task_id="create_silver_db",
        aws_conn_id=conn_id,
        database="default",
        workgroup=workgroup,
        query="CREATE DATABASE IF NOT EXISTS team3_silver COMMENT 'Silver Layer'",
        output_location=ATHENA_OUTPUT,
    )

    create_gold_db = AthenaOperator(
        task_id="create_gold_db",
        aws_conn_id=conn_id,
        database="default",
        workgroup=workgroup,
        query="CREATE DATABASE IF NOT EXISTS team3_gold COMMENT 'Gold Layer (Mart)'",
        output_location=ATHENA_OUTPUT,
    )

    # 3. Silver 테이블 등록 루프
    for spec in TABLE_SPECS:
        table = spec["table"]
        sql_file = spec["sql"]

        # S3 경로 규칙: api1 -> api-1
        s3_dataset = spec.get("s3_path", f"{table[:3]}-{table[3:]}")
        location = f"s3://team3-batch/silver/{s3_dataset}/"

        logger.info(f"Registering table: {table} at {location}")

        with TaskGroup(group_id=f"register_{table}") as tg:
            # DROP 기존 테이블
            drop = AthenaOperator(
                task_id="drop",
                aws_conn_id=conn_id,
                database="team3_silver",
                workgroup=workgroup,
                query=f"DROP TABLE IF EXISTS team3_silver.{table}",
                output_location=ATHENA_OUTPUT,
            )

            # CREATE 테이블 (Jinja2 템플릿 사용)
            create = AthenaOperator(
                task_id="create",
                aws_conn_id=conn_id,
                database="team3_silver",
                workgroup=workgroup,
                query=sql_file,  # template_searchpath에서 sql 파일 찾음
                params={"database": "team3_silver", "table": table, "location": location},
                output_location=ATHENA_OUTPUT,
            )

            # REPAIR 파티션 복구(데이터 synk)
            repair = AthenaOperator(
                task_id="repair_partitions",
                aws_conn_id=conn_id,
                database="team3_silver",
                workgroup=workgroup,
                query=f"MSCK REPAIR TABLE team3_silver.{table}",
                output_location=ATHENA_OUTPUT,
            )

            # TaskGroup 내부 의존성
            drop >> create >> repair

        # 데이터베이스 생성 후 테이블 등록
        [create_silver_db, create_gold_db] >> tg


# DAG 인스턴스 생성
setup_athena_base_dag()
