import logging
from pathlib import Path

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sdk import TaskGroup, dag
from config.constants import (
    ATHENA_OUTPUT_SILVER,
    BUCKET_NAME,
    GOLD_DATABASE,
    GOLD_METADATA_DATABASE,
    SILVER_DATABASE,
    SILVER_PREFIX,
    SILVER_TABLES,
)
from connection_utils import get_athena_config, get_query_engine_conn_id
import pendulum

logger = logging.getLogger(__name__)

# 연결 설정
CONN_ID = get_query_engine_conn_id()
_, WORKGROUP = get_athena_config(CONN_ID)
DAGS_DIR = Path(__file__).resolve().parent
SQL_DIR = DAGS_DIR / "sql" / "athena" / "silver"

# 테이블 스펙 정의
TABLE_SPECS = [{"table": table, "sql": f"{table}_table.sql"} for table in SILVER_TABLES]


@dag(
    dag_id="schema_register_silver_all_athena",
    schedule=None,
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"owner": "jungeun_park"},
    tags=["silver", "athena", "schema", "synk"],
    description="Athena Silver Layer 스키마 등록 DAG",
)
def setup_athena_base_dag():
    """Athena Glue Catalog에 Silver Layer 테이블을 등록하는 DAG.

    - Silver/Gold 데이터베이스 생성
    - 테이블별 DROP/CREATE/REPAIR 실행
    """
    logger.info(f"Setting up Athena schema for {len(TABLE_SPECS)} tables")

    # 1. Database 생성 (Silver / Gold / metadata)
    create_silver_db = AthenaOperator(
        task_id="create_silver_db",
        aws_conn_id=CONN_ID,
        database="default",
        workgroup=WORKGROUP,
        query=f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE} COMMENT 'Silver Layer'",
        output_location=ATHENA_OUTPUT_SILVER,
    )

    create_gold_db = AthenaOperator(
        task_id="create_gold_db",
        aws_conn_id=CONN_ID,
        database="default",
        workgroup=WORKGROUP,
        query=f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE} COMMENT 'Gold Layer (Mart)'",
        output_location=ATHENA_OUTPUT_SILVER,
    )

    create_gold_metadata_db = AthenaOperator(
        task_id="create_gold_metadata_db",
        aws_conn_id=CONN_ID,
        database="default",
        workgroup=WORKGROUP,
        query=f"CREATE DATABASE IF NOT EXISTS {GOLD_METADATA_DATABASE} COMMENT 'Gold Metadata Layer'",
        output_location=ATHENA_OUTPUT_SILVER,
    )

    # 2. Silver 테이블 등록 루프
    for spec in TABLE_SPECS:
        table = spec["table"]
        sql_file = spec["sql"]

        # S3 경로 규칙: api1 -> api-1
        s3_dataset = spec.get("s3_path", f"{table[:3]}-{table[3:]}")
        location = f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/{s3_dataset}/"

        logger.info(f"Registering table: {table} at {location}")

        with TaskGroup(group_id=f"register_{table}") as tg:
            # 2-1. DROP 기존 테이블
            drop = AthenaOperator(
                task_id="drop",
                aws_conn_id=CONN_ID,
                database=SILVER_DATABASE,
                workgroup=WORKGROUP,
                query=f"DROP TABLE IF EXISTS {SILVER_DATABASE}.{table}",
                output_location=ATHENA_OUTPUT_SILVER,
            )
            # 2-2. CREATE 테이블
            create = AthenaOperator(
                task_id="create",
                aws_conn_id=CONN_ID,
                database=SILVER_DATABASE,
                workgroup=WORKGROUP,
                query=sql_file,
                params={
                    "database": SILVER_DATABASE,
                    "table": table,
                    "location": location,
                },
                output_location=ATHENA_OUTPUT_SILVER,
            )
            # 2-3. REPAIR 파티션 복구(데이터 synk)
            repair = AthenaOperator(
                task_id="repair_partitions",
                aws_conn_id=CONN_ID,
                database=SILVER_DATABASE,
                workgroup=WORKGROUP,
                query=f"MSCK REPAIR TABLE {SILVER_DATABASE}.{table}",
                output_location=ATHENA_OUTPUT_SILVER,
            )

            drop >> create >> repair

        [create_silver_db, create_gold_db, create_gold_metadata_db] >> tg


setup_athena_base_dag()
