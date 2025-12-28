import logging
from typing import Any, Dict

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.sdk import dag, task
from connection_utils import (
    get_athena_config,
    get_query_engine_conn_id,
)
import pendulum


@dag(
    dag_id="test_athena_conn",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "jungeun_park"},
    tags=["test", "athena", "query_engine"],
)
def verify_athena_registration():

    @task
    def check_registered_tables() -> Dict[str, Any]:
        logger = logging.getLogger(__name__)
        conn_id = get_query_engine_conn_id()
        database, workgroup = get_athena_config(conn_id)
        hook = AthenaHook(aws_conn_id=conn_id)

        # ---------------------------------------------------------------------
        # 1. 테이블 목록 조회
        # [수정] result_configuration={} 인자를 명시적으로 추가
        # ---------------------------------------------------------------------
        list_query = f"SHOW TABLES IN `{database}`"
        logger.info(f"🔎 실행 중인 쿼리: {list_query}")

        exec_id = hook.run_query(
            query=list_query,
            result_configuration={},  # AthenaHook 필수 인자
            query_context={'Database': database},
            workgroup=workgroup
        )
        hook.poll_query_status(exec_id)

        list_results = hook.get_query_results(exec_id)
        rows = list_results.get('ResultSet', {}).get('Rows', [])
        tables = [row['Data'][0].get('VarCharValue') for row in rows if row['Data'][0]]

        logger.info(f"✅ 발견된 테이블 목록: {tables}")

        # ---------------------------------------------------------------------
        # 2. 각 테이블 상세 구조 검증
        # ---------------------------------------------------------------------
        verification_details = {}
        for table in tables:
            desc_query = f"DESCRIBE `{database}`.`{table}`"
            logger.info(f"   🔍 테이블 구조 확인: {table}")

            d_exec_id = hook.run_query(
                query=desc_query,
                result_configuration={},  # AthenaHook 필수 인자
                query_context={'Database': database},
                workgroup=workgroup
            )
            hook.poll_query_status(d_exec_id)

            desc_results = hook.get_query_results(d_exec_id)
            col_count = len(desc_results.get('ResultSet', {}).get('Rows', []))

            verification_details[table] = {"column_count": col_count, "status": "Healthy"}
            logger.info(f"   👉 테이블 '{table}': {col_count}개 컬럼 감지")

        return {
            "summary": {"database": database, "total_tables": len(tables)},
            "registered_tables": tables,
            "details": verification_details
        }

    check_registered_tables()

verify_athena_registration()
