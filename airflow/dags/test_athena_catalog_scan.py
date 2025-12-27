import logging
from typing import Any, Dict, List

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.sdk import dag, task
from connection_utils import (
    get_query_engine_conn_id,
    get_athena_config,
)
import pendulum


@dag(
    dag_id="test_athena_catalog_scan",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "jungeun_park"},
    tags=["test", "athena", "catalog", "schema"],
)
def verify_athena_catalog():

    @task
    def scan_all_databases() -> Dict[str, Any]:
        logger = logging.getLogger(__name__)

        conn_id = get_query_engine_conn_id()
        database, workgroup = get_athena_config(conn_id)
        hook = AthenaHook(aws_conn_id=conn_id)

        logger.info("🚀 Athena Catalog 전체 스캔 시작")

        # ---------------------------------------------------------------------
        # 1️⃣ Database 목록 조회
        # ---------------------------------------------------------------------
        list_db_query = "SHOW DATABASES"
        logger.info(f"🔎 Database 목록 조회 쿼리: {list_db_query}")

        db_exec_id = hook.run_query(
            query=list_db_query,
            result_configuration={},
            query_context={"Database": database},
            workgroup=workgroup,
        )
        hook.poll_query_status(db_exec_id)

        db_results = hook.get_query_results(db_exec_id)
        db_rows = db_results.get("ResultSet", {}).get("Rows", [])
        databases = [
            row["Data"][0].get("VarCharValue")
            for row in db_rows
            if row.get("Data")
        ]

        logger.info(f"📚 발견된 Database 수: {len(databases)}")
        logger.info(f"📚 Database 목록: {databases}")

        catalog_summary: Dict[str, Any] = {}

        # ---------------------------------------------------------------------
        # 2️⃣ 각 Database별 테이블 목록 + 구조 조회
        # ---------------------------------------------------------------------
        for db in databases:
            logger.info(f"📦 Database 스캔 시작: {db}")

            list_tables_query = f"SHOW TABLES IN `{db}`"
            t_exec_id = hook.run_query(
                query=list_tables_query,
                result_configuration={},
                query_context={"Database": db},
                workgroup=workgroup,
            )
            hook.poll_query_status(t_exec_id)

            table_results = hook.get_query_results(t_exec_id)
            table_rows = table_results.get("ResultSet", {}).get("Rows", [])
            tables: List[str] = [
                row["Data"][0].get("VarCharValue")
                for row in table_rows
                if row.get("Data")
            ]

            logger.info(f"   📄 {db} 내 테이블 수: {len(tables)}")

            table_details = {}

            for table in tables:
                desc_query = f"DESCRIBE `{db}`.`{table}`"
                logger.info(f"      🔍 테이블 구조 확인: {db}.{table}")

                d_exec_id = hook.run_query(
                    query=desc_query,
                    result_configuration={},
                    query_context={"Database": db},
                    workgroup=workgroup,
                )
                hook.poll_query_status(d_exec_id)

                desc_results = hook.get_query_results(d_exec_id)
                col_count = len(desc_results.get("ResultSet", {}).get("Rows", []))

                table_details[table] = {
                    "column_count": col_count,
                    "status": "Healthy",
                }

                logger.info(
                    f"         👉 {db}.{table}: {col_count}개 컬럼 감지"
                )

            catalog_summary[db] = {
                "table_count": len(tables),
                "tables": table_details,
            }

        logger.info("✅ Athena Catalog 전체 스캔 완료")

        return {
            "total_databases": len(databases),
            "databases": catalog_summary,
        }

    scan_all_databases()


verify_athena_catalog()
