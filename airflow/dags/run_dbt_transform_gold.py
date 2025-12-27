"""
Silver → Gold 데이터 마트 변환 DAG (dbt, Athena)
dbt 모델을 실행하여 Gold 레이어 데이터 마트 생성
"""

import logging

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from connection_utils import (
    get_query_engine_conn_id,
    get_athena_config,
)
import pendulum

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="run_dbt_transform_gold",
    start_date=pendulum.datetime(2025, 12, 11, tz="UTC"),
    schedule="0 3 * * *",  # 매일 새벽 3시
    catchup=False,
    default_args={
        "owner": "jungeun_park",
    },
    tags=["dbt", "gold", "transform", "athena"],
    description="Silver → Gold 데이터 마트 변환 (dbt + Athena)",
)
def dbt_transform_gold():
    """
    dbt를 사용한 Gold 데이터 마트 생성 (Athena 기준)
    """

    # ---------------------------------------------------------------------
    # 1️⃣ Athena Connection / Config
    # ---------------------------------------------------------------------
    conn_id = get_query_engine_conn_id()
    database, workgroup = get_athena_config(conn_id)

    ATHENA_OUTPUT = "s3://team3-batch/gold/athena-results/"

    # ---------------------------------------------------------------------
    # 2️⃣ Silver 파티션 동기화 (Athena)
    # ---------------------------------------------------------------------
    sync_silver = AthenaOperator(
        task_id="sync_silver_partitions",
        aws_conn_id=conn_id,
        database=database,
        workgroup=workgroup,
        query="MSCK REPAIR TABLE team3_silver.api1",
        output_location=ATHENA_OUTPUT,
    )

    # ---------------------------------------------------------------------
    # 3️⃣ dbt 모델 실행 (dbt 컨테이너)
    # ---------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            'docker exec dbt bash -c '
            '"cd /usr/app/Threelacha && dbt run"'
        ),
        do_xcom_push=True,
    )

    # ---------------------------------------------------------------------
    # 4️⃣ dbt 결과 로깅
    # ---------------------------------------------------------------------
    @task
    def log_dbt_results(**context):
        """dbt 실행 결과 로깅"""
        ti = context["ti"]
        dbt_output = ti.xcom_pull(task_ids="dbt_run")

        if dbt_output:
            logger.info("=" * 80)
            logger.info("dbt run 실행 결과")
            logger.info("=" * 80)
            logger.info(dbt_output)
            logger.info("=" * 80)
        else:
            logger.warning("⚠️ dbt run 출력을 가져올 수 없습니다.")

    log_results = log_dbt_results()

    # ---------------------------------------------------------------------
    # 실행 순서
    # ---------------------------------------------------------------------
    sync_silver >> dbt_run >> log_results


dbt_transform_gold()
