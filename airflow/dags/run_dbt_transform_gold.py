import logging

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup, dag, task
from config.constants import (
    ATHENA_OUTPUT_GOLD,
    DBT_CONTAINER_NAME,
    DBT_PROJECT_PATH,
    SILVER_DATABASE,
    SILVER_TABLES,
)
from connection_utils import get_athena_config, get_query_engine_conn_id
import pendulum

logger = logging.getLogger("airflow.task")

# 연결 설정
CONN_ID = get_query_engine_conn_id()
DATABASE, WORKGROUP = get_athena_config(CONN_ID)


@dag(
    dag_id="run_dbt_transform_gold",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"owner": "jungeun_park"},
    tags=["dbt", "gold", "transform", "athena", "synk"],
    description="Silver → Gold 데이터 마트 변환 (dbt + Athena)",
)
def dbt_transform_gold():
    """dbt를 사용한 Gold 데이터 마트 생성 및 테스트.

    Silver 테이블의 파티션을 동기화하고, dbt run으로 Gold 데이터 마트를 생성한 후,
    dbt test로 데이터 품질을 검증합니다.
    """

    # 1. Silver 파티션 동기화
    with TaskGroup(group_id="sync_silver_partitions") as sync_silver:
        for table in SILVER_TABLES:
            AthenaOperator(
                task_id=f"repair_{table}",
                aws_conn_id=CONN_ID,
                database=DATABASE,
                workgroup=WORKGROUP,
                query=f"MSCK REPAIR TABLE {SILVER_DATABASE}.{table}",
                output_location=ATHENA_OUTPUT_GOLD,
            )

    # 2. dbt 모델 실행 (dbt 컨테이너)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f'docker exec {DBT_CONTAINER_NAME} bash -c "cd {DBT_PROJECT_PATH} && dbt run"',
        do_xcom_push=True,
    )

    # 3. dbt 결과 로깅
    @task
    def log_dbt_results(**context):
        """dbt run 실행 결과를 로깅합니다."""
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

    # 4. dbt 테스트 실행
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f'docker exec {DBT_CONTAINER_NAME} bash -c "cd {DBT_PROJECT_PATH} && dbt test"',
        do_xcom_push=True,
    )

    # 5. dbt 테스트 결과 로깅
    @task
    def log_dbt_test_results(**context):
        """dbt test 실행 결과를 로깅합니다."""
        ti = context["ti"]
        test_output = ti.xcom_pull(task_ids="dbt_test")

        if test_output:
            logger.info("=" * 80)
            logger.info("dbt test 실행 결과")
            logger.info("=" * 80)
            logger.info(test_output)
            logger.info("=" * 80)

            if "ERROR" in test_output or "FAIL" in test_output:
                logger.error("⚠️ dbt 테스트 실패가 감지되었습니다. 결과를 확인하세요.")
        else:
            logger.warning("⚠️ dbt test 출력을 가져올 수 없습니다.")

    log_test_results = log_dbt_test_results()

    sync_silver >> dbt_run >> log_results >> dbt_test >> log_test_results


dbt_transform_gold()
