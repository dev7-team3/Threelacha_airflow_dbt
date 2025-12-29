import logging

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup, dag, task
from connection_utils import get_athena_config, get_query_engine_conn_id
import pendulum

logger = logging.getLogger("airflow.task")

# Silver 테이블 목록
SILVER_TABLES = ["api1", "api10", "api13", "api17"]


@dag(
    dag_id="run_dbt_transform_gold",
    start_date=pendulum.datetime(2025, 12, 11, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "jungeun_park",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=1),
    },
    params={
        "full_refresh": False,  # UI에서 True로 변경 가능
    },
    tags=["dbt", "gold", "transform", "athena"],
    description="Silver → Gold 데이터 마트 변환 (dbt + Athena)",
)
def dbt_transform_gold():
    """
    dbt를 사용한 Gold 데이터 마트 생성 (Athena 기준)
    """

    # 1. Athena Connection / Config
    conn_id = get_query_engine_conn_id()
    database, workgroup = get_athena_config(conn_id)

    ATHENA_OUTPUT = "s3://team3-batch/gold/athena-results/"

    # 2. Silver 파티션 동기화 (모든 테이블)
    with TaskGroup(group_id="sync_silver_partitions") as sync_silver:
        for table in SILVER_TABLES:
            AthenaOperator(
                task_id=f"repair_{table}",
                aws_conn_id=conn_id,
                database=database,
                workgroup=workgroup,
                query=f"MSCK REPAIR TABLE team3_silver.{table}",
                output_location=ATHENA_OUTPUT,
            )

    # 3. dbt 모델 실행 (dbt 컨테이너)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            'docker exec dbt bash -c "cd /usr/app/Threelacha && '
            'dbt run {{ "--full-refresh" if params.full_refresh else "" }}"'
        ),
        do_xcom_push=True,
    )

    # 4. dbt 결과 로깅
    @task
    def log_dbt_results(**context):
        """dbt 실행 결과 로깅"""
        ti = context["ti"]
        dbt_output = ti.xcom_pull(task_ids="dbt_run")

        full_refresh = context["params"].get("full_refresh", False)
        mode = "FULL REFRESH" if full_refresh else "INCREMENTAL"

        if dbt_output:
            logger.info("=" * 80)
            logger.info(f"dbt run 실행 결과 - 모드: {mode}")
            logger.info("=" * 80)
            logger.info(dbt_output)
            logger.info("=" * 80)
        else:
            logger.warning("⚠️ dbt run 출력을 가져올 수 없습니다.")

    log_results = log_dbt_results()

    sync_silver >> dbt_run >> log_results


dbt_transform_gold()
