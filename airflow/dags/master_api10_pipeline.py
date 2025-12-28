from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

from airflow import DAG

default_args = {
    "owner": "dahye",
    "depends_on_past": False,
    "retries": 0,
}

local_tz = pendulum.timezone("Asia/Seoul")

with DAG(
    "master_api10_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule="00 11 * * *",  # "0 20 * * 1-5", UTC 기준
    catchup=False,
    max_active_runs=1,
) as dag:
    # 1. raw: api 10 collecting
    dag1 = TriggerDagRunOperator(
        task_id="trigger_raw_api10_collect_daily",
        trigger_dag_id="raw_api10_collect_daily",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # 2. silver: api 10 preprocessing
    dag2 = TriggerDagRunOperator(
        task_id="trigger_silver_api10_transform",
        trigger_dag_id="silver_api10_transform",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    # dags process
    dag1 >> dag2
