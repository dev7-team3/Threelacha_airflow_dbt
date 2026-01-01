from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag
from datetime import datetime


@dag(
    dag_id="master_api17_pipeline",
    start_date=datetime(2025, 12, 10),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "jiyeon_kim",
        "depends_on_past": False,
    },
)
def master_api17_pipeline():
    trigger_raw = TriggerDagRunOperator(
        task_id="trigger_raw_api17_collect",
        trigger_dag_id="raw_api17_collect_daily",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_api17_transform",
        trigger_dag_id="silver_api17_transform_daily",
    )

    trigger_raw >> trigger_silver


master_api17_pipeline()
