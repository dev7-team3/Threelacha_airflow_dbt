from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="schema_bootstrap_all",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["schema", "metastore", "bootstrap"],
) as dag:
    trigger_api1 = TriggerDagRunOperator(
        task_id="trigger_api1_schema",
        trigger_dag_id="schema_register_silver_api1",
    )

    trigger_api10 = TriggerDagRunOperator(
        task_id="trigger_api10_schema",
        trigger_dag_id="schema_register_silver_api10",
    )

    trigger_api13 = TriggerDagRunOperator(
        task_id="trigger_api13_schema",
        trigger_dag_id="schema_register_silver_api13",
    )

    trigger_api17 = TriggerDagRunOperator(
        task_id="trigger_api17_schema",
        trigger_dag_id="schema_register_silver_api17",
    )

    (trigger_api1 >> trigger_api10 >> trigger_api13 >> trigger_api17)
