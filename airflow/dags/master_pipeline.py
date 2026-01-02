from datetime import datetime

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# 공통 함수 정의
def make_trigger(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    """
    TriggerDagRunOperator를 생성하는 함수.

    Args:
        task_id (str): Airflow 내에서 사용할 태스크 ID
        dag_id (str): 트리거할 대상 DAG의 ID

    Returns:
        TriggerDagRunOperator: 지정된 DAG을 트리거하는 Operator 인스턴스
    """
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

@dag(
    dag_id="master_pipeline",
    schedule="0 1 * * *",  # UTC 1시 (KST 10시)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pipeline", "trigger"],
)
def master_pipeline():
    """
    Master Pipeline DAG.

    여러 API 수집 및 변환 DAG을 순차적으로 트리거하는 파이프라인을 정의한다.
    - API 1, API 10, API 17의 raw → silver 단계 실행
    - 수집/정제 완료 후 gold 변환 및 RDS 적재 단계 실행

    실행 순서:
        raw_api1_collect_daily → silver_api1_transform_daily →
        raw_api10_collect_daily → silver_api10_transform_daily →
        raw_api17_collect_daily → silver_api17_transform_daily →
        run_dbt_transform_gold → load_s3_gold_to_rds_mart
    """
    # --- API 1 Line ---
    t1_raw = make_trigger("trigger_raw_api1", "raw_api1_collect_daily")
    t1_silver = make_trigger("trigger_silver_api1", "silver_api1_transform_daily")

    # --- API 10 Line ---
    t10_raw = make_trigger("trigger_raw_api10", "raw_api10_collect_daily")
    t10_silver = make_trigger("trigger_silver_api10", "silver_api10_transform_daily")

    # --- API 17 Line ---
    t17_raw = make_trigger("trigger_raw_api17", "raw_api17_collect_daily")
    t17_silver = make_trigger("trigger_silver_api17", "silver_api17_transform_daily")

    # --- Gold Layer ---
    dbt_run = make_trigger("trigger_dbt_gold", "run_dbt_transform_gold")

    # --- RDS Load ---
    rds_sync = make_trigger("trigger_rds_sync", "load_s3_gold_to_rds_mart")

    # --- 순차 실행 체인 ---
    (t1_raw >> t1_silver >> t10_raw >> t10_silver >> t17_raw >> t17_silver >> dbt_run >> rds_sync)

master_pipeline()
