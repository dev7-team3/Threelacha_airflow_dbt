from datetime import datetime
import logging

from airflow.providers.standard.operators.python import PythonOperator
from daily_collector import collect_daily_price

from airflow import DAG

logger = logging.getLogger(__name__)

# ============================================================
# Default Args
# ============================================================

default_args = {
    "owner": "dahye",
    "depends_on_past": False,
    "retries": 0,
}

# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id="raw_api10_collect_daily",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    PythonOperator(
        task_id="collect_daily_price",
        python_callable=collect_daily_price,
        op_kwargs={"bucket": "team3-batch"},
    )
