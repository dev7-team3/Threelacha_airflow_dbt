import logging
import os

logger = logging.getLogger(__name__)


def get_storage_conn_id() -> str:
    """환경에 따른 적절한 storage connection ID를 반환"""
    env = os.environ.get("AIRFLOW_ENV", "local")
    conn_id = "s3_conn" if env == "aws" else "minio_conn"
    logger.info(f"   - 환경: {env}")
    logger.info(f"   - Connection ID: {conn_id}")
    return conn_id
