import logging
import os
from typing import Literal, Tuple

from airflow.providers.amazon.aws.hooks.athena import AthenaHook

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# 환경별 연결 설정
# -------------------------------------------------------------------------
CONNECTION_CONFIG = {
    "aws": {
        "storage": "s3_conn",
        "query_engine": "athena_conn",
        "dbms": "rds_conn",
    },
    "local": {
        "storage": "minio_conn",
        "query_engine": "trino_conn",
        "dbms": None,  # 추후 업데이트 예쩡
    },
}

# -------------------------------------------------------------------------
# Environment Resolver
# -------------------------------------------------------------------------
DEFAULT_ENV = "aws"


def get_current_env() -> str:
    """
    현재 Airflow 환경을 반환합니다.
    Returns:
        str: 현재 환경 ('aws' 또는 'local')
    """
    return os.environ.get("AIRFLOW_ENV", DEFAULT_ENV)


# -------------------------------------------------------------------------
# Connection Resolver
# -------------------------------------------------------------------------
def get_connection_id(conn_type: Literal["storage", "query_engine", "dbms"], log_info: bool = True) -> str:
    """
    환경에 따른 적절한 connection ID를 반환합니다.
    Args:
        conn_type: 연결 타입 ('storage', 'query_engine', 'dbms')
        log_info: 로그 출력 여부 (기본값: True)
    Returns:
        str: 사용할 connection ID
    Raises:
        ValueError: 잘못된 conn_type이 전달된 경우
    """
    env = get_current_env()

    if env not in CONNECTION_CONFIG:
        logger.warning(f"알 수 없는 환경 '{env}'이 감지되었습니다. 기본 환경 '{DEFAULT_ENV}'을 사용합니다.")
        env = DEFAULT_ENV

    if conn_type not in CONNECTION_CONFIG[env]:
        raise ValueError(f"잘못된 연결 타입: '{conn_type}'. 가능한 값: {list(CONNECTION_CONFIG[env].keys())}")

    conn_id = CONNECTION_CONFIG[env][conn_type]

    if log_info:
        logger.info(f"   - 환경: {env}")
        logger.info(f"   - 연결 타입: {conn_type}")
        logger.info(f"   - Connection ID: {conn_id}")

    return conn_id


def get_storage_conn_id() -> str:
    """
    환경에 따른 적절한 storage connection ID를 반환합니다.
    Returns:
        str: 사용할 storage connection ID
    """
    return get_connection_id("storage")


def get_query_engine_conn_id() -> str:
    """
    환경에 따른 적절한 query engine connection ID를 반환합니다.
    Returns:
        str: 사용할 query engine connection ID
    """
    return get_connection_id("query_engine")


def get_dbms_conn_id() -> str:
    """
    환경에 따른 적절한 query engine connection ID를 반환합니다.
    Returns:
        str: 사용할 query engine connection ID
    """
    return get_connection_id("dbms")


# -------------------------------------------------------------------------
# Athena-specific Helper
# -------------------------------------------------------------------------
def get_athena_config(conn_id: str = "athena_conn") -> Tuple[str, str]:
    """
    Athena Connection Extra에서 database / work_group 추출
    Args:
        conn_id: query engine connection ID
    Returns:
        Tuple[str, str]: (database, work_group)
    """
    hook = AthenaHook(aws_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    database = extra.get("database", "team3-glue")
    work_group = extra.get("work_group", "primary")

    logger.info("Athena config resolved")
    logger.info(f"   - database   : {database}")
    logger.info(f"   - work_group : {work_group}")

    return database, work_group
