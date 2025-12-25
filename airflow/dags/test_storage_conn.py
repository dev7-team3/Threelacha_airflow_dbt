import logging
import os
from typing import Any, Dict

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from connection_utils import get_storage_conn_id
import pendulum


@dag(
    dag_id="test_storage_conn",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "jungeun_park",
        "retries": 1,  # 재시도 설정 추가
    },
    tags=["test", "storage", "connection"],
    description="Airflow 구동 환경에 적합한 Storage 연결 테스트를 위한 DAG",
)
def simple_storage_conn_test():
    """
    Airflow의 S3Hook을 사용하여 Storage 연결을 테스트하고,
    버킷 목록을 조회하는 DAG
    """

    @task
    def test_storage_connection() -> Dict[str, Any]:
        """Storage 연결 테스트 및 버킷 목록 조회"""
        logger = logging.getLogger(__name__)
        # 런타임에 conn_id 결정
        conn_id = get_storage_conn_id()
        env = os.environ.get("AIRFLOW_ENV", "local")

        try:
            # S3Hook 초기화
            hook = S3Hook(aws_conn_id=conn_id)
            # Boto3 클라이언트 객체 가져오기
            client = hook.get_conn()
            # 버킷 목록 조회
            buckets_response = client.list_buckets()
            logger.info("=" * 50)
            logger.info("✅ 연결 성공! 버킷 목록을 조회했습니다.")
            logger.info("=" * 50)
            # 버킷 정보 추출
            buckets = buckets_response.get("Buckets", [])
            bucket_names = [b["Name"] for b in buckets]
            logger.info(f"현재 존재하는 버킷 수: {len(bucket_names)}")
            for idx, name in enumerate(bucket_names, 1):
                logger.info(f"  {idx}. {name}")
            # 결과 반환 (다른 태스크에서 활용 가능)
            return {
                "status": "success",
                "environment": env,
                "connection_id": conn_id,
                "bucket_count": len(bucket_names),
                "buckets": bucket_names,
            }
        except Exception as e:
            logger.exception("=" * 50)
            logger.exception("❌ 연결 실패 또는 API 호출 오류 발생")
            logger.exception(f"   - Connection ID: {conn_id}")
            logger.exception(f"   - 에러 타입: {type(e).__name__}")
            logger.exception(f"   - 에러 메시지: {e!s}")  # noqa: TRY401
            logger.exception("=" * 50)
            logger.exception("🔧 확인 사항:")
            logger.exception(f"   1. Airflow UI에서 '{conn_id}' 연결이 올바르게 설정되었는지 확인")
            logger.exception("   2. Storage 서버가 실행 중인지 확인")
            logger.exception("   3. 네트워크 연결 상태 확인")
            raise

    test_storage_connection()


simple_storage_conn_test()
