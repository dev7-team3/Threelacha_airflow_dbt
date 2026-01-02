"""
KAMIS API1 데이터 수집 DAG

일별 소매/도매 품목 카테고리별 가격 데이터를 수집하여 S3에 저장합니다.

작업 흐름:
    1. fetch_api: API 호출 및 응답 검증
    2. build_path: 메타데이터 추출 및 S3 경로 생성
    3. save_to_s3: JSON 데이터를 S3에 업로드
"""

import logging
from typing import Dict, Optional

from airflow.sdk import dag, task
from api_caller_utils import call_kamis_api, validate_api_response
from metadata_loader_utils import generate_api1_params
import pendulum
from s3_uploader_utils import build_s3_path, extract_metadata_from_response, upload_json_to_s3

# 요청 api action 부분
API1_ACTION = "dailyPriceByCategoryList"


@dag(
    dag_id="raw_api1_collect_daily",
    description="KAMIS API1 일별 품목 카테고리별 가격 데이터 수집",
    schedule=None,
    start_date=pendulum.datetime(2025, 12, 23),
    catchup=False,
    tags=["KAMIS", "api-1", "raw", "daily"],
    default_args={
        "owner": "jungeun_park",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(hours=1),
    },
)
def extract_and_load_kamis_api1():
    """KAMIS API1 데이터 수집 파이프라인"""

    @task(max_active_tis_per_dag=3)
    def fetch_api(
        req: Dict[str, Optional[str]],
        data_interval_start=None,  # noqa: ANN001
    ) -> Optional[Dict]:
        """
        Task 1: API 호출 및 응답 검증

        Args:
            req: API 요청 파라미터
                - product_cls_code: 제품 분류 (01: 소매, 02: 도매)
                - category_code: 품목 카테고리 (100~600)
                - country_code: 지역 코드 (None: 전체)
            data_interval_start: Airflow 실행 시간

        Returns:
            API 응답
        """
        logger = logging.getLogger("airflow.task")

        # 전일 데이터 수집
        regday = (data_interval_start - pendulum.duration(days=1)).strftime("%Y-%m-%d")

        logger.info(
            f"[API 호출 시작] "
            f"날짜={regday}, "
            f"제품분류={req['product_cls_code']}, "
            f"카테고리={req['category_code']}, "
            f"지역={req['country_code'] or '전체'}"
        )

        # API 파라미터 구성
        params = {
            "p_product_cls_code": req["product_cls_code"],
            "p_item_category_code": req["category_code"],
            "p_country_code": req["country_code"],
            "p_regday": regday,
        }

        # API 호출 및 검증
        response = call_kamis_api(action=API1_ACTION, params=params)
        validate_api_response(response, allow_empty=True)

        return response

    @task
    def build_path(response: Dict) -> Optional[Dict]:
        """
        Task 2: S3 경로 생성

        Args:
            response: API 응답

        Returns:
            API 응답 및 S3 경로 또는 None
        """
        logger = logging.getLogger("airflow.task")

        # 응답데이터 기반 메타데이터 추출
        metadata = extract_metadata_from_response(response)

        # S3 경로 생성
        s3_key = build_s3_path(
            api_number="1",
            dt=metadata.get("p_regday", ""),
            product_cls=metadata.get("p_product_cls_code", "01"),
            country=metadata.get("p_country_code") or "all",
            category=metadata.get("p_category_code", ""),
            dt_normalized=False,
        )

        logger.info(f"📁 S3 경로 생성: {s3_key}")

        return {
            "response": response,
            "s3_key": s3_key,
        }

    @task
    def save_to_s3(data: Optional[Dict]) -> Optional[str]:
        """
        Task 3: S3 업로드

        Args:
            data: build_path의 출력
                - response: API 응답 (원본 그대로)
                - s3_key: S3 저장 경로

        Returns:
            업로드된 S3 키 또는 None
        """
        return upload_json_to_s3(
            data=data["response"],
            s3_key=data["s3_key"],
        )

    # ========================================
    # Task 체이닝
    # ========================================

    # 파라미터 조합 생성 (메타데이터 기반)
    requests = list(generate_api1_params())

    # Task 실행: fetch → build → save
    api_responses = fetch_api.expand(req=requests)
    s3path_wirh_res = build_path.expand(response=api_responses)
    save_to_s3.expand(data=s3path_wirh_res)


# ============================================================
# DAG 인스턴스 생성
# ============================================================

extract_and_load_kamis_api1()
