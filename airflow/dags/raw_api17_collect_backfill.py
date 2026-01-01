"""
KAMIS API17 Raw 데이터 백필 DAG

이 DAG는 KAMIS API17에서 과거 연간 데이터를 수집하여 S3 Raw 레이어에 저장합니다.
백필(backfill) 용도로 사용되며, 수동 실행(schedule=None)으로 동작합니다.
TaskFlow API를 사용하여 각 지역별로 병렬로 데이터를 수집합니다.

처리 흐름:
1. 각 지역(country_code)별로 독립적인 task 생성
2. 각 task는 카테고리/품목/품종/판매코드 조합에 대해 API 호출
3. 연간 데이터를 S3에 JSON 파일로 저장

저장 경로: raw/api-17/dt=YYYY/product_cls=01/country=CODE/category=XXX/item=XXX/kind=XX/product_rank=XX/data.json
"""

from datetime import datetime
import json
import logging
import os

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connection_utils import get_storage_conn_id
from dotenv import load_dotenv
from mapping_utils import load_country_code_mapping, set_category_product_variety_retail_code
from upload_utils import upload_json_to_s3

load_dotenv()

logger = logging.getLogger(__name__)

# 상수 정의
S3_CONN_ID = get_storage_conn_id()
API_KEY = os.getenv("CERT_KEY")
ID = os.getenv("CERT_ID")
BUCKET_NAME = "team3-batch"
REQUEST_URL = os.getenv("KAMIS_BASE_URL") + "action=periodRetailProductList"

# 코드 매핑 데이터 로드
country_code_mapping = load_country_code_mapping()


def get_data(
    region: str,
    item_category_code: str,
    item_code: str,
    kind_code: str,
    product_rank_code: str,
    start_day: str,
    end_day: str,
) -> dict:
    """
    API를 호출하여 데이터를 가져옵니다.

    Args:
        region: 지역 코드
        item_category_code: 카테고리 코드
        item_code: 품목 코드
        kind_code: 품종 코드
        product_rank_code: 판매코드
        start_day: 시작 날짜 (YYYY-MM-DD)
        end_day: 종료 날짜 (YYYY-MM-DD)

    Returns:
        API 응답 데이터 (JSON)
    """
    url = (
        f"{REQUEST_URL}&p_cert_key={API_KEY}&p_cert_id={ID}"
        f"&p_countrycode={region}&p_convert_kg_yn=N"
        f"&p_returntype=json&p_startday={start_day}&p_endday={end_day}"
        f"&p_itemcategorycode={item_category_code}"
        f"&p_itemcode={item_code}&p_kindcode={kind_code}"
        f"&p_productrankcode={product_rank_code}"
    )

    # http:// 사용 시 SSL 불필요
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"API 호출 오류: {e}") from e


def upload_data_to_s3(region: str, data: dict, category_row: dict, year: str) -> str:
    """
    데이터를 S3에 업로드합니다.

    Args:
        region: 지역 코드
        data: API 응답 데이터
        category_row: 카테고리, 품목, 품종, 판매코드 정보
        year: 연도 (YYYY)

    Returns:
        업로드된 S3 키
    """
    # API 응답에서 item 리스트 추출
    if "data" in data and "item" in data["data"]:
        items = data["data"]["item"]
        if not isinstance(items, list):
            items = [items]
    else:
        items = []

    json_data = json.dumps(items, ensure_ascii=False)

    # 경로 구성: raw/api-17/dt=YYYY/product_cls=01/country=CODE/category=XXX/item=XXX/kind=XX/product_rank=XX/data.json
    product_cls = "01"
    item_category_code = category_row["item_category_code"]
    item_code = category_row["item_code"]
    kind_code = category_row["kind_code"]
    product_rank_code = category_row["product_rank_code"]

    key = (
        f"raw/api-17/dt={year}/"
        f"product_cls={product_cls}/country={region}/category={item_category_code}/"
        f"item={item_code}/kind={kind_code}/product_rank={product_rank_code}/data.json"
    )

    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    upload_json_to_s3(hook, json_data, key, BUCKET_NAME)

    logger.info(f"✅ Uploaded: {key} ({len(items)} items)")
    return key


@dag(
    dag_id="raw_api17_collect_yearly",
    start_date=datetime(2025, 12, 10),
    schedule=None,  # 수동 실행
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(hours=1),
    },
    tags=["api_ingestion", "raw", "api17", "backfill"],
    description="KAMIS API17 연간 데이터 백필 DAG",
)
def raw_api17_collect_yearly():
    """
    KAMIS API17 연간 데이터 백필 DAG

    각 지역별로 병렬로 연간 데이터를 수집하여 S3 Raw 레이어에 저장합니다.
    TaskFlow API의 expand 기능을 사용하여 동적으로 task를 생성합니다.

    Returns:
        None
    """
    start_day = "2025-01-01"
    end_day = "2025-12-31"
    year = "2025"  # 백필 대상 연도

    @task
    def collect_yearly_data_by_region(region: str) -> None:
        """
        특정 지역의 연간 데이터를 수집합니다.

        Args:
            region: 지역 코드

        Returns:
            None
        """
        for category_row in set_category_product_variety_retail_code():
            try:
                data = get_data(
                    region=region,
                    item_category_code=category_row["item_category_code"],
                    item_code=category_row["item_code"],
                    kind_code=category_row["kind_code"],
                    product_rank_code=category_row["product_rank_code"],
                    start_day=start_day,
                    end_day=end_day,
                )

                upload_data_to_s3(region, data, category_row, year)

            except Exception as e:
                logger.warning(
                    f"❌ Error collecting data for {region} "
                    f"{category_row['item_category_code']}/{category_row['item_code']}/"
                    f"{category_row['kind_code']}/{category_row['product_rank_code']}: {e}"
                )
                continue

        logger.info(f"✅ Completed yearly data collection for region: {region}")

    # DAG 실행 흐름
    # 각 지역별로 병렬 task 생성
    country_codes = list(country_code_mapping.values())
    collect_yearly_data_by_region.expand(region=country_codes)


# DAG 인스턴스 생성
raw_api17_collect_yearly()
