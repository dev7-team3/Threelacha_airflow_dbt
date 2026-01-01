from datetime import datetime, timedelta
import json
import logging
import os

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


def group_data_by_date(data: dict) -> dict[str, list] | None:
    """
    날짜별로 데이터를 그룹화합니다.

    Args:
        data: API 응답 데이터

    Returns:
        날짜별 데이터 딕셔너리. 오류 발생 시 None 반환
    """
    grouped = {}

    if data["data"]["error_code"] != "000":
        return None

    for item in data["data"]["item"]:
        yyyy = item.get("yyyy", "")
        regday = item.get("regday", "")

        if yyyy and regday:
            # "12/17" -> "12-17" (MM/DD -> MM-DD)
            month_day = regday.replace("/", "-")
            date_str = f"{yyyy}-{month_day}"  # "2025-12-17"

            if date_str not in grouped:
                grouped[date_str] = []
            grouped[date_str].append(item)

    return grouped


def get_data(
    country_code: str,
    item_category_code: str,
    item_code: str,
    kind_code: str,
    product_rank_code: str,
    start_day: str,
    end_day: str,
) -> dict | None:
    """
    API를 호출하여 데이터를 가져옵니다.

    Args:
        country_code: 도시 코드
        item_category_code: 카테고리 코드
        item_code: 품목 코드
        kind_code: 품종 코드
        product_rank_code: 판매코드
        start_day: 시작 날짜 (YYYY-MM-DD)
        end_day: 종료 날짜 (YYYY-MM-DD)

    Returns:
        날짜별 데이터. 오류 발생 시 None 반환
    """
    url = (
        f"{REQUEST_URL}&p_cert_key={API_KEY}&p_cert_id={ID}&p_returntype=json"
        f"&p_startday={start_day}&p_endday={end_day}&p_countrycode={country_code}"
        f"&p_convert_kg_yn=N&p_itemcategorycode={item_category_code}"
        f"&p_itemcode={item_code}&p_kindcode={kind_code}&p_productrankcode={product_rank_code}"
    )

    logger.info(f"🔄 Getting data for country={country_code}, category={item_category_code}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return group_data_by_date(data)
    except requests.exceptions.RequestException as e:
        raise Exception(f"API 호출 오류: {e}") from e


def upload_data_to_s3(
    hook: S3Hook,
    country_code: str,
    date_data: dict[str, list],
    category_info: dict,
) -> None:
    """
    날짜별 데이터를 S3에 업로드합니다.

    Args:
        hook: S3Hook 인스턴스
        country_code: 도시 코드
        date_data: 날짜별 데이터
        category_info: 카테고리, 품목, 품종, 판매코드 정보
    """
    product_cls = "01"
    item_category_code = category_info["item_category_code"]
    item_code = category_info["item_code"]
    kind_code = category_info["kind_code"]
    product_rank_code = category_info["product_rank_code"]

    for date_str, data in date_data.items():
        key = (
            f"raw/api-17/dt={date_str}/"
            f"product_cls={product_cls}/country={country_code}/category={item_category_code}/"
            f"item={item_code}/kind={kind_code}/product_rank={product_rank_code}/data.json"
        )

        json_data = json.dumps(data, ensure_ascii=False)
        upload_json_to_s3(hook, json_data, key, BUCKET_NAME)


@dag(
    dag_id="raw_api17_collect_daily",
    start_date=datetime(2025, 12, 10),
    schedule="0 5 * * *",  # 매일 오전 5시
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "api17"],
    description="KAMIS API17 소매 가격 데이터 수집 DAG",
)
def raw_api17_collect_daily():
    """
    KAMIS API17 Raw 데이터 수집 DAG

    각 지역별로 병렬로 데이터를 수집하여 S3 Raw 레이어에 저장합니다.
    TaskFlow API의 expand 기능을 사용하여 동적으로 task를 생성합니다.

    Returns:
        None
    """
    s3_conn_id = S3_CONN_ID

    @task
    def extract_date_info(**context) -> dict:
        """
        Airflow 컨텍스트에서 처리할 날짜 정보를 추출합니다.

        Args:
            **context: Airflow 실행 컨텍스트

        Returns:
            날짜 정보 딕셔너리
        """
        logical_date = context.get("logical_date") or context.get("data_interval_start")

        if logical_date is None:
            raise ValueError("logical_date 또는 data_interval_start를 찾을 수 없습니다.")

        start_day = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        end_day = logical_date.strftime("%Y-%m-%d")

        logger.info(f"📅 Processing date range: {start_day} ~ {end_day}")

        return {"start_day": start_day, "end_day": end_day}

    @task
    def collect_data_by_country(country_code: str, date_info: dict) -> None:
        """
        특정 지역의 데이터를 수집합니다.

        Args:
            country_code: 도시 코드
            date_info: 날짜 정보 딕셔너리

        Returns:
            None
        """
        start_day = date_info["start_day"]
        end_day = date_info["end_day"]

        hook = S3Hook(aws_conn_id=s3_conn_id)

        for category in set_category_product_variety_retail_code():
            data_from_api = get_data(
                country_code=country_code,
                item_category_code=category["item_category_code"],
                item_code=category["item_code"],
                kind_code=category["kind_code"],
                product_rank_code=category["product_rank_code"],
                start_day=start_day,
                end_day=end_day,
            )

            if data_from_api is None:
                logger.warning(
                    f"❌ No data found for {country_code} {start_day}~{end_day} "
                    f"{category['item_category_code']}/{category['item_code']}/"
                    f"{category['kind_code']}/{category['product_rank_code']}"
                )
                continue

            upload_data_to_s3(
                hook=hook,
                country_code=country_code,
                date_data=data_from_api,
                category_info=category,
            )

        logger.info(f"✅ Completed data collection for country: {country_code}")

    # DAG 실행 흐름
    date_info = extract_date_info()

    # 각 지역별로 병렬 task 생성
    country_codes = list(country_code_mapping.values())
    collect_data_by_country.expand(country_code=country_codes, date_info=[date_info] * len(country_codes))


# DAG 인스턴스 생성
raw_api17_collect_daily()
