from itertools import product
import json
import logging
import os
from typing import Dict, List, Optional, TypedDict

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from connection_utils import get_storage_conn_id
import pendulum
import requests

# ---------------------------------------------------------
# 공통 설정 및 상수 정의
# ---------------------------------------------------------

BASE_API: str = "http://www.kamis.or.kr/service/price/xml.do"
CERT_KEY: str = os.environ.get("CERT_KEY")
CERT_ID: str = os.environ.get("CERT_ID")
BUCKET_NAME = "team3-batch"
S3_CONN_ID = get_storage_conn_id()

PRODUCT_CLS_CODES: Dict[str, str] = {
    "01": "소매",
}

CATEGORY_CODES: Dict[str, str] = {
    "100": "식량작물",
    "200": "채소류",
    "300": "특용작물",
    "400": "과일류",
    "500": "축산물",
    "600": "수산물",
}

COUNTRY_CODES: Dict[Optional[str], str] = {
    "1101": "서울",
    "2100": "부산",
    "2200": "대구",
    "2401": "광주",
    "2501": "대전",
    None: "전체지역",
}


# ---------------------------------------------------------
# 요청 단위 정의
# ---------------------------------------------------------
class APIRequestParams(TypedDict):
    """API 요청 파라미터를 담는 타입 정의.

    Attributes:
        product_cls_code: 제품 분류 코드 (01: 소매, 02: 도매)
        category_code: 품목 카테고리 코드 (100~600)
        country_code: 지역 코드 (None인 경우 전체 지역)
    """

    product_cls_code: str
    category_code: str
    country_code: Optional[str]


class APIResponse(TypedDict):
    """API 응답 데이터를 담는 타입 정의.

    Attributes:
        json_data: API 응답 JSON
        s3_key: 저장할 S3 경로
        request_params: 원본 요청 파라미터
    """

    json_data: Dict
    s3_key: str
    request_params: APIRequestParams


# ---------------------------------------------------------
# 모든 API 요청 조합 생성
# ---------------------------------------------------------
def generate_request_combinations() -> List[APIRequestParams]:
    """API 요청에 필요한 모든 파라미터 조합을 생성.

    Returns:
        List[APIRequestParams]: 제품분류, 품목카테고리, 지역의 모든 조합 리스트
    """
    return [
        {
            "product_cls_code": p_cls,
            "category_code": category,
            "country_code": country,
        }
        for p_cls, category, country in product(
            PRODUCT_CLS_CODES.keys(),
            CATEGORY_CODES.keys(),
            COUNTRY_CODES.keys(),
        )
    ]


@dag(
    dag_id="raw_api1_collect_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 12, 11, tz="UTC"),
    catchup=True,
    max_active_runs=5,
    tags=["KAMIS", "api-1", "raw"],
    default_args={
        "owner": "jungeun_park",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(hours=1),
    },
)
def extract_and_load_kamis_api1():
    """
    KAMIS API1 데이터 수집 DAG

    Task 구조:
    1. fetch_api: API 호출 및 응답 검증
    2. save_to_s3: S3에 JSON 저장
    """

    @task(max_active_tis_per_dag=5)
    def fetch_api(req: APIRequestParams, data_interval_start=None) -> Optional[APIResponse]:  # noqa: ANN001
        """
        API 호출 및 응답 검증

        Args:
            req: API 요청 파라미터
            data_interval_start: Airflow 실행 시간

        Returns:
            APIResponse 또는 None (Skip 시)
        """
        logger = logging.getLogger("airflow.task")

        # 처리 날짜 계산 (전일 데이터 수집)
        regday = (data_interval_start - pendulum.duration(days=1)).strftime("%Y-%m-%d")

        # 작업 시작 로그
        logger.info(
            f"Starting fetch - Request_Date: {regday}, "
            f"product_cls_code: {req['product_cls_code']}, "
            f"category_code: {req['category_code']}, "
            f"country_code: {req['country_code'] or 'all'}"
        )

        # 1. API 파라미터 설정
        params = {
            "action": "dailyPriceByCategoryList",
            "p_cert_key": CERT_KEY,
            "p_cert_id": CERT_ID,
            "p_returntype": "json",
            "p_product_cls_code": req["product_cls_code"],
            "p_item_category_code": req["category_code"],
            "p_regday": regday,
        }
        if req["country_code"]:
            params["p_country_code"] = req["country_code"]

        # 2. API 호출 (에러 타입별 처리)
        try:
            response = requests.get(BASE_API, params=params, timeout=60)
            response.raise_for_status()
            json_data = response.json()

            logger.debug(f"API call successful - Response time: {response.elapsed.total_seconds():.2f}s")

        except requests.exceptions.Timeout:
            logger.exception(f"API Timeout for date {regday}, params: {req}")
            raise  # 재시도 가능

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded, will retry with backoff")
                raise  # 재시도 가능
            elif e.response.status_code >= 500:
                logger.exception(f"Server error ({e.response.status_code}): {e}")  # noqa: TRY401
                raise  # 재시도 가능
            else:
                logger.exception(f"Client error ({e.response.status_code}): {e}")  # noqa: TRY401
                return None  # 재시도 불필요 (4xx 에러)

        except requests.exceptions.RequestException as e:
            logger.exception(f"Request failed: {e}")  # noqa: TRY401
            raise  # 재시도 가능

        except json.JSONDecodeError as e:
            logger.exception(f"Invalid JSON response: {e}")  # noqa: TRY401
            raise  # 재시도 가능

        # 3. 응답 검증
        data_block = json_data.get("data")
        if not data_block:
            logger.exception(f"No data block in response: {json_data}")
            # 재시도 불필요 - 응답 구조 자체가 잘못됨
            raise AirflowSkipException("Missing data block in API response - skipping task")

        # 에러 코드 판별
        if isinstance(data_block, dict):
            error_code = data_block.get("error_code")
            if error_code == "000":
                logger.debug("API returned success code (000)")
            else:
                error_msg = data_block.get("error_msg", "Unknown error")
                logger.exception(f"KAMIS API Error - Code: {error_code}, Message: {error_msg}")
                # 재시도 불필요 - API가 명시적으로 에러 응답
                raise AirflowSkipException(f"KAMIS API Error: {error_code} - {error_msg}")

        elif isinstance(data_block, list):
            if len(data_block) == 0:
                logger.exception("Empty data list returned from API - this is unexpected")
                # 재시도 불필요 - API가 명시적으로 에러 응답
                raise AirflowSkipException("API returned empty data list - invalid response format")

            # 001은 데이터 없음(정상) - 저장은 하되 로그 남김
            if data_block[0] == "001":
                logger.info(f"No data available for {regday} (code 001) - Will save empty response")
            else:
                logger.exception(f"Unexpected API response code: {data_block[0]}")
                # 재시도 불필요
                raise AirflowSkipException(f"KAMIS API returned unexpected code: {data_block[0]}")

        # 4. S3 경로 생성 - 응답 메타데이터 기반
        try:
            res_meta = json_data["condition"][0]

            # 모든 값을 응답에서 추출 (공백 제거)
            regday_from_response = res_meta["p_regday"].strip()
            product_cls = res_meta["p_product_cls_code"].strip()
            category = res_meta["p_category_code"].strip()

            # country 처리: 문자열이 아니면(리스트, None 등) 'all' 사용
            country_raw = res_meta.get("p_country_code", "")
            if isinstance(country_raw, str):
                country = country_raw.strip()
                country_path = country if country else "all"
            else:
                country_path = "all"
                logger.debug(f"p_country_code is not a string (type: {type(country_raw)}), using 'all'")

            s3_key = (
                f"raw/api-1/dt={regday_from_response}/"
                f"product_cls={product_cls}/"
                f"country={country_path}/"
                f"category={category}/data.json"
            )

            logger.debug(f"S3 path generated from API response: {s3_key}")

        except (KeyError, IndexError, AttributeError) as e:
            logger.exception(f"Failed to extract metadata from API response: {e}")  # noqa: TRY401
            logger.exception(f"Response structure: {json_data}")
            raise ValueError("Invalid API response structure: missing required fields in 'condition'")  # noqa: B904

        logger.info(f"✅ API fetch successful - Data size: {len(str(json_data)):,} chars")

        return {"json_data": json_data, "s3_key": s3_key, "request_params": req}

    @task
    def save_to_s3(api_response: Optional[APIResponse]) -> Optional[str]:
        """
        S3에 JSON 데이터 저장

        Args:
            api_response: API 응답 데이터 (None이면 Skip)

        Returns:
            저장된 S3 키 또는 None
        """
        logger = logging.getLogger("airflow.task")

        # fetch_api에서 None 반환 시 (Skip된 경우)
        if api_response is None:
            logger.info("No data to save (skipped in fetch_api)")
            return None

        json_data = api_response["json_data"]
        s3_key = api_response["s3_key"]
        req = api_response["request_params"]

        logger.info(
            f"Starting S3 save - "
            f"Key: {s3_key}, "
            f"product_cls: {req['product_cls_code']}, "
            f"category: {req['category_code']}"
        )

        try:
            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
            json_string = json.dumps(json_data, ensure_ascii=False)

            s3_hook.load_string(
                string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True, encoding="utf-8"
            )

            data_size = len(json_string.encode("utf-8"))
            logger.info(
                f"✅ Successfully saved to S3 - Key: {s3_key}, Size: {data_size:,} bytes ({data_size / 1024:.2f} KB)"
            )

            return s3_key  # noqa: TRY300

        except Exception as e:
            logger.exception(f"Failed to save to S3: {e}")  # noqa: TRY401
            raise

    # --- DAG Flow ---
    requests_list = generate_request_combinations()

    # Task 체이닝: fetch -> save
    api_responses = fetch_api.expand(req=requests_list)
    save_to_s3.expand(api_response=api_responses)


extract_and_load_kamis_api1()
