# ============================================================
# Imports
# ============================================================

import json
import logging
import os
from pathlib import Path
import time
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connection_utils import get_storage_conn_id
from dotenv import load_dotenv
import requests

# ============================================================
# 1. 환경 설정
# ============================================================

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

BASE_URL = os.environ["KAMIS_BASE_URL"]
CONN_ID = get_storage_conn_id()

COMMON_PARAMS = {
    "action": "dailyCountyList",
    "p_cert_key": os.environ["CERT_KEY"],
    "p_cert_id": os.environ["CERT_ID"],
    "p_returntype": "json",
}

# ============================================================
# 2. S3 업로드
# ============================================================


def upload_json_to_s3(
    data: dict[str, Any],
    bucket: str,
    key: str,
) -> None:
    """
    JSON 데이터를 S3에 업로드

    Args:
        data (dict[str, Any]): 직렬화 가능한 JSON 데이터
        bucket (str): 대상 S3 버킷 이름
        key (str): 저장할 오브젝트 키

    Returns:
        None
    """
    s3 = S3Hook(aws_conn_id=CONN_ID)
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    s3.load_string(
        string_data=body,
        key=key,
        bucket_name=bucket,
        replace=True,
    )


# ============================================================
# 3. S3 object key 생성 함수
# ============================================================


def build_daily_s3_key(
    *,
    base_prefix: str,
    dt: str,
    country: str,
) -> str:
    """
    일별 소매 가격 데이터를 저장할 S3 오브젝트 키를 생성

    Args:
        base_prefix (str): 기본 S3 prefix
        dt (str): 데이터 날짜 (YYYYMMDD)
        country (str): 지역 코드

    Returns:
        str: 생성된 S3 오브젝트 키
    """
    return f"{base_prefix}/dt={dt}/product_cls=01/country={country}/data.json"


# ============================================================
# 4. API 호출
# ============================================================


def fetch_region_daily_price(
    country_code: str,
    timeout: int = 10,
    max_retries: int = 3,
    retry_delay: float = 3.0,
) -> dict[str, Any]:
    """
    특정 지역 코드에 대한 소매 가격 API를 호출

    Args:
        country_code (str): 지역 코드
        timeout (int): 요청 타임아웃 (초)
        max_retries (int): 최대 재시도 횟수
        retry_delay (float): 재시도 간격 (초)

    Returns:
        dict[str, Any]: API 응답 JSON
    """
    params = {
        **COMMON_PARAMS,
        "p_countycode": country_code,
    }

    for attempt in range(max_retries):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[{country_code}] API 실패 ({attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)


# ============================================================
# 5. 데일리 소매 데이터 수집 함수
# ============================================================


def collect_daily_price(
    *,
    bucket: str,
    country_code_file: str = "plugins/retail_country_code.json",
    base_prefix: str = "raw/api-10-test4",
) -> None:
    """
    지역별 소매 가격 데이터를 수집하여 S3에 저장

    Args:
        bucket (str): 대상 S3 버킷 이름
        country_code_file (str): 지역 코드 JSON 파일 경로
        base_prefix (str): S3 prefix

    Returns:
        None
    """
    country_code_path = Path(country_code_file)
    with country_code_path.open("r", encoding="utf-8") as f:
        country_codes = json.load(f).keys()

    success = 0
    skipped = 0

    for country_code in country_codes:
        response = fetch_region_daily_price(country_code)

        price = response.get("price")

        # ❗ price가 문자열이면 비정상 응답
        if not isinstance(price, list):
            logger.warning(f"[SKIP] country={country_code} empty price")
            skipped += 1
            continue

        try:
            dt = response["condition"][0][0]
        except Exception:
            logger.warning(f"[SKIP] country={country_code} invalid condition")
            skipped += 1
            continue

        key = build_daily_s3_key(
            base_prefix=base_prefix,
            dt=dt,
            country=country_code,
        )

        upload_json_to_s3(
            data=response,
            bucket=bucket,
            key=key,
        )

        logger.info(f"[OK] country={country_code} saved")
        success += 1

    logger.info(f"✅ collect done | success={success}, skipped={skipped}")
