# ============================================================
# Imports
# ============================================================

import json
import logging
import os
from pathlib import Path
import time
from typing import Any

import boto3
from dotenv import load_dotenv
import requests

# 환경 변수 로드
load_dotenv()

# logging 설정
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Config
base_url = os.environ["KAMIS_BASE_URL"]

common_params = {
    "action": "EcoPriceList",
    "p_cert_key": os.environ["CERT_KEY"],
    "p_cert_id": os.environ["CERT_ID"],
    "p_returntype": "json",
    "p_product_cls_code": "01",
    "p_convert_kg_yn": "N",
}

# ============================================================
# 1. S3 JSON 업로드 함수
# ============================================================


def upload_json_to_s3(
    data: dict[str, Any],
    bucket: str,
    key: str,
) -> None:
    """
    JSON 객체를 S3에 업로드합니다.
    기존 object가 있으면 overwrite 발생 여부를 반환합니다.

    Args:
        data (dict): JSON 직렬화 가능한 딕셔너리
        bucket (str): 대상 버킷 이름
        key (str): S3 오브젝트 키

    Returns:
        bool: True면 overwrite, False면 신규 적재
    """
    client = boto3.client("s3")

    body = json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    # 기존 object 존재 여부 확인
    try:
        client.head_object(Bucket=bucket, Key=key)
        existed = True  # 이미 존재 → overwrite
    except client.exceptions.ClientError:
        existed = False  # 존재하지 않음 → 신규 적재

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    return existed


# ============================================================
# 2. S3 object key 생성 함수
# ============================================================


def build_s3_object_key(
    response_json: dict[str, Any],
    *,
    base_prefix: str = "raw/api-13",
) -> str:
    """
    API 응답 조건을 기반으로 S3 오브젝트 키를 생성합니다.

    디렉토리 구조:
    dt={regday}/product_cls=01/country=all/category=.../item=.../kind=.../product_rank=.../data.json

    Args:
        response_json (dict): API 응답 JSON
        base_prefix (str): 기본 S3 prefix

    Returns:
        str: S3 오브젝트 키

    Raises:
        ValueError: response_json에 condition 필드가 없거나 잘못된 경우
    """
    try:
        condition = response_json["condition"][0]
    except (KeyError, IndexError, TypeError) as err:
        raise ValueError("유효하지 않은 API 응답: condition 없음") from err

    return (
        f"{base_prefix}/"
        f"dt={condition['p_regday']}/"
        f"product_cls=01/"
        f"country=all/"
        f"category={condition['p_itemcategorycode']}/"
        f"item={condition['p_itemcode']}/"
        f"kind={condition['p_kindcode']}/"
        f"product_rank={condition['p_productrankcode']}/"
        f"data.json"
    )


# ============================================================
# 3. API 호출 함수
# ============================================================


def fetch_eco_data(
    base_url: str,
    params: dict[str, Any],
    *,
    timeout: int = 10,
    max_retries: int = 3,
    retry_delay: float = 3.0,  # 3초
) -> dict[str, Any]:
    """
    친환경 가격 API에서 데이터를 호출합니다.

    Args:
        base_url (str): API 엔드포인트
        params (dict): 쿼리 파라미터
        timeout (int): 요청 타임아웃 (초)
        max_retries (int): 최대 재시도 횟수
        retry_delay (float): 재시도 간격 (초)

    Returns:
        dict: API 응답 JSON

    Raises:
        requests.RequestException: 최대 재시도 횟수 이후에도 HTTP 요청이 실패한 경우
        ValueError: 응답 파싱에 실패하거나 응답이 유효하지 않은 경우
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            logging.warning(f"API 호출 실패 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)


# ============================================================
# 4. API 호출 및 적재
# ============================================================


def collect_eco_data(
    base_url: str,
    regday: str,
    common_params: dict[str, Any],
    bucket: str,
    eco_params_file: str = "plugins/eco_api_params.json",
) -> None:
    """
    친환경 가격 데이터를 수집하고 S3에 원본 API 응답을 업로드합니다.

    S3 경로는 요청한 regday가 아니라 API 응답에 포함된 regday 기준으로 결정됩니다.
    eco_api_params.json 파일에서 파라미터를 불러와 사용합니다.
    overwrite/new 여부를 카운트합니다.

    Args:
        base_url (str): API 엔드포인트
        regday (str): 요청할 regday (YYYY-MM-DD)
        common_params (dict): 공통 API 파라미터
        bucket (str): 대상 S3 버킷 이름
        eco_params_file (str): eco API 파라미터 JSON 파일 경로
    """
    # JSON 파일에서 eco_api_params 불러오기
    eco_params_path = Path(eco_params_file)
    with eco_params_path.open("r", encoding="utf-8") as f:
        eco_api_params = json.load(f)

    overwrite_count = 0
    new_count = 0

    for params in eco_api_params:
        params_with_regday = {
            **common_params,
            **params,
            "p_regday": regday,
        }

        response_json = fetch_eco_data(
            base_url=base_url,
            params=params_with_regday,
        )

        object_key = build_s3_object_key(response_json)

        existed = upload_json_to_s3(
            data=response_json,
            bucket=bucket,
            key=object_key,
        )

        if existed:
            overwrite_count += 1
            logger.info(f"♻️ overwrite: s3://{bucket}/{object_key}")
        else:
            new_count += 1
            logger.info(f"🆕 new: s3://{bucket}/{object_key}")

    logger.info(f"Summary for regday={regday}: new={new_count}, overwrite={overwrite_count}")
