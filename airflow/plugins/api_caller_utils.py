import logging
import os
from typing import Any, Dict

from airflow.exceptions import AirflowSkipException
import requests

logger = logging.getLogger("airflow.task")

CERT_KEY: str = os.environ.get("CERT_KEY")
CERT_ID: str = os.environ.get("CERT_ID")
BASE_URL = os.environ.get("KAMIS_BASE_URL", "http://www.kamis.or.kr/service/price/xml.do?")


def call_kamis_api(
    action: str,
    params: Dict[str, Any],
    timeout: int = 120,
) -> Dict[str, Any]:
    """
    KAMIS API 호출 (단일 요청)

    재시도 및 백오프는 Airflow task retry 메커니즘에 위임합니다.
    - Client Error (4xx): Skip 처리 (재시도 불필요)
    - Server Error (5xx): Raise (Airflow가 재시도)
    - Network Error: Raise (Airflow가 재시도)

    Args:
        action: API 액션
        params: API 추가 파라미터
        timeout: 타임아웃 (초)

    Returns:
        API 응답 JSON

    Raises:
        AirflowSkipException: Client Error (4xx) - 재시도 불필요
        requests.RequestException: Server/Network Error - Airflow 재시도 대상
    """
    # 인증 정보 검증
    if not CERT_KEY or not CERT_ID:
        raise ValueError("CERT_KEY and CERT_ID must be set in environment variables")

    request_params = {
        "action": action,
        "p_cert_key": CERT_KEY,
        "p_cert_id": CERT_ID,
        "p_returntype": "json",
        **params,
    }

    try:
        response = requests.get(
            BASE_URL,
            params=request_params,
            timeout=timeout,
        )
        response.raise_for_status()

        logger.debug(f"KAMIS API call successful - elapsed={response.elapsed.total_seconds():.2f}s")

        return response.json()

    except requests.exceptions.Timeout:
        logger.warning(f"⏱️ KAMIS API 타임아웃 (timeout={timeout}s)")
        raise  # Airflow 재시도

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code

        if 400 <= status_code < 500:
            # Client Error → 재시도 의미 없음
            logger.warning(f"❌ KAMIS API Client Error: {status_code}")
            raise AirflowSkipException(f"KAMIS API client error: {status_code}") from e

        # Server Error (5xx) → Airflow 재시도 대상
        logger.warning(f"🔴 KAMIS API Server Error: {status_code}")
        raise  # Airflow 재시도

    except requests.exceptions.RequestException as e:
        logger.warning(f"🌐 KAMIS API 네트워크 에러: {e}")
        raise  # Airflow 재시도


def validate_api_response(
    response: Dict[str, Any],
    allow_empty: bool = False,
) -> None:
    """
    KAMIS API 응답 검증

    Args:
        response: API 응답 JSON
        allow_empty: True면 빈 데이터(001) 허용

    Raises:
        AirflowSkipException: 검증 실패 또는 데이터 없음
    """
    data = response.get("data")

    # data 블록 없음
    if not data:
        raise AirflowSkipException("Missing 'data' block")

    # Dict 타입: error_code 체크
    if isinstance(data, dict):
        error_code = data.get("error_code", "")

        if error_code == "000":
            return  # 정상

        error_msg = data.get("error_msg", "Unknown error")
        raise AirflowSkipException(f"API Error: {error_code} - {error_msg}")

    # List 타입: 빈 리스트 또는 "001" 체크
    if isinstance(data, list):
        # 빈 리스트 또는 "001" (데이터 없음)
        if len(data) == 0 or data[0] == "001":
            if allow_empty:
                logger.info("데이터 없음 (저장 허용됨)")
                return
            raise AirflowSkipException("No data available")

        return  # 정상

    # 예상치 못한 타입
    raise AirflowSkipException(f"Unexpected data type: {type(data)}")


# ============================================================
# API별 전용 검증 함수
# ============================================================


def validate_api10_response(response: Dict[str, Any]) -> None:
    """
    API10 전용 검증: price 필드 체크

    Args:
        response: API 응답

    Raises:
        AirflowSkipException: price 필드 유효하지 않음
    """
    price = response.get("price")

    if not isinstance(price, list):
        logger.warning("⚠️ API10: price 필드가 리스트가 아님")
        raise AirflowSkipException("Invalid price field (not a list)")

    if len(price) == 0:
        logger.info("API10: 빈 price 리스트")
        raise AirflowSkipException("Empty price list")

    logger.debug(f"✅ API10: price 필드 정상 ({len(price)}개 항목)")


def validate_api17_response(response: Dict[str, Any]) -> None:
    """
    API17 전용 검증: error_code 체크

    Args:
        response: API 응답

    Raises:
        AirflowSkipException: error_code가 000이 아님
    """
    error_code = response.get("data", {}).get("error_code")

    if error_code != "000":
        error_msg = response.get("data", {}).get("error_msg", "Unknown error")
        logger.warning(f"⚠️ API17 에러 - Code: {error_code}, Message: {error_msg}")
        raise AirflowSkipException(f"API17 Error: {error_code} - {error_msg}")

    logger.debug("✅ API17: 응답 정상 (error_code=000)")
