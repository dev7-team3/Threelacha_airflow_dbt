from io import BytesIO
import json
import logging
from typing import Any, Dict, Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from config.constants import BUCKET_NAME
from connection_utils import get_storage_conn_id
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("airflow.task")
S3_CONN_ID = get_storage_conn_id()


def upload_json_to_s3(
    data: Dict[str, Any],
    s3_key: str,
    bucket_name: str = BUCKET_NAME,
    aws_conn_id: str = S3_CONN_ID,
    replace: bool = True,
) -> str:
    """
    수집한 raw JSON 데이터를 S3에 업로드

    Args:
        data: 업로드할 데이터
        s3_key: S3 객체 키
        bucket_name: S3 버킷명
        aws_conn_id: Airflow S3 저장소 연결 ID
        replace: 기존 파일 덮어쓰기 여부

    Returns:
        업로드된 S3 키
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        json_string = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

        s3_hook.load_string(
            string_data=json_string,
            key=s3_key,
            bucket_name=bucket_name,
            replace=replace,
            encoding="utf-8",
        )

        data_size = len(json_string.encode("utf-8"))
        logger.info(f"✅ S3 upload success - {s3_key} ({data_size:,} bytes / {data_size / 1024:.2f} KB)")

        return s3_key  # noqa: TRY300

    except Exception as e:
        logger.exception(f"S3 upload failed: {e}")  # noqa: TRY401
        raise


def build_s3_path(
    api_number: str,
    dt: str,
    product_cls: str = "01",
    dt_normalized: bool = False,
    country: Optional[str] = None,
    category: Optional[str] = None,
    item: Optional[str] = None,
    kind: Optional[str] = None,
    product_rank: Optional[str] = None,
) -> str:
    """
    KAMIS 표준 S3 경로 생성

    Args:
        api_number: API 번호 ("1", "10", "17")
        dt: 날짜 (YYYY-MM-DD)
        product_cls: 제품 분류 (기본: "01")
        dt_normalized: True이면 하이픈 제거 (YYYYMMDD)
        country: 지역 코드
        category: 카테고리 코드
        item: 품목 코드
        kind: 품종 코드
        product_rank: 등급 코드

    Returns:
        S3 경로
    """
    # 날짜 정규화
    if dt_normalized:
        dt = dt.replace("-", "")

    # 기본 경로
    parts = [
        f"raw/api-{api_number}",
        f"dt={dt}",
        f"product_cls={product_cls}",
    ]

    # 선택적 파티션
    if country:
        parts.append(f"country={country}")
    if category:
        parts.append(f"category={category}")
    if item:
        parts.append(f"item={item}")
    if kind:
        parts.append(f"kind={kind}")
    if product_rank:
        parts.append(f"product_rank={product_rank}")

    parts.append("data.json")

    return "/".join(parts)


def extract_metadata_from_response(response: Dict[str, Any]) -> Dict[str, str]:
    """
    API 응답에서 메타데이터 추출

    Args:
        response: API 응답

    Returns:
        메타데이터 딕셔너리

    Raises:
        ValueError: condition 블록 구조가 잘못됨
    """
    try:
        condition = response["condition"][0]

        # 요청 파라미터에 대한 정보
        if isinstance(condition, dict):
            return {k: str(v).strip() if isinstance(v, str) and v else "" for k, v in condition.items()}

    except (KeyError, IndexError, TypeError) as e:
        logger.exception(f"Failed to extract metadata: {e}")  # noqa: TRY401
        raise ValueError("Invalid response structure") from e


def read_json_from_s3(
    s3_key: str,
    bucket_name: str = BUCKET_NAME,
    aws_conn_id: str = S3_CONN_ID,
) -> Optional[list]:
    """
    S3에서 JSON 파일 읽기

    Args:
        s3_key: S3 객체 키
        bucket_name: S3 버킷명
        aws_conn_id: Airflow S3 저장소 연결 ID

    Returns:
        JSON 데이터 (리스트) 또는 None
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        response = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)

        if response is None:
            return None

        json_data = json.loads(response.get()["Body"].read().decode("utf-8"))

        if len(json_data) == 0:
            return None
        else:
            return json_data

    except Exception as e:
        logger.warning(f"Error reading {s3_key}: {e}")
        return None


def read_parquet_from_s3(
    s3_key: str,
    bucket_name: str = BUCKET_NAME,
    aws_conn_id: str = S3_CONN_ID,
) -> Optional[pd.DataFrame]:
    """
    S3에서 Parquet 파일 읽기

    Args:
        s3_key: S3 객체 키
        bucket_name: S3 버킷명
        aws_conn_id: Airflow S3 저장소 연결 ID

    Returns:
        DataFrame 또는 None
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        response = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)

        if response is None:
            return None
        else:
            data = response.get()["Body"].read()
            buffer = BytesIO(data)
            table = pq.read_table(buffer)
            df = table.to_pandas()

            logger.info(f"✅ Read existing Parquet file: {s3_key} ({len(df):,} records)")
            return df

    except Exception as e:
        logger.warning(f"Error reading {s3_key}: {e}")
        return None


def upload_parquet_to_s3(
    df: pd.DataFrame,
    s3_key: str,
    bucket_name: str = BUCKET_NAME,
    aws_conn_id: str = S3_CONN_ID,
    replace: bool = True,
    compression: str = "snappy",
) -> str:
    """
    DataFrame을 Parquet로 변환하여 S3에 업로드

    Args:
        df: 업로드할 DataFrame
        s3_key: S3 객체 키
        bucket_name: S3 버킷명
        aws_conn_id: Airflow S3 저장소 연결 ID
        replace: 기존 파일 덮어쓰기 여부
        compression: 압축 방식 (기본: "snappy")

    Returns:
        업로드된 S3 키
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression=compression)
        buffer.seek(0)

        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=replace,
        )

        logger.info(f"✅ Uploaded Parquet file: {s3_key} ({len(df):,} records)")

    except Exception as e:
        logger.warning(f"Error uploading {s3_key}: {e}")
        raise
    else:
        return s3_key


def read_csv_from_s3(
    s3_key: str,
    bucket_name: str = BUCKET_NAME,
    aws_conn_id: str = S3_CONN_ID,
) -> Optional[pd.DataFrame]:
    """
    S3에서 CSV 파일 읽기

    Args:
        s3_key: S3 객체 키
        bucket_name: S3 버킷명
        aws_conn_id: Airflow S3 저장소 연결 ID

    Returns:
        DataFrame 또는 None
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        response = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)

        if response is None:
            return None
        else:
            return pd.read_csv(response.get()["Body"])

    except Exception as e:
        logger.warning(f"Error reading {s3_key}: {e}")
        return None


def upload_csv_to_s3(
    df: pd.DataFrame,
    s3_key: str,
    bucket_name: str = BUCKET_NAME,
    aws_conn_id: str = S3_CONN_ID,
    replace: bool = True,
    index: bool = False,
) -> str:
    """
    DataFrame을 CSV로 변환하여 S3에 업로드

    Args:
        df: 업로드할 DataFrame
        s3_key: S3 객체 키
        bucket_name: S3 버킷명
        aws_conn_id: Airflow S3 저장소 연결 ID
        replace: 기존 파일 덮어쓰기 여부
        index: 인덱스 포함 여부

    Returns:
        업로드된 S3 키
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        buffer = BytesIO()
        df.to_csv(buffer, index=index)
        buffer.seek(0)

        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=replace,
        )

        logger.info(f"✅ Uploaded CSV file: {s3_key} ({len(df):,} records)")

    except Exception as e:
        logger.warning(f"Error uploading {s3_key}: {e}")
        raise
    else:
        return s3_key
