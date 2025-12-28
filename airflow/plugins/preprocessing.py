# ============================================================
# Imports
# ============================================================

import io
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================
# 1. preprocessing functions
# ============================================================


def normalize_price(series: pd.Series) -> pd.Series:
    """
    가격 문자열 Series를 float 타입으로 변환한다.

    - 쉼표, 공백, NBSP 제거
    - 변환 실패 시 NaN 처리

    Args:
        series (pd.Series): 가격 문자열 Series.

    Returns:
        pd.Series: float 타입으로 변환된 가격 Series.
    """
    cleaned = series.astype(str).str.replace(r"[,\s\u00a0]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce").astype("Float64")


def add_date_features(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    날짜 파생 컬럼을 추가한다.

    지정한 날짜 컬럼을 파싱하여 연/월/주차/요일번호/요일명/주말여부를 생성한다.
    파싱 실패는 NaT로 기록되며, 파생 컬럼은 NaT에 대해 NaN/None/False로 일관되게 처리한다.

    Args:
        df (pd.DataFrame): 데이터프레임.
        date_col (str): 날짜 컬럼명(예: 'res_dt').

    Returns:
        pd.DataFrame: 파생 컬럼이 추가된 데이터프레임.
    """
    if date_col not in df.columns:
        logger.warning("add_date_features: '%s' 컬럼이 없습니다.", date_col)
        return df

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    df["year"] = df[date_col].dt.year.astype("Int64")
    df["month"] = df[date_col].dt.month.astype("Int64")
    df["week_of_year"] = df[date_col].dt.isocalendar().week.astype("Int64")
    df["weekday_num"] = df[date_col].dt.weekday.astype("Int64")
    df[date_col] = df[date_col].dt.date

    weekday_kr = {
        0: "월요일",
        1: "화요일",
        2: "수요일",
        3: "목요일",
        4: "금요일",
        5: "토요일",
        6: "일요일",
    }

    df["weekday_nm"] = df["weekday_num"].map(weekday_kr).fillna("알수없음")
    df["weekend_yn"] = df["weekday_num"].isin([5, 6]).astype(bool)

    return df


def fix_cd_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    컬럼명이 '_cd'로 끝나는 경우 문자열(str) 타입으로 변환한다.

    Args:
        df (pd.DataFrame): 입력 데이터프레임.

    Returns:
        pd.DataFrame: '_cd' 컬럼이 문자열로 변환된 데이터프레임.
    """
    cd_cols = [col for col in df.columns if col.endswith("_cd")]
    for col in cd_cols:
        df[col] = df[col].astype(str)
    return df


# ============================================================
# 3. metadata loading function
# ============================================================


def prepare_metadata(csv_bytes: bytes) -> pd.DataFrame:
    """
    메타데이터 CSV 바이트를 표준화된 데이터프레임으로 변환한다.

    코드 컬럼의 포맷을 고정 길이(zfill) 또는 문자열로 일관화한다.

    Args:
        csv_bytes (bytes): CSV 파일의 바이트 콘텐츠.

    Returns:
        pd.DataFrame: 표준화된 메타데이터 데이터프레임.
    """
    if isinstance(csv_bytes, bytes):
        df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
    else:  # 이미 str인 경우
        df = pd.read_csv(io.StringIO(csv_bytes))

    df["product_cls_cd"] = df["product_cls_cd"].astype(str).str.zfill(2)
    df["category_cd"] = df["category_cd"].astype(str)
    df["item_cd"] = df["item_cd"].astype(str)
    df["kind_cd"] = df["kind_cd"].astype(str).str.zfill(2)
    df["rank_cd"] = df["rank_cd"].astype(str).str.zfill(2)

    return df


# ============================================================
# 4. S3 upload function
# ============================================================


def upload_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    base_prefix: str,
    aws_conn_id: str,
) -> None:
    """
    DataFrame을 Parquet으로 변환하여 S3(MinIO)에 업로드한다.

    디렉토리 구조:
        s3://{bucket}/{base_prefix}/year={year}/month={month}/data_{dt}.parquet

    Args:
        df (pd.DataFrame): 업로드할 데이터프레임 (year, month, res_dt 컬럼 포함).
        bucket (str): S3 버킷명.
        base_prefix (str): 기본 prefix 경로.
        aws_conn_id (str): Airflow 연결 ID.

    Returns:
        None
    """
    # DataFrame에서 날짜 관련 컬럼 추출
    year = str(df["year"].iloc[0])
    month = str(df["month"].iloc[0]).zfill(2)  # 월은 두 자리로 맞춤
    res_dt = str(df["res_dt"].iloc[0])
    dt = res_dt.replace("-", "")

    # 업로드 경로 생성
    key = f"{base_prefix}/year={year}/month={month}/data_{dt}.parquet"

    # DataFrame → Parquet 변환 (메모리 버퍼 사용)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # S3Hook 이용해서 업로드
    hook = S3Hook(aws_conn_id=aws_conn_id)
    hook.load_bytes(bytes_data=buffer.getvalue(), key=key, bucket_name=bucket, replace=True)

    logger.info("✅ 업로드 완료: s3://%s/%s", bucket, key)
