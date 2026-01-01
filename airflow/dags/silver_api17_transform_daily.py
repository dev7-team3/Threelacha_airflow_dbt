from datetime import datetime, timedelta
import logging
import re

import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from connection_utils import get_storage_conn_id
from mapping_utils import get_country_code_reverse
from preprocessing import add_date_features, normalize_price, prepare_metadata
from read_utils import read_json_from_s3, read_parquet_from_s3
from upload_utils import upload_parquet_to_s3

logger = logging.getLogger(__name__)

# 상수 정의
BUCKET_NAME = "team3-batch"
METADATA_KEY = "metadata/dim_product_no.csv"
EXCLUDED_COUNTY_NAMES = ["평균", "평년"]
SILVER_PREFIX = "silver/api-17"

# 컬럼 순서 정의
COLUMN_ORDER = [
    "res_dt",
    "week_of_year",
    "weekday_num",
    "weekday_nm",
    "weekend_yn",
    "product_no",
    "product_cls_cd",
    "product_cls_nm",
    "country_cd",
    "country_nm",
    "county_nm",
    "category_cd",
    "category_nm",
    "item_cd",
    "item_nm",
    "kind_cd",
    "kind_nm",
    "rank_cd",
    "rank_nm",
    "market_nm",
    "price",
    "year",
    "month",
]


def parse_file_path(file_path: str) -> dict:
    """파일 경로에서 메타데이터 추출"""
    result = {}

    product_cls_match = re.search(r"product_cls=(\w+)", file_path)
    result["product_cls_cd"] = product_cls_match.group(1) if product_cls_match else None

    country_match = re.search(r"country=(\w+)", file_path)
    result["country_cd"] = country_match.group(1) if country_match else None

    category_match = re.search(r"category=(\w+)", file_path)
    result["category_cd"] = category_match.group(1) if category_match else None

    item_match = re.search(r"item=(\w+)", file_path)
    result["item_cd"] = item_match.group(1) if item_match else None

    kind_match = re.search(r"kind=(\w+)", file_path)
    result["kind_cd"] = kind_match.group(1) if kind_match else None

    product_rank_match = re.search(r"product_rank=(\w+)", file_path)
    result["rank_cd"] = product_rank_match.group(1) if product_rank_match else None

    return result


def format_dataframe(df: pd.DataFrame, object_name: str) -> pd.DataFrame:
    """DataFrame 포맷팅"""
    path_info = parse_file_path(object_name)
    country_code_reverse = get_country_code_reverse()
    path_info["country_nm"] = country_code_reverse.get(path_info["country_cd"], "기타")

    for key, value in path_info.items():
        df[key] = value

    def parse_regday(row: pd.Series) -> pd.Timestamp | None:
        if pd.isna(row.get("regday")) or row.get("regday") == "":
            return None
        try:
            regday_str = str(row["regday"]).strip()
            if "/" in regday_str:
                month, day = regday_str.split("/")
                year = str(row.get("yyyy", "2025"))
                date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                return pd.to_datetime(date_str, format="%Y-%m-%d")
            else:
                return None
        except Exception:
            return None

    df["res_dt"] = df.apply(parse_regday, axis=1)

    # 날짜 파생 컬럼 추가
    df = add_date_features(df, "res_dt")

    # 가격 정규화
    df["price_numeric"] = normalize_price(df["price"])

    # 컬럼 정리
    df = df.drop(columns=["price", "yyyy", "regday", "itemname", "kindname"])
    df = df.rename(columns={"price_numeric": "price", "countyname": "county_nm", "marketname": "market_nm"})

    return df


@dag(
    dag_id="silver_api17_transform_daily",
    start_date=datetime(2025, 12, 10),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"depends_on_past": False, "owner": "jiyeon_kim"},
    tags=["preprocessing", "api17"],
    description="KAMIS API17 Raw 데이터를 읽어 코드 매핑 및 파생 컬럼을 추가한 Silver 데이터로 변환",
)
def silver_api17_transform_daily():
    """
    KAMIS API17 Raw → Silver 변환 DAG

    Returns:
        None
    """
    s3_conn_id = get_storage_conn_id()

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
            logger.error("logical_date 또는 data_interval_start를 찾을 수 없습니다.")
            logical_date = datetime.now()
        else:
            logical_date = logical_date.date()

        target_date_obj = logical_date - timedelta(days=1)
        target_date = target_date_obj.strftime("%Y-%m-%d")
        year = logical_date.year
        month = logical_date.month
        month_str = f"{month:02d}"

        logger.info(f"📅 Processing date: {target_date} (Year: {year}, Month: {month})")

        return {
            "logical_date": logical_date,
            "target_date": target_date,
            "target_date_obj": target_date_obj,
            "year": year,
            "month": month,
            "month_str": month_str,
        }

    @task
    def list_json_files(date_info: dict) -> list[str]:
        """
        S3에서 처리 대상 JSON 파일 목록을 조회합니다.

        Args:
            date_info: 날짜 정보 딕셔너리

        Returns:
            JSON 파일 경로 리스트
        """
        target_date = date_info["target_date"]
        hook = S3Hook(aws_conn_id=s3_conn_id)

        prefix = f"raw/api-17/dt={target_date}/"
        logger.info(f"🔍 Searching for files with prefix: {prefix}")

        try:
            keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
            json_files = [key for key in keys if key.endswith(".json")]
            logger.info(f"✅ Found {len(json_files)} JSON files")
            return json_files
        except Exception as e:
            logger.warning(f"⚠️ Error listing files: {e}")
            return []

    @task
    def process_json_files(json_files: list[str]) -> pd.DataFrame:
        """
        JSON 파일들을 읽어서 DataFrame으로 변환 및 전처리합니다.

        Args:
            json_files: JSON 파일 경로 리스트

        Returns:
            변환된 DataFrame
        """
        if not json_files:
            logger.warning("⚠️ No JSON files to process")
            return pd.DataFrame()

        hook = S3Hook(aws_conn_id=s3_conn_id)
        all_dataframes = []

        for file_path in json_files:
            logger.info(f"📄 Processing: {file_path}")
            data = read_json_from_s3(hook, file_path, BUCKET_NAME)

            if data and isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)
                df_formatted = format_dataframe(df, file_path)

                # 평균/평년 데이터 제거
                if "county_nm" in df_formatted.columns:
                    df_formatted = df_formatted[~df_formatted["county_nm"].isin(EXCLUDED_COUNTY_NAMES)]

                all_dataframes.append(df_formatted)

        if not all_dataframes:
            logger.warning("⚠️ No valid data found")
            return pd.DataFrame()

        df_new = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"✅ Processed {len(df_new):,} records from {len(json_files)} files")
        return df_new

    @task
    def merge_metadata(df: pd.DataFrame) -> pd.DataFrame:
        """
        메타데이터와 병합하여 상품 정보를 추가합니다.

        Args:
            df: 병합할 DataFrame

        Returns:
            메타데이터가 병합된 DataFrame
        """
        if df.empty:
            logger.warning("⚠️ Empty DataFrame, skipping metadata merge")
            return df

        hook = S3Hook(aws_conn_id=s3_conn_id)
        response = hook.get_key(key=METADATA_KEY, bucket_name=BUCKET_NAME)

        if response is None:
            logger.error(f"❌ Metadata file not found: {METADATA_KEY}")
            return df

        meta_data = prepare_metadata(response.get()["Body"].read())

        merge_keys = ["product_cls_cd", "category_cd", "item_cd", "kind_cd", "rank_cd"]
        df = pd.merge(df, meta_data, on=merge_keys, how="left")
        df = df.drop(columns=["county_nm"], errors="ignore")

        logger.info(f"✅ Metadata merged: {len(df):,} records")
        return df

    @task
    def merge_existing_data(df_new: pd.DataFrame, date_info: dict) -> pd.DataFrame:
        """
        기존 Silver 데이터와 병합하여 중복을 제거합니다.

        Args:
            df_new: 새로 처리한 DataFrame
            date_info: 날짜 정보 딕셔너리

        Returns:
            병합된 DataFrame
        """
        if df_new.empty:
            logger.warning("⚠️ Empty DataFrame, skipping merge")
            return df_new

        year = date_info["year"]
        month_str = date_info["month_str"]
        target_date_obj = date_info["target_date_obj"]

        hook = S3Hook(aws_conn_id=s3_conn_id)
        parquet_key = f"{SILVER_PREFIX}/year={year}/month={month_str}/data.parquet"
        df_existing = read_parquet_from_s3(hook, parquet_key, BUCKET_NAME)

        if df_existing is not None:
            logger.info(f"📊 Existing data: {len(df_existing):,} records")
            # 새 데이터의 날짜에 해당하는 기존 데이터 제거
            df_existing = df_existing[df_existing["res_dt"] != target_date_obj]
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            logger.info(f"✅ Combined data: {len(df_combined):,} records")
        else:
            df_combined = df_new
            logger.info("ℹ️ No existing parquet file, using new data only")

        # 컬럼 순서 정리
        existing_columns = [col for col in COLUMN_ORDER if col in df_combined.columns]
        remaining_columns = [col for col in df_combined.columns if col not in existing_columns]
        df_combined = df_combined[existing_columns + remaining_columns]

        return df_combined

    @task
    def upload_to_s3(df: pd.DataFrame, date_info: dict) -> None:
        """
        최종 DataFrame을 Parquet 형식으로 S3에 저장합니다.

        Args:
            df: 저장할 DataFrame
            date_info: 날짜 정보 딕셔너리

        Returns:
            None
        """
        if df.empty:
            logger.warning("⚠️ Empty DataFrame, skipping upload")
            return

        year = date_info["year"]
        month_str = date_info["month_str"]
        target_date = date_info["target_date"]

        hook = S3Hook(aws_conn_id=s3_conn_id)
        parquet_key = f"{SILVER_PREFIX}/year={year}/month={month_str}/data.parquet"

        upload_parquet_to_s3(hook, df, parquet_key, BUCKET_NAME)
        logger.info(f"✅ Silver transformation completed: {target_date} -> {parquet_key}")

    # DAG 실행 흐름 정의
    date_info = extract_date_info()
    json_files = list_json_files(date_info)
    df_processed = process_json_files(json_files)
    df_with_metadata = merge_metadata(df_processed)
    df_final = merge_existing_data(df_with_metadata, date_info)
    upload_to_s3(df_final, date_info)


# DAG 인스턴스 생성
silver_api17_transform_daily()
