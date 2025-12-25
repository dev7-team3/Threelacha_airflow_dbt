from datetime import datetime, timedelta
from io import BytesIO
import json
import logging
from pathlib import Path
import re

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from connection_utils import get_storage_conn_id
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG

logger = logging.getLogger(__name__)

# 코드 매핑 데이터 로드
with Path.open(Path(__file__).parent.parent / "plugins" / "param_tree.json", "r", encoding="utf-8") as f:
    params_tree = json.load(f)

with Path.open(Path(__file__).parent.parent / "plugins" / "country_code.json", "r", encoding="utf-8") as f:
    country_code_mapping = json.load(f)

# country_code 역매핑 (코드 -> 이름)
country_code_reverse = {v: k for k, v in country_code_mapping.items()}

BUCKET_NAME = "team3-batch"
S3_CONN_ID = get_storage_conn_id()


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
    df["res_dt"] = df["res_dt"].dt.date

    df["week_of_year"] = df["res_dt"].dt.isocalendar().week
    df["weekday_num"] = df["res_dt"].dt.weekday
    df["year"] = df["res_dt"].dt.year
    df["month"] = df["res_dt"].dt.month

    weekday_map = {
        0: "월요일",
        1: "화요일",
        2: "수요일",
        3: "목요일",
        4: "금요일",
        5: "토요일",
        6: "일요일",
    }
    df["weekday_nm"] = df["weekday_num"].map(weekday_map)
    df["weekend_yn"] = df["weekday_num"].isin([5, 6])

    # price를 숫자로 변환 (쉼표 제거, '-' 처리)
    if "price" in df.columns:

        def clean_price(value: str | None) -> float | None:
            if pd.isna(value) or str(value).strip() == "-" or str(value).strip() == "":
                return None
            try:
                return float(str(value).strip().replace(",", ""))
            except ValueError:
                return None

        df["price_numeric"] = df["price"].apply(clean_price)

    df = df.drop(columns=["price", "yyyy", "regday", "itemname", "kindname"])
    df = df.rename(columns={"price_numeric": "price", "countyname": "county_nm", "marketname": "market_nm"})

    return df


def read_json_from_s3(hook: S3Hook, object_name: str) -> list | None:
    """S3에서 JSON 파일 읽기"""
    try:
        response = hook.get_key(key=object_name, bucket_name=BUCKET_NAME)
        if response is None:
            return None

        json_data = json.loads(response.get()["Body"].read().decode("utf-8"))

        if len(json_data) == 0:
            return None
        else:
            return json_data
    except Exception as e:
        logger.warning(f"Error reading {object_name}: {e}")
        return None


def read_parquet_from_s3(hook: S3Hook, object_name: str) -> pd.DataFrame | None:
    """S3에서 Parquet 파일 읽기"""
    try:
        response = hook.get_key(key=object_name, bucket_name=BUCKET_NAME)
        if response is None:
            return None
        else:
            data = response.get()["Body"].read()
            buffer = BytesIO(data)
            table = pq.read_table(buffer)
            df = table.to_pandas()

            logger.info(f"✅ Read existing Parquet file: {object_name} ({len(df):,} records)")
            return df
    except Exception as e:
        logger.warning(f"Error reading {object_name}: {e}")
        return None


def upload_parquet_to_s3(hook: S3Hook, df: pd.DataFrame, object_name: str) -> None:
    """DataFrame을 Parquet로 변환하여 S3에 업로드"""
    try:
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=object_name,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        logger.info(f"✅ Uploaded Parquet file: {object_name} ({len(df):,} records)")
    except Exception as e:
        logger.warning(f"Error uploading {object_name}: {e}")
        raise


def upload_csv_to_s3(hook: S3Hook, df: pd.DataFrame, object_name: str) -> None:
    """DataFrame을 CSV로 변환하여 S3에 업로드"""
    try:
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=object_name,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        logger.info(f"✅ Uploaded CSV file: {object_name} ({len(df):,} records)")
    except Exception as e:
        logger.warning(f"Error uploading {object_name}: {e}")
        raise


def read_csv_from_s3(hook: S3Hook, object_name: str) -> pd.DataFrame | None:
    """S3에서 CSV 파일 읽기"""
    try:
        response = hook.get_key(key=object_name, bucket_name=BUCKET_NAME)
        if response is None:
            return None
        return pd.read_csv(response.get()["Body"])
    except Exception as e:
        logger.warning(f"Error reading {object_name}: {e}")
        return None


def merge_dataframes(df: pd.DataFrame) -> pd.DataFrame:
    """두 개의 DataFrame을 병합"""
    s3_client = S3Hook(aws_conn_id=S3_CONN_ID)

    response = s3_client.get_key(key="metadata/dim_product_no.csv", bucket_name=BUCKET_NAME)
    meta_data = pd.read_csv(BytesIO(response.get()["Body"].read()))

    meta_data["product_cls_cd"] = meta_data["product_cls_cd"].astype(str).str.zfill(2)
    meta_data["category_cd"] = meta_data["category_cd"].astype(str)
    meta_data["item_cd"] = meta_data["item_cd"].astype(str)
    meta_data["kind_cd"] = meta_data["kind_cd"].astype(str).str.zfill(2)
    meta_data["rank_cd"] = meta_data["rank_cd"].astype(str).str.zfill(2)

    df = pd.merge(df, meta_data, on=["product_cls_cd", "category_cd", "item_cd", "kind_cd", "rank_cd"], how="left")
    df = df.drop(columns=["county_nm"])
    return df


def transform_raw_to_silver(**context) -> None:
    """Raw 데이터를 Silver 레이어로 변환"""
    logical_date = context.get("logical_date") or context.get("data_interval_start")

    logical_date = logical_date.date()

    if logical_date is None:
        logger.error("logical_date 또는 data_interval_start를 찾을 수 없습니다.")
        logical_date = datetime.now()

    target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
    year = logical_date.year
    month = logical_date.month
    month_str = f"{month:02d}"

    logger.info(f"Processing date: {target_date} (Year: {year}, Month: {month})")

    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # 해당 날짜의 모든 JSON 파일 목록 가져오기
    prefix = f"raw/api-17/dt={target_date}/"
    logger.info(f"Searching for files with prefix: {prefix}")

    try:
        keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
        json_files = [key for key in keys if key.endswith(".json")]
        logger.info(f"Found {len(json_files)} JSON files")
    except Exception as e:
        logger.warning(f"Error listing files: {e}")
        json_files = []

    if not json_files:
        logger.warning(f"No JSON files found for date {target_date}")
        return

    # 모든 JSON 파일을 읽어서 DataFrame으로 변환
    all_dataframes = []

    for file_path in json_files:
        logger.info(f"Processing: {file_path}")
        data = read_json_from_s3(hook, file_path)

        if data and isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            df_formatted = format_dataframe(df, file_path)

            # 평균/평년 데이터 제거
            if "county_nm" in df_formatted.columns:
                df_formatted = df_formatted[~df_formatted["county_nm"].isin(["평균", "평년"])]

            all_dataframes.append(df_formatted)

    if not all_dataframes:
        logger.warning("No valid data found")
        return

    # # 모든 DataFrame 합치기
    df_new = pd.concat(all_dataframes, ignore_index=True)

    logger.info("Before merge:")
    logger.info(f"New data: {len(df_new):,} records")
    logger.info(f"New data columns: {df_new.columns}")
    logger.info(f"New data head: {df_new.head()}")

    df_new = merge_dataframes(df_new)

    logger.info("After merge:")
    logger.info(f"New data: {len(df_new):,} records")
    logger.info(f"New data columns: {df_new.columns}")
    logger.info(f"New data head: {df_new.head()}")

    # 기존 Parquet 파일 읽기
    parquet_key = f"silver/api-17/year={year}/month={month_str}/data.parquet"
    df_existing = read_parquet_from_s3(hook, parquet_key)

    if df_existing is not None:
        # 기존 데이터와 새 데이터 병합
        # 중복 제거 (같은 날짜, 같은 키의 데이터)
        logger.info(f"Existing data: {len(df_existing):,} records")

        # 새 데이터의 날짜에 해당하는 기존 데이터 제거
        df_existing = df_existing[df_existing["res_dt"].dt.date != target_date]

        # 병합
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        logger.info(f"Combined data: {len(df_combined):,} records")
    else:
        df_combined = df_new
        logger.info("No existing parquet file, using new data only")

    # # 컬럼 순서 정리
    column_order = [
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

    existing_columns = [col for col in column_order if col in df_combined.columns]
    remaining_columns = [col for col in df_combined.columns if col not in existing_columns]
    df_combined = df_combined[existing_columns + remaining_columns]

    # Parquet로 저장
    upload_parquet_to_s3(hook, df_combined, parquet_key)

    logger.info(f"✅ Silver transformation completed: {target_date} -> {parquet_key}")


with DAG(
    dag_id="silver_api17_transform_daily",
    start_date=datetime(2025, 12, 10),
    schedule="0 6 * * *",  # 매일 오전 6시 (Raw 수집 후)
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["silver", "transform", "api17"],
) as dag:
    transform_task = PythonOperator(
        task_id="transform_raw_to_silver",
        python_callable=transform_raw_to_silver,
    )
