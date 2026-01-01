from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
from io import BytesIO
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def read_json_from_s3(hook: S3Hook, object_name: str, bucket_name: str) -> list | None:
    """S3에서 JSON 파일 읽기

    Args:
        hook: S3Hook 인스턴스
        object_name: S3 객체 키 (경로)
        bucket_name: S3 버킷명
    """
    try:
        response = hook.get_key(key=object_name, bucket_name=bucket_name)
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


def read_parquet_from_s3(hook: S3Hook, object_name: str, bucket_name: str) -> pd.DataFrame | None:
    """S3에서 Parquet 파일 읽기

    Args:
        hook: S3Hook 인스턴스
        object_name: S3 객체 키 (경로)
        bucket_name: S3 버킷명
    """
    try:
        response = hook.get_key(key=object_name, bucket_name=bucket_name)
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


def read_csv_from_s3(hook: S3Hook, object_name: str, bucket_name: str) -> pd.DataFrame | None:
    """S3에서 CSV 파일 읽기

    Args:
        hook: S3Hook 인스턴스
        object_name: S3 객체 키 (경로)
        bucket_name: S3 버킷명
    """
    try:
        response = hook.get_key(key=object_name, bucket_name=bucket_name)
        if response is None:
            return None
        return pd.read_csv(response.get()["Body"])
    except Exception as e:
        logger.warning(f"Error reading {object_name}: {e}")
        return None
