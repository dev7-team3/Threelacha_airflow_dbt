from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logger = logging.getLogger(__name__)


def upload_json_to_s3(hook: S3Hook, json_data: str, object_name: str, bucket_name: str) -> None:
    """JSON 데이터를 S3에 업로드

    Args:
        json_data: 업로드할 JSON 데이터
        object_name: S3 객체 키 (경로)
        bucket_name: S3 버킷명
    """
    try:
        hook.load_string(string_data=json_data, key=object_name, bucket_name=bucket_name, replace=True)
        logger.info(f"✅ Uploaded JSON file: {object_name}")
    except Exception as e:
        logger.warning(f"Error uploading {object_name}: {e}")
        raise


def upload_csv_to_s3(hook: S3Hook, df: pd.DataFrame, object_name: str, bucket_name: str) -> None:
    """DataFrame을 CSV로 변환하여 S3에 업로드

    Args:
        hook: S3Hook 인스턴스
        df: 업로드할 DataFrame
        object_name: S3 객체 키 (경로)
        bucket_name: S3 버킷명
    """
    try:
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=object_name,
            bucket_name=bucket_name,
            replace=True,
        )
        logger.info(f"✅ Uploaded CSV file: {object_name} ({len(df):,} records)")
    except Exception as e:
        logger.warning(f"Error uploading {object_name}: {e}")
        raise


def upload_parquet_to_s3(hook: S3Hook, df: pd.DataFrame, object_name: str, bucket_name: str) -> None:
    """DataFrame을 Parquet로 변환하여 S3에 업로드

    Args:
        hook: S3Hook 인스턴스
        df: 업로드할 DataFrame
        object_name: S3 객체 키 (경로)
        bucket_name: S3 버킷명
    """
    try:
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=object_name,
            bucket_name=bucket_name,
            replace=True,
        )

        logger.info(f"✅ Uploaded Parquet file: {object_name} ({len(df):,} records)")
    except Exception as e:
        logger.warning(f"Error uploading {object_name}: {e}")
        raise
