from datetime import datetime, timedelta
import json
import os
from pathlib import Path

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connection_utils import get_storage_conn_id
import dotenv
import requests

from airflow import DAG

dotenv.load_dotenv()

REQUEST_URL = "http://www.kamis.or.kr/service/price/xml.do?action=periodRetailProductList"

API_KEY = os.getenv("CERT_KEY")
ID = os.getenv("CERT_ID")
S3_CONN_ID = get_storage_conn_id()
BUCKET_NAME = "team3-batch"

with Path.open(Path(__file__).parent.parent / "plugins" / "param_tree.json", "r", encoding="utf-8") as f:
    params_tree = json.load(f)

with Path.open(Path(__file__).parent.parent / "plugins" / "country_code.json", "r", encoding="utf-8") as f:
    country_code = json.load(f)


def set_category_product_variety_retail_code() -> dict:
    """카테고리, 품목, 품종, 판매코드 정보를 설정

    Yields:
        Iterator[dict]: 카테고리, 품목, 품종, 판매코드 정보
    """
    for category in params_tree:
        for product in params_tree[category]["products"]:
            for variety in params_tree[category]["products"][product]["varieties"]:
                for retail_code in params_tree[category]["products"][product]["varieties"][variety]["retail_codes"]:
                    yield {
                        "item_category_code": category,
                        "item_code": product,
                        "kind_code": variety,
                        "product_rank_code": retail_code,
                    }


def get_data(
    region: str,
    item_category_code: str,
    item_code: str,
    kind_code: str,
    product_rank_code: str,
    start_day: str = "2025-12-10",
    end_day: str = "2025-12-14",
) -> dict:
    """API를 호출하여 데이터를 가져옴"""
    url = (
        f"{REQUEST_URL}&p_cert_key={API_KEY}&p_cert_id={ID}"
        f"&p_countrycode={region}&p_convert_kg_yn=N"
        f"&p_returntype=json&p_startday={start_day}&p_endday={end_day}"
        f"&p_itemcategorycode={item_category_code}"
        f"&p_itemcode={item_code}&p_kindcode={kind_code}"
        f"&p_productrankcode={product_rank_code}"
    )

    # http:// 사용 시 SSL 불필요
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Error: {e}") from e


def collect_yearly_data(region: str, start_day: str, end_day: str) -> list[dict]:
    """연간 데이터를 수집하여 S3에 업로드"""
    for category_row in set_category_product_variety_retail_code():
        data = get_data(
            item_category_code=category_row["item_category_code"],
            item_code=category_row["item_code"],
            kind_code=category_row["kind_code"],
            product_rank_code=category_row["product_rank_code"],
            start_day=start_day,
            end_day=end_day,
            region=region,
        )
        upload_data_to_s3(region, data, category_row)


def upload_data_to_s3(region: str, data: list[dict], category_row: dict) -> str:
    """데이터를 MinIO에 업로드 (지역/품목별 경로 구성)"""
    json_data = json.dumps(data, ensure_ascii=False)

    # 폴더 경로 구성: s3://team3-batch/raw/api-17/dt=YYYYMMDD/product_cls=01/category=100/country=1101/product_rank=04/data.json

    date_str = "2025"  # 백필에서 필요한 년도로 세팅해야함
    product_cls = "01"
    item_category_code = category_row["item_category_code"]
    item_code = category_row["item_code"]
    kind_code = category_row["kind_code"]
    product_rank_code = category_row["product_rank_code"]
    key = (
        f"raw/api-17/dt={date_str}/"
        f"product_cls={product_cls}/country={region}/category={item_category_code}/"
        f"item={item_code}/kind={kind_code}/product_rank={product_rank_code}/"
        f"data.json"
    )

    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    hook.load_string(
        string_data=json_data,
        key=key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    return key


with DAG(
    dag_id="raw_api17_collect_yearly",
    start_date=datetime(2025, 12, 10),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["api_ingestion"],
) as dag:
    for region in country_code.values():
        collect_yearly_data_task = PythonOperator(
            task_id=f"collect_yearly_data_{region}",
            python_callable=collect_yearly_data,
            op_kwargs={
                "region": region,
                "start_day": "2025-01-01",
                "end_day": "2025-12-22",
            },
        )
