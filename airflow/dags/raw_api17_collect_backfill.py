from datetime import datetime
import logging

from airflow.decorators import dag, task
from api_caller_utils import call_kamis_api, validate_api17_response
from config.constants import BUCKET_NAME
from connection_utils import get_storage_conn_id
from metadata_loader_utils import MetadataLoader, generate_api17_params
import pendulum
from s3_uploader_utils import build_s3_path, upload_json_to_s3

logger = logging.getLogger(__name__)

# 상수 정의
S3_CONN_ID = get_storage_conn_id()
API17_ACTION = "periodRetailProductList"

# 코드 매핑 데이터 로드
country_code_mapping = MetadataLoader.get_country_codes()


def get_data(
    region: str,
    item_category_code: str,
    item_code: str,
    kind_code: str,
    product_rank_code: str,
    start_day: str,
    end_day: str,
) -> dict:
    """
    API를 호출하여 데이터를 가져옵니다.

    Args:
        region: 지역 코드
        item_category_code: 카테고리 코드
        item_code: 품목 코드
        kind_code: 품종 코드
        product_rank_code: 판매코드
        start_day: 시작 날짜 (YYYY-MM-DD)
        end_day: 종료 날짜 (YYYY-MM-DD)

    Returns:
        API 응답 데이터 (JSON)
    """
    params = {
        "p_countrycode": region,
        "p_convert_kg_yn": "N",
        "p_startday": start_day,
        "p_endday": end_day,
        "p_itemcategorycode": item_category_code,
        "p_itemcode": item_code,
        "p_kindcode": kind_code,
        "p_productrankcode": product_rank_code,
    }

    try:
        response = call_kamis_api(action=API17_ACTION, params=params, timeout=30)
        validate_api17_response(response)
    except Exception as e:
        logger.warning(f"❌ API 호출 실패: {e}")
        raise
    return response


def upload_data_to_s3(region: str, data: dict, category_row: dict, year: str) -> str:
    """
    데이터를 S3에 업로드합니다.

    Args:
        region: 지역 코드
        data: API 응답 데이터
        category_row: 카테고리, 품목, 품종, 판매코드 정보
        year: 연도 (YYYY)

    Returns:
        업로드된 S3 키
    """
    # API 응답에서 item 리스트 추출
    items = data.get("data", {}).get("item", [])
    if not isinstance(items, list):
        items = [items] if items else []

    if not items:
        logger.warning(f"⚠️ No items found for {region} {category_row}")
        return ""

    product_cls = "01"
    item_category_code = category_row["item_category_code"]
    item_code = category_row["item_code"]
    kind_code = category_row["kind_code"]
    product_rank_code = category_row["product_rank_code"]

    key = build_s3_path(
        api_number="17",
        dt=year,
        product_cls=product_cls,
        country=region,
        category=item_category_code,
        item=item_code,
        kind=kind_code,
        product_rank=product_rank_code,
    )

    upload_json_to_s3(data=items, s3_key=key, bucket_name=BUCKET_NAME)

    logger.info(f"✅ Uploaded: {key} ({len(items)} items)")
    return key


@dag(
    dag_id="raw_api17_collect_yearly",
    start_date=datetime(2025, 12, 10),
    schedule=None,  # 수동 실행
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(hours=1),
    },
    tags=["ingestion", "api17", "backfill"],
    description="KAMIS API17 연간 데이터 백필 DAG",
)
def raw_api17_collect_yearly():
    """
    KAMIS API17 연간 데이터 백필 DAG

    각 지역별로 병렬로 연간 데이터를 수집하여 S3 Raw 레이어에 저장합니다.
    TaskFlow API의 expand 기능을 사용하여 동적으로 task를 생성합니다.

    Returns:
        None
    """
    start_day = "2025-01-01"
    end_day = "2025-12-31"
    year = "2025"  # 백필 대상 연도

    @task
    def collect_yearly_data_by_region(region: str) -> None:
        """
        특정 지역의 연간 데이터를 수집합니다.

        Args:
            region: 지역 코드

        Returns:
            None
        """
        for category_row in generate_api17_params():
            try:
                data = get_data(
                    region=region,
                    item_category_code=category_row["item_category_code"],
                    item_code=category_row["item_code"],
                    kind_code=category_row["kind_code"],
                    product_rank_code=category_row["product_rank_code"],
                    start_day=start_day,
                    end_day=end_day,
                )

                upload_data_to_s3(region, data, category_row, year)

            except Exception as e:
                logger.warning(
                    f"❌ Error collecting data for {region} "
                    f"{category_row['item_category_code']}/{category_row['item_code']}/"
                    f"{category_row['kind_code']}/{category_row['product_rank_code']}: {e}"
                )
                continue

        logger.info(f"✅ Completed yearly data collection for region: {region}")

    # DAG 실행 흐름
    # 각 지역별로 병렬 task 생성
    country_codes = list(country_code_mapping.values())
    collect_yearly_data_by_region.expand(region=country_codes)


# DAG 인스턴스 생성
raw_api17_collect_yearly()
