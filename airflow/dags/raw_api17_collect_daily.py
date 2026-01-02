from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from api_caller_utils import call_kamis_api, validate_api17_response
from connection_utils import get_storage_conn_id
from metadata_loader_utils import MetadataLoader, generate_api17_params
from s3_uploader_utils import build_s3_path, upload_json_to_s3

logger = logging.getLogger(__name__)

# 상수 정의
S3_CONN_ID = get_storage_conn_id()
API17_ACTION = "periodRetailProductList"

# 코드 매핑 데이터 로드
country_code_mapping = MetadataLoader.get_country_codes(wholesale_only=True)


def group_data_by_date(data: dict) -> dict[str, list] | None:
    """
    날짜별로 데이터를 그룹화합니다.

    Args:
        data: API 응답 데이터

    Returns:
        날짜별 데이터 딕셔너리. 오류 발생 시 None 반환
    """
    grouped = {}

    # API 응답 검증
    validate_api17_response(data)

    items = data.get("data", {}).get("item", [])
    if not isinstance(items, list):
        items = [items] if items else []

    for item in items:
        yyyy = item.get("yyyy", "")
        regday = item.get("regday", "")

        if yyyy and regday:
            # "12/17" -> "12-17" (MM/DD -> MM-DD)
            month_day = regday.replace("/", "-")
            date_str = f"{yyyy}-{month_day}"  # "2025-12-17"

            if date_str not in grouped:
                grouped[date_str] = []
            grouped[date_str].append(item)

    if not grouped:
        return None

    return grouped


def get_data(
    country_code: str,
    item_category_code: str,
    item_code: str,
    kind_code: str,
    product_rank_code: str,
    start_day: str,
    end_day: str,
) -> dict | None:
    """
    API를 호출하여 데이터를 가져옵니다.

    Args:
        country_code: 도시 코드
        item_category_code: 카테고리 코드
        item_code: 품목 코드
        kind_code: 품종 코드
        product_rank_code: 판매코드
        start_day: 시작 날짜 (YYYY-MM-DD)
        end_day: 종료 날짜 (YYYY-MM-DD)

    Returns:
        날짜별 데이터. 오류 발생 시 None 반환
    """
    params = {
        "p_startday": start_day,
        "p_endday": end_day,
        "p_countrycode": country_code,
        "p_convert_kg_yn": "N",
        "p_itemcategorycode": item_category_code,
        "p_itemcode": item_code,
        "p_kindcode": kind_code,
        "p_productrankcode": product_rank_code,
    }

    logger.info(f"🔄 Getting data for country={country_code}, category={item_category_code}")

    try:
        response = call_kamis_api(action=API17_ACTION, params=params, timeout=30)

        logger.info(response)
        return group_data_by_date(response)
    except Exception as e:
        logger.warning(f"❌ API 호출 실패: {e}")
        return None


def upload_data_to_s3(
    country_code: str,
    date_data: dict[str, list],
    category_info: dict,
) -> None:
    """
    날짜별 데이터를 S3에 업로드합니다.

    Args:
        country_code: 도시 코드
        date_data: 날짜별 데이터
        category_info: 카테고리, 품목, 품종, 판매코드 정보
    """
    product_cls = "01"
    item_category_code = category_info["item_category_code"]
    item_code = category_info["item_code"]
    kind_code = category_info["kind_code"]
    product_rank_code = category_info["product_rank_code"]

    for date_str, data in date_data.items():
        key = build_s3_path(
            api_number="17",
            dt=date_str,
            product_cls=product_cls,
            country=country_code,
            category=item_category_code,
            item=item_code,
            kind=kind_code,
            product_rank=product_rank_code,
        )

        upload_json_to_s3(data=data, s3_key=key)


@dag(
    dag_id="raw_api17_collect_daily",
    start_date=datetime(2025, 12, 10),
    schedule="0 5 * * *",  # 매일 오전 5시
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "api17"],
    description="KAMIS API17 소매 가격 데이터 수집 DAG",
)
def raw_api17_collect_daily():
    """
    KAMIS API17 Raw 데이터 수집 DAG

    각 지역별로 병렬로 데이터를 수집하여 S3 Raw 레이어에 저장합니다.
    TaskFlow API의 expand 기능을 사용하여 동적으로 task를 생성합니다.

    Returns:
        None
    """

    @task
    def collect_data_by_country(country_code: str, **context) -> None:
        """
        특정 지역의 데이터를 수집합니다.

        Args:
            country_code: 도시 코드
            **context: Airflow 실행 컨텍스트

        Returns:
            None
        """
        # date_info를 context에서 가져오기
        logical_date = context.get("logical_date") or context.get("data_interval_start")
        if logical_date is None:
            raise ValueError("logical_date 또는 data_interval_start를 찾을 수 없습니다.")

        start_day = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        end_day = logical_date.strftime("%Y-%m-%d")

        for category in generate_api17_params():
            data_from_api = get_data(
                country_code=country_code,
                item_category_code=category["item_category_code"],
                item_code=category["item_code"],
                kind_code=category["kind_code"],
                product_rank_code=category["product_rank_code"],
                start_day=start_day,
                end_day=end_day,
            )

            if data_from_api is None:
                logger.warning(
                    f"❌ No data found for {country_code} {start_day}~{end_day} "
                    f"{category['item_category_code']}/{category['item_code']}/"
                    f"{category['kind_code']}/{category['product_rank_code']}"
                )
                continue

            upload_data_to_s3(
                country_code=country_code,
                date_data=data_from_api,
                category_info=category,
            )

        logger.info(f"✅ Completed data collection for country: {country_code}")

    # DAG 실행 흐름
    # 각 지역별로 병렬 task 생성 (date_info는 context에서 가져옴)
    country_codes = list(country_code_mapping.keys())
    logger.info(f"지역 코드 목록: {country_codes} (총 {len(country_codes)}개)")
    collect_data_by_country.expand(country_code=country_codes)


# DAG 인스턴스 생성
raw_api17_collect_daily()
