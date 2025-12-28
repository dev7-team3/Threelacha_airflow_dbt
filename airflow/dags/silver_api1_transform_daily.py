from datetime import datetime
from io import BytesIO
import json
import logging
import re
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from connection_utils import get_storage_conn_id
import numpy as np
import pandas as pd
import pendulum

logger = logging.getLogger("airflow.task")

# ---------------------------------------------------------
# 상수 정의
# ---------------------------------------------------------
BUCKET_NAME = "team3-batch"
CONN_ID = get_storage_conn_id()

# ---------------------------------------------------------
# 유틸리티 함수
# ---------------------------------------------------------


def extract_metadata_from_path(file_path: str) -> Dict[str, str]:
    """파일 경로에서 메타데이터 추출

    Args:
        file_path: S3 파일 경로 (예: raw/api-1/dt=2025-01-01/product_cls=01/country=1101/category=100/data.json)

    Returns:
        메타데이터 딕셔너리 (product_cls_cd, category_cd, country_cd, res_dt)
    """
    product_cls = re.search(r"product_cls=(\w+)", file_path)
    category = re.search(r"category=(\w+)", file_path)
    country = re.search(r"country=(\w+)", file_path)
    regday = re.search(r"dt=(\d{4}-\d{2}-\d{2})", file_path)

    return {
        "product_cls_cd": product_cls.group(1) if product_cls else "",
        "category_cd": category.group(1) if category else "",
        "country_cd": country.group(1) if country else "",
        "res_dt": regday.group(1) if regday else "",
    }


def clean_price(value: Any) -> Optional[float]:
    """가격 데이터 정제 (쉼표 제거, 빈 값/'-' 처리)

    Args:
        value: 원본 가격 값 (예: "5,500", "-", "", None)

    Returns:
        정제된 가격 float형 (예: 5500.0) 또는 None
    """
    if pd.isna(value) or not str(value).strip() or str(value).strip() == "-":
        return None
    try:
        return float(str(value).strip().replace(",", ""))  # "5,500" -> 5500.0
    except ValueError:
        return None


# ---------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------


@dag(
    dag_id="silver_api1_transform_daily",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 12, 11, tz="UTC"),
    catchup=True,
    max_active_runs=10,
    default_args={
        "owner": "jungeun_park",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["KAMIS", "api-1", "silver", "transform"],
    description="KAMIS API1 Raw 데이터를 읽어 코드 매핑 및 파생 컬럼을 추가한 Silver 데이터로 변환",
)
def transform_api1_raw_to_silver() -> None:
    """
    KAMIS API1 Raw → Silver 변환 DAG

    처리 흐름:
    1. Raw 데이터 파일 리스트 조회 (S3)
    2. JSON 파일 읽기 및 파싱
    3. 코드 매핑 및 파생 컬럼 생성 (요일, 주차 등)
    4. Parquet 형식으로 Silver 레이어에 저장 (날짜별 파일)

    Note:
    - day1~day7 컬럼은 "당일 (12/11)", "1일전 (12/10)" 같은 형식으로 문자열 그대로 저장
    - 실제 날짜 기준은 res_dt 컬럼 사용
    """

    @task
    def list_raw_files(target_date: str) -> List[str]:
        """S3에서 처리 대상 파일 목록 추출

        Args:
            target_date: 처리 대상 날짜 (YYYY-MM-DD)

        Returns:
            S3 파일 키 리스트

        Raises:
            AirflowSkipException: 파일이 없는 경우
        """
        logger.info(f"📋 {target_date}의 파일 리스트 조회 시작")

        s3_hook = S3Hook(aws_conn_id=CONN_ID)
        prefix = f"raw/api-1/dt={target_date}/"
        all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
        # data.json 파일만 필터링
        keys = [key for key in (all_keys or []) if key.endswith("data.json")]

        if not keys:
            logger.warning(f"⚠️ No raw files found for date: {target_date}")
            raise AirflowSkipException(f"No raw files found for date: {target_date}")

        logger.info(f"✅ {len(keys)}개의 JSON 파일 발견")
        return keys

    @task
    def read_and_parse(file_keys: List[str]) -> List[Dict[str, Any]]:
        """JSON 파일을 읽어서 기본 레코드 리스트로 변환

        Args:
            file_keys: S3 파일 키 리스트

        Returns:
            파싱된 레코드 리스트 (각 레코드는 품목의 가격 정보를 담은 딕셔너리)

        Note:
            - data가 리스트인 경우(에러 응답) 스킵
            - day1~day7: 문자열 그대로 저장 (예: "당일 (11/01)", "1주일전 (09/24)")
            - dpr1~dpr7: 가격 정제 후 저장 (쉼표 제거, None 처리)
        """
        logger.info(f"🔄 {len(file_keys)}개 파일 파싱 시작")

        s3_hook = S3Hook(aws_conn_id=CONN_ID)

        all_records = []
        skipped_files = 0

        for i, key in enumerate(file_keys, 1):
            try:
                content_str = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
                content = json.loads(content_str)

                metadata = extract_metadata_from_path(key)

                # data 섹션이 리스트(에러 응답)인 경우 스킵
                data_section = content.get("data", {})
                if isinstance(data_section, list):
                    logger.debug(f"Skipping file with list data (code: {data_section}): {key}")
                    skipped_files += 1
                    continue

                items = data_section.get("item", [])
                if not isinstance(items, list):
                    items = [items]

                for item in items:
                    record = {
                        # 메타데이터
                        "res_dt": metadata["res_dt"],
                        "product_cls_cd": metadata["product_cls_cd"],
                        "category_cd": metadata["category_cd"],
                        "country_cd": metadata["country_cd"],
                        # 품목 정보
                        "item_nm": str(item.get("item_name", "")).strip(),
                        "item_cd": str(item.get("item_code", "")).strip(),
                        "kind_nm": str(item.get("kind_name", "")).strip(),
                        "kind_cd": str(item.get("kind_code", "")).strip(),
                        "rank_nm": str(item.get("rank", "")).strip(),
                        "rank_cd": str(item.get("rank_code", "")).strip(),
                        "unit": str(item.get("unit", "")).strip(),
                        # 날짜 라벨 (문자열 그대로: "당일 (10/01)", "1일전 (09/30)" 등)
                        "base_dt": str(item.get("day1", "")).strip(),
                        "prev_1d_dt": str(item.get("day2", "")).strip(),
                        "prev_1w_dt": str(item.get("day3", "")).strip(),
                        "prev_2w_dt": str(item.get("day4", "")).strip(),
                        "prev_1m_dt": str(item.get("day5", "")).strip(),
                        "prev_1y_dt": str(item.get("day6", "")).strip(),
                        "avg_tp": str(item.get("day7", "")).strip(),
                        # 가격 정보 (쉼표 제거, None 처리)
                        "base_pr": clean_price(item.get("dpr1", "")),
                        "prev_1d_pr": clean_price(item.get("dpr2", "")),
                        "prev_1w_pr": clean_price(item.get("dpr3", "")),
                        "prev_2w_pr": clean_price(item.get("dpr4", "")),
                        "prev_1m_pr": clean_price(item.get("dpr5", "")),
                        "prev_1y_pr": clean_price(item.get("dpr6", "")),
                        "avg_pr": clean_price(item.get("dpr7", "")),
                    }
                    all_records.append(record)

                if i % 10 == 0:
                    logger.info(f"   진행률: {i}/{len(file_keys)} ({i / len(file_keys) * 100:.1f}%)")

            except Exception as e:
                logger.exception(f"파일 처리 실패: {key} | 에러: {e!s}")  # noqa: TRY401
                continue

        logger.info(f"✅ {len(all_records):,}개 레코드 파싱 완료 (스킵된 파일: {skipped_files}개)")
        return all_records

    @task
    def enrich_data(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """코드 매핑 및 파생 컬럼 생성

        Args:
            records: 원본 레코드 리스트

        Returns:
            enrichment된 데이터프레임

        처리 내용:
        - 코드 → 명칭 매핑 (상품분류, 카테고리, 지역)
        - 요일 정보 추가 (weekday_num: 0~6, weekday_nm: 월~일, weekend_yn)
        - 주차 정보 추가 (week_of_year: ISO 8601 기준)
        - base_pr이 None인 레코드 제거
        - res_dt를 date 타입으로 변환 (시분초 없음)
        """
        # 컬럼 순서 정의
        column_order = [
            # 날짜/시간 정보
            "res_dt",
            "week_of_year",
            "weekday_num",
            "weekday_nm",
            "weekend_yn",
            # 상품 분류
            "product_cls_cd",
            "product_cls_nm",
            "category_cd",
            "category_nm",
            # 지역
            "country_cd",
            "country_nm",
            # 품목 정보
            "item_nm",
            "item_cd",
            "kind_nm",
            "kind_cd",
            "rank_nm",
            "rank_cd",
            "unit",
            # 당일 정보
            "base_dt",
            "base_pr",
            # 1일전
            "prev_1d_dt",
            "prev_1d_pr",
            # 1주일전
            "prev_1w_dt",
            "prev_1w_pr",
            # 2주일전
            "prev_2w_dt",
            "prev_2w_pr",
            # 1개월전
            "prev_1m_dt",
            "prev_1m_pr",
            # 1년전
            "prev_1y_dt",
            "prev_1y_pr",
            # 평년
            "avg_tp",
            "avg_pr",
        ]

        if not records:
            logger.warning("⚠️ 빈 레코드 리스트")
            return pd.DataFrame(columns=column_order)

        logger.info(f"🏷️ {len(records):,}개 레코드 enrichment 시작")

        df = pd.DataFrame(records)
        df["res_dt"] = pd.to_datetime(df["res_dt"])

        # 코드 → 명칭 매핑
        product_cls_map = {"01": "소매", "02": "도매"}
        category_map = {
            "100": "식량작물",
            "200": "채소류",
            "300": "특용작물",
            "400": "과일류",
            "500": "축산물",
            "600": "수산물",
        }
        country_map = {
            "1101": "서울",
            "2100": "부산",
            "2200": "대구",
            "2401": "광주",
            "2501": "대전",
            "all": "전체지역",
        }

        df["product_cls_nm"] = df["product_cls_cd"].map(product_cls_map).fillna("미분류")
        df["category_nm"] = df["category_cd"].map(category_map).fillna("미분류")
        df["country_nm"] = df["country_cd"].map(country_map).fillna("기타")

        # 요일 정보 (pandas 기본 값: 0=월요일, 6=일요일)
        df["weekday_num"] = df["res_dt"].dt.dayofweek

        weekday_map = {0: "월요일", 1: "화요일", 2: "수요일", 3: "목요일", 4: "금요일", 5: "토요일", 6: "일요일"}
        df["weekday_nm"] = df["weekday_num"].map(weekday_map)
        df["weekend_yn"] = df["weekday_num"].isin([5, 6])  # 토요일(5), 일요일(6)

        # 주차 정보 (ISO 8601 기준)
        df["week_of_year"] = df["res_dt"].dt.isocalendar().week.astype(np.int32)

        # 필수 데이터 필터링 (base_pr이 없는 레코드 제거)
        initial_count = len(df)
        df = df.dropna(subset=["base_pr"])
        removed_count = initial_count - len(df)

        if removed_count > 0:
            logger.info(f"🧹 base_pr NA 제거: {removed_count:,}개 레코드 삭제")
        logger.info(f"✅ Enrichment 완료: {len(df):,}개 레코드")

        # res_dt를 date 타입으로 변환 (시분초 없는 날짜만)
        df["res_dt"] = df["res_dt"].dt.date

        return df[column_order]

    @task
    def save_parquet(df: pd.DataFrame, target_date: str) -> Dict[str, Any]:
        """S3에 최종 Parquet 저장 (날짜별 개별 파일)

        Args:
            df: 저장할 데이터프레임
            target_date: 처리 대상 날짜 (YYYY-MM-DD)

        Returns:
            처리 결과 정보 딕셔너리

        저장 전략:
        - 경로: silver/api-1/year=YYYY/month=MM/data_YYYYMMDD.parquet
        - 날짜별로 별도 파일 저장
        - 같은 날짜 재처리 시 해당 파일만 덮어쓰기
        - 다른 날짜 데이터는 영향 없음 (안전)
        """
        if df.empty:
            logger.warning("⚠️ 저장할 데이터가 없습니다")
            return {"date": target_date, "record_count": 0, "status": "no_data"}

        logger.info(f"💾 Parquet 저장 시작: {len(df):,}개 레코드")

        s3_hook = S3Hook(aws_conn_id=CONN_ID)
        dt_obj = datetime.strptime(target_date, "%Y-%m-%d")

        # 날짜별 개별 파일로 저장
        path = f"silver/api-1/year={dt_obj.strftime('%Y')}/month={dt_obj.strftime('%m')}/"
        file_key = f"{path}data_{target_date.replace('-', '')}.parquet"

        # Parquet를 메모리 버퍼에 저장
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        s3_hook.load_bytes(bytes_data=buffer.getvalue(), key=file_key, bucket_name=BUCKET_NAME, replace=True)

        logger.info(f"✅ Saved to: s3://{BUCKET_NAME}/{file_key}")
        logger.info(f"   Records: {len(df):,}개")
        logger.info("   Strategy: 날짜별 개별 파일 (다른 날짜 데이터 안전)")

        # 샘플 데이터 로깅 - 기본 정보
        sample_basic = df.head(3)[["res_dt", "week_of_year", "weekday_nm", "category_nm", "item_nm"]]
        logger.info(f"\n📋 저장된 데이터 샘플 (기본 정보):\n{sample_basic.to_string()}")

        # 샘플 데이터 로깅 - 가격 정보 (날짜 라벨 포함)
        sample_price = df.head(3)[["item_nm", "base_dt", "base_pr", "prev_1d_dt", "prev_1d_pr"]]
        logger.info(f"\n💰 저장된 데이터 샘플 (가격 정보):\n{sample_price.to_string()}")

        return {"date": target_date, "record_count": len(df), "file_key": file_key, "status": "success"}

    # --- DAG Flow ---
    # data_interval_start - 1일 = 전일 데이터 처리
    target_date = "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

    file_keys = list_raw_files(target_date)
    raw_records = read_and_parse(file_keys)
    enriched_df = enrich_data(raw_records)
    save_parquet(enriched_df, target_date)


transform_api1_raw_to_silver()
