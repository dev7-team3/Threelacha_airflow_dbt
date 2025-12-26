from datetime import datetime
import json
import logging
from typing import Any, Optional

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connection_utils import get_storage_conn_id
import pandas as pd
from preprocessing import add_date_features, fix_cd_columns, normalize_price, prepare_metadata, upload_parquet_to_s3


@dag(
    dag_id="silver_api10_transform",
    schedule=None,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["preprocessing", "api10"],
)
def silver_api10_transform():
    """
    Silver layer API10 데이터 처리 DAG 정의 함수.

    전체 파이프라인을 TaskFlow API로 구성

    task 1: [extract_json] s3에서 raw 데이터 추출
    task 2: [transform]raw 데이터 전처리
    task 3: [join_metadata] metadata와 join하여 메타정보 merge
    task 4: [upload] s3의 silver/api10/으로 업로드

    Returns:
        None
    """
    s3_conn_id = get_storage_conn_id()
    bucket = "team3-batch"
    meta_key = "metadata/dim_product_no.csv"

    @task
    def extract_json() -> pd.DataFrame:
        """
        S3(MinIO)에서 실행일자(dt=YYYYMMDD)에 해당하는 data.json 파일들을 읽어 DataFrame으로 변환한다.

        Returns:
            pd.DataFrame: 추출된 데이터프레임.
        """
        hook = S3Hook(aws_conn_id=s3_conn_id)
        records = []

        def safe_value(val: Any) -> Optional[Any]:
            """리스트([])는 None으로 변환, 리스트에 값이 있으면 첫 번째 값 사용"""
            if isinstance(val, list):
                return None if len(val) == 0 else val[0]
            return val

        # 모든 키 가져오기
        all_keys = hook.list_keys(bucket_name=bucket, prefix="raw/api-10/")
        dt_values = []

        # 키에서 dt=YYYYMMDD 추출
        dt_values = [p.replace("dt=", "") for key in all_keys for p in key.split("/") if p.startswith("dt=")]

        if not dt_values:
            raise ValueError("S3에 dt=YYYYMMDD 폴더가 없습니다.")

        # 가장 최신 날짜 선택
        latest_dt = max(dt_values)
        prefix = f"raw/api-10/dt={latest_dt}/product_cls=01/"

        for obj in hook.list_keys(bucket_name=bucket, prefix=prefix):
            if obj.endswith("data.json"):
                body = hook.read_key(key=obj, bucket_name=bucket)
                try:
                    data = json.loads(body)
                    price_data = data.get("price", [])
                    if isinstance(price_data, list):
                        for item in price_data:
                            record = {
                                k: safe_value(v)
                                for k, v in {
                                    "key": obj,
                                    "error_code": data.get("error_code"),
                                    "condition": data.get("condition"),
                                    **item,
                                }.items()
                            }
                            records.append(record)
                    else:
                        record = {
                            "key": obj,
                            "error_code": data.get("error_code"),
                            "condition": data.get("condition"),
                            "price": safe_value(price_data),
                        }
                        records.append(record)
                except Exception:
                    logging.exception(f"[extract_json] JSON 파싱 실패: {obj}")

        df = pd.DataFrame(records)
        logging.info(f"🤖[extract_json] → 총 {len(df)}행")
        return df

    @task
    def transform(df: pd.DataFrame) -> dict:
        """
        DataFrame 전처리 및 분할 (main, livestock, grocery).

        Args:
            df (pd.DataFrame): 원본 데이터프레임.

        Returns:
            dict: main, livestock, grocery 데이터프레임 딕셔너리.
        """
        logging.info(f"🤖[transform] 초기 shape={df.shape}")

        # 품목명 분리
        df[["item_nm", "kind_nm"]] = df["productName"].str.split("/", n=1, expand=True)

        # 가격 정규화
        for col in ["dpr1", "dpr2", "dpr3", "dpr4"]:
            df[col] = normalize_price(df[col])

        # 날짜 파생 컬럼 추가
        df = add_date_features(df, "lastest_day")

        logging.info(
            f"🤖[transform] 1차 전처리 확인: {df[['item_nm', 'kind_nm', 'dpr1', 'dpr3', 'lastest_day']].head(2)}"
        )

        # 컬럼명 표준화
        df = df.rename(
            columns={
                "product_cls_code": "product_cls_cd",
                "product_cls_name": "product_cls_nm",
                "county_code": "country_cd",
                "county_name": "country_nm",
                "category_code": "category_cd",
                "category_name": "category_nm",
                "productno": "product_no",
                "lastest_day": "res_dt",
                "unit": "product_cls_unit",
                "day1": "base_dt",
                "dpr1": "base_pr",
                "day2": "prev_1d_dt",
                "dpr2": "prev_1d_pr",
                "day3": "prev_1m_dt",
                "dpr3": "prev_1m_pr",
                "day4": "prev_1y_dt",
                "dpr4": "prev_1y_pr",
                "direction": "direction_tp",
                "value": "direction_pct",
            }
        )

        # 데이터 타입 처리
        df["product_no"] = df["product_no"].astype("Int64")
        df = fix_cd_columns(df)

        # 필터링
        df = df[df["product_cls_cd"] == "01"]
        logging.info(f"🤖[transform] 필터링 후 shape={df.shape}")

        # 분할
        main_df = df[df["category_cd"].isin(["100", "200", "300", "400", "600"])]
        livestock_df = df[df["category_cd"] == "500"]
        grocery_df = df[df["category_cd"] == "800"]
        logging.info(f"🤖[transform] main={main_df.shape}, livestock={livestock_df.shape}, grocery={grocery_df.shape}")

        return {"main": main_df, "livestock": livestock_df, "grocery": grocery_df}

    @task
    def join_metadata(dfs: dict) -> dict:
        """
        메타데이터 조인 (main_df만), 나머지는 컬럼 순서만 맞춤.

        Args:
            dfs (dict): main, livestock, grocery 데이터프레임 딕셔너리.

        Returns:
            dict: 메타데이터 조인 후 데이터프레임 딕셔너리.
        """
        main_df = dfs["main"]
        livestock_df = dfs["livestock"]
        grocery_df = dfs["grocery"]

        hook = S3Hook(aws_conn_id=s3_conn_id)
        meta_body = hook.read_key(key=meta_key, bucket_name=bucket)
        meta_df = prepare_metadata(meta_body)

        meta_df = meta_df[["product_no", "item_cd", "kind_cd", "rank_cd", "rank_nm"]]
        meta_df.loc[len(meta_df)] = [3021, "616", "01", "21", "中"]

        merged = pd.merge(main_df, meta_df, on=["product_no"], how="left")

        desired_order = [
            # 품목 정보
            "product_no",
            "product_cls_cd",
            "product_cls_nm",
            "category_cd",
            "category_nm",
            "item_cd",
            "item_nm",
            "kind_cd",
            "kind_nm",
            "product_cls_unit",
            # 가격 정보
            "base_dt",
            "base_pr",
            "prev_1d_dt",
            "prev_1d_pr",
            "direction_tp",
            "direction_pct",
            "prev_1m_dt",
            "prev_1m_pr",
            "prev_1y_dt",
            "prev_1y_pr",
            # 지역 정보
            "country_cd",
            "country_nm",
            # 날짜 정보
            "res_dt",
            "year",
            "month",
            "week_of_year",
            "weekday_num",
            "weekday_nm",
            "weekend_yn",
        ]

        merged = merged[[c for c in desired_order if c in merged.columns]]
        livestock_df = livestock_df[[c for c in desired_order if c in livestock_df.columns]]
        grocery_df = grocery_df[[c for c in desired_order if c in grocery_df.columns]]

        logging.info("[join_metadata] 메타데이터 조인 완료")
        return {"main": merged, "livestock": livestock_df, "grocery": grocery_df}

    @task
    def upload(dfs: dict) -> None:
        """
        최종 DataFrame들을 Parquet으로 변환하여 S3에 업로드한다.

        Args:
            dfs (dict): main, livestock, grocery 데이터프레임 딕셔너리.

        Returns:
            None
        """
        for name, df in dfs.items():
            base_prefix = f"silver/api-10/{name}"
            upload_parquet_to_s3(
                df=df,
                bucket=bucket,
                base_prefix=base_prefix,
                aws_conn_id=s3_conn_id,
            )
            logging.info(f"[upload] {name} 업로드 완료")

    # DAG 실행 흐름
    raw_df = extract_json()
    dfs = transform(raw_df)
    dfs_final = join_metadata(dfs)
    upload(dfs_final)


silver_api10_transform()
