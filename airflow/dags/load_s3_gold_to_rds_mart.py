import io
import logging
from typing import List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from connection_utils import get_dbms_conn_id, get_storage_conn_id
import pandas as pd
import pendulum
import pyarrow.parquet as pq

# [환경 설정]
BUCKET_NAME = "team3-batch"
GOLD_PREFIX = "gold/team3_gold"
RDS_CONN_ID = get_dbms_conn_id()
S3_CONN_ID = get_storage_conn_id()
TARGET_SCHEMA = "mart"
AWS_REGION = "ap-northeast-2"

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_s3_gold_to_rds_mart",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"owner": "jungeun_park", "retries": 2},
    tags=["mart", "rds", "recursive_s3"],
)
def sync_gold_to_rds():
    """
    S3 Gold 레이어의 Parquet 데이터를 RDS Mart 스키마로 Full Load하는 파이프라인.

    S3의 디렉토리 구조를 기반으로 테이블 목록을 동적으로 파악하고,
    각 테이블 내의 모든 하위 파티션 파일을 재귀적으로 탐색하여 적재합니다.
    트랜잭션을 통해 데이터 일관성을 보장하며, 실패 시 롤백을 수행합니다.
    """

    @task
    def get_table_list() -> List[str]:
        """S3 최상위 디렉토리에서 테이블 목록 추출.

        GOLD_PREFIX 경로 바로 아래에 있는 디렉토리명들을 테이블 이름으로 간주합니다.

        Returns:
            List[str]: S3 디렉토리 구조에서 추출한 테이블 이름 리스트.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        client = s3_hook.get_conn()
        response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{GOLD_PREFIX}/", Delimiter="/")
        return [p["Prefix"].split("/")[-2] for p in response.get("CommonPrefixes", [])]

    @task
    def prepare_environment():
        """대상 RDS 데이터베이스의 Mart 스키마 준비.

        데이터 적재 전, 대상 스키마가 존재하지 않을 경우 생성합니다.
        """
        rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
        rds_hook.run(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")

    @task
    def process_table_sync(table_name: str):
        """특정 테이블의 데이터를 S3에서 RDS로 트랜잭션 적재.

        1. S3 하위 모든 Parquet 파일 탐색
        2. 샘플 파일 기반 PostgreSQL 스키마 생성
        3. 단일 트랜잭션 내에서 TRUNCATE 및 COPY 실행
        4. 데이터 통계 갱신(ANALYZE) 및 커밋

        Args:
            table_name (str): 적재할 대상 테이블의 이름 (S3 디렉토리명).

        Raises:
            Exception: 데이터 적재 중 오류 발생 시 트랜잭션을 롤백하고 예외를 발생시킴.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)

        # 1. 재귀적 파일 탐색
        prefix = f"{GOLD_PREFIX}/{table_name}/"
        all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
        parquet_files = [k for k in all_keys if not k.endswith("/")] if all_keys else []

        if not parquet_files:
            logger.warning(f"[{table_name}] 하위 디렉토리에 파일이 없습니다.")
            return

        logger.info(f"[{table_name}] 총 {len(parquet_files)}개의 파일을 발견했습니다. (하위 디렉토리 포함)")

        # 2. 스키마 분석 (첫 번째 파일 활용)
        obj = s3_hook.get_key(parquet_files[0], bucket_name=BUCKET_NAME)
        buffer = io.BytesIO(obj.get()["Body"].read())
        schema = pq.ParquetFile(buffer).schema_arrow

        columns = []
        for field in schema:
            at = str(field.type).lower()
            if "int64" in at:
                pg_t = "BIGINT"  # noqa: E701
            elif "int" in at:
                pg_t = "INTEGER"  # noqa: E701
            elif "double" in at or "float64" in at:
                pg_t = "DOUBLE PRECISION"  # noqa: E701
            elif "float" in at:
                pg_t = "REAL"  # noqa: E701
            elif "timestamp" in at:
                pg_t = "TIMESTAMP"  # noqa: E701
            elif "date" in at:
                pg_t = "DATE"  # noqa: E701
            elif "bool" in at:
                pg_t = "BOOLEAN"  # noqa: E701
            else:
                pg_t = "TEXT"  # noqa: E701
            columns.append(f'"{field.name}" {pg_t}')

        # 3. DB 트랜잭션 수행
        conn = rds_hook.get_conn()
        conn.autocommit = False  # 수동 트랜잭션 제어 활성화
        cursor = conn.cursor()

        try:
            # 테이블 구조 생성
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{table_name} ({', '.join(columns)});")

            # TRUNCATE: 트랜잭션 내에 있으므로 실패 시 롤백 가능
            logger.info(f"[{table_name}] 데이터 초기화 (TRUNCATE)")
            cursor.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.{table_name};")

            total_rows = 0
            for s3_key in parquet_files:
                # S3 -> Pandas -> CSV Buffer 로직
                file_obj = s3_hook.get_key(s3_key, bucket_name=BUCKET_NAME)
                df = pd.read_parquet(io.BytesIO(file_obj.get()["Body"].read()))

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)

                # copy_expert를 통한 대량 적재
                cursor.copy_expert(f"COPY {TARGET_SCHEMA}.{table_name} FROM STDIN WITH CSV", csv_buffer)
                total_rows += len(df)

            # 통계 정보 업데이트 및 커밋
            cursor.execute(f"ANALYZE {TARGET_SCHEMA}.{table_name};")
            conn.commit()
            logger.info(f"✅ [{table_name}] 전체 하위 파일 적재 완료 ({total_rows:,} rows)")

        except Exception as e:
            conn.rollback()  # 오류 발생 시 전체 취소
            logger.error(f"❌ [{table_name}] 에러 발생으로 롤백: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    # [Workflow 구성]
    tables = get_table_list()
    prepare_env = prepare_environment()

    # 동적 태스크 맵핑을 사용하여 테이블별 병렬 실행
    prepare_env >> process_table_sync.expand(table_name=tables)


# DAG 인스턴스 실행
sync_gold_to_rds()
