import io
import logging
from typing import List, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from config.constants import BUCKET_NAME, GOLD_PREFIX, MART_PREFIX, MART_SCHEMA
from connection_utils import get_dbms_conn_id, get_storage_conn_id
import pandas as pd
import pendulum
import pyarrow.parquet as pq

logger = logging.getLogger("airflow.task")

# 연결 설정
RDS_CONN_ID = get_dbms_conn_id()
S3_CONN_ID = get_storage_conn_id()

# 칼럼 타입 매핑
ARROW_TO_POSTGRES_TYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "SMALLINT",
    "int8": "SMALLINT",
    "float64": "DOUBLE PRECISION",
    "double": "DOUBLE PRECISION",
    "float32": "REAL",
    "float": "REAL",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "bool": "BOOLEAN",
}


def map_arrow_type_to_postgres(arrow_type: str) -> str:
    """Arrow 타입을 PostgreSQL 타입으로 매핑.

    Args:
        arrow_type (str): Arrow 데이터 타입 문자열.

    Returns:
        str: 매핑된 PostgreSQL 타입.
    """
    arrow_type_lower = arrow_type.lower()

    for key, pg_type in ARROW_TO_POSTGRES_TYPE_MAP.items():
        if key in arrow_type_lower:
            return pg_type

    return "TEXT"


@dag(
    dag_id="load_s3_gold_to_rds_mart",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"owner": "jungeun_park"},
    tags=["mart", "rds", "s3", "gold", "synk"],
)
def sync_gold_to_rds():
    """S3 Gold 레이어의 Parquet 데이터를 RDS Mart 스키마로 Full Load하는 파이프라인.

    S3의 디렉토리 구조를 기반으로 테이블 목록을 동적으로 파악하고,
    각 테이블 내의 모든 하위 파티션 파일을 재귀적으로 탐색하여 적재합니다.
    트랜잭션을 통해 데이터 일관성을 보장하며, 실패 시 롤백을 수행합니다.
    """

    @task
    def get_table_list() -> List[str]:
        """S3 최상위 디렉토리에서 테이블 목록 추출.

        MART_PREFIX로 시작하는 디렉토리명만 대상으로 합니다.

        Returns:
            List[str]: 테이블 이름 리스트.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        client = s3_hook.get_conn()
        response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{GOLD_PREFIX}/", Delimiter="/")

        common_prefixes = response.get("CommonPrefixes", [])
        if not common_prefixes:
            logger.warning(f"S3 경로 {GOLD_PREFIX}에 테이블 디렉토리가 없습니다.")
            return []

        # s3 경로에서 필요 폴더명만 추출
        all_tables = [p["Prefix"].split("/")[-2] for p in common_prefixes]
        # Gold 마트 규칙(MART_PREFIX)에 맞는 테이블만 필터링
        tables = [t for t in all_tables if t.startswith(MART_PREFIX)]

        if not tables:
            logger.warning(f"'{MART_PREFIX}'로 시작하는 테이블을 찾을 수 없습니다. 전체 목록: {all_tables}")
            return []

        logger.info(f"총 {len(tables)}개의 mart 테이블을 발견했습니다: {tables}")
        return tables

    @task
    def prepare_environment():
        """대상 RDS 데이터베이스의 Mart 스키마 준비."""
        rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
        rds_hook.run(f"CREATE SCHEMA IF NOT EXISTS {MART_SCHEMA};")
        logger.info(f"스키마 {MART_SCHEMA} 준비 완료")

    @task
    def get_s3_files(table_name: str) -> Tuple[str, List[str]]:
        """테이블의 S3 데이터 파일 목록 조회.

        Args:
            table_name (str): 대상 테이블 이름.

        Returns:
            Tuple[str, List[str]]: (테이블명, 파일 경로 리스트).
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        prefix = f"{GOLD_PREFIX}/{table_name}/"
        all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)

        # 파일만 추출
        data_files = [k for k in (all_keys or []) if not k.endswith("/")]

        if not data_files:
            logger.warning(f"[{table_name}] 데이터 파일이 없습니다.")
            return (table_name, [])

        logger.info(f"[{table_name}] 총 {len(data_files)}개의 데이터 파일 발견")
        return (table_name, data_files)

    @task
    def create_table_structure(file_info: Tuple[str, List[str]]):
        """S3 파일 스키마를 분석하여 RDS 테이블 구조 생성.

        Args:
            file_info (Tuple[str, List[str]]): (테이블명, 파일 경로 리스트).
        """
        table_name, data_files = file_info

        if not data_files:
            logger.warning(f"[{table_name}] 파일이 없어 테이블 생성을 건너뜁니다.")
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)

        obj = s3_hook.get_key(data_files[0], bucket_name=BUCKET_NAME)
        buffer = io.BytesIO(obj.get()["Body"].read())
        schema = pq.ParquetFile(buffer).schema_arrow

        columns = [f'"{field.name}" {map_arrow_type_to_postgres(str(field.type))}' for field in schema]

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {MART_SCHEMA}.{table_name} (
                {", ".join(columns)}
            );
        """
        rds_hook.run(create_table_sql)
        logger.info(f"[{table_name}] 테이블 구조 생성 완료")

    @task
    def load_table_data(file_info: Tuple[str, List[str]]):
        """테이블 데이터를 S3에서 RDS로 트랜잭션 적재.

        Args:
            file_info (Tuple[str, List[str]]): (테이블명, 파일 경로 리스트).
        """
        table_name, data_files = file_info

        if not data_files:
            logger.warning(f"[{table_name}] 적재할 파일이 없습니다.")
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)

        conn = rds_hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            logger.info(f"[{table_name}] 데이터 초기화 (TRUNCATE)")
            cursor.execute(f"TRUNCATE TABLE {MART_SCHEMA}.{table_name};")

            total_rows = 0
            for idx, s3_key in enumerate(data_files, 1):
                file_obj = s3_hook.get_key(s3_key, bucket_name=BUCKET_NAME)
                df = pd.read_parquet(io.BytesIO(file_obj.get()["Body"].read()))

                if df.empty:
                    logger.warning(f"[{table_name}] 파일 {idx}/{len(data_files)} 비어있음: {s3_key}")
                    continue

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)

                cursor.copy_expert(f"COPY {MART_SCHEMA}.{table_name} FROM STDIN WITH CSV", csv_buffer)
                total_rows += len(df)

                if idx % 10 == 0:
                    logger.info(f"[{table_name}] 진행: {idx}/{len(data_files)} 파일 처리 완료")

            cursor.execute(f"ANALYZE {MART_SCHEMA}.{table_name};")
            conn.commit()
            logger.info(f"✅ [{table_name}] 적재 완료 - {len(data_files)}개 파일, {total_rows:,} rows")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ [{table_name}] 에러 발생으로 롤백: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    tables = get_table_list()
    prepare_env = prepare_environment()

    file_infos = get_s3_files.expand(table_name=tables)
    create_tables = create_table_structure.expand(file_info=file_infos)
    load_data = load_table_data.expand(file_info=file_infos)

    prepare_env >> file_infos >> create_tables >> load_data


sync_gold_to_rds()
