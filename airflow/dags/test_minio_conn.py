import pendulum
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(
    dag_id="test_minio_conn",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "jungeun_park",
    },
    tags=["test", "minio", "connection"],
    description='MinIO 연결 테스트를 위한 간단한 DAG',
)
def simple_minio_conn_test():
    """
    Airflow의 S3Hook을 사용하여 MinIO 연결을 테스트하고, 버킷 목록을 조회하는 DAG
    """

    @task
    def test_minio_connection():
        print("🔍 MinIO 연결을 테스트합니다. (minio_conn 사용)")

        # 1) S3Hook 초기화
        # 이 Hook은 Airflow UI에 설정된 'minio_conn' 정보를 사용합니다.
        hook = S3Hook(aws_conn_id="minio_conn")
        
        # 2) Boto3 클라이언트 객체 가져오기 (실제 통신 객체)
        # 이 시점에서 MinIO 서버로 연결을 시도합니다.
        client = hook.get_conn()

        try:
            # 3) 버킷 목록 조회 테스트 (가장 간단한 S3 API 호출)
            buckets_response = client.list_buckets()
            
            print("=========================================")
            print("✅ 연결 성공! 버킷 목록을 조회했습니다.")
            print("=========================================")
            
            # 조회된 버킷 이름을 출력
            bucket_names = [b["Name"] for b in buckets_response.get("Buckets", [])]
            print("현재 존재하는 버킷:", bucket_names)

        except Exception as e:
            print("=========================================")
            print("❌ 연결 실패 또는 API 호출 오류 발생:")
            print(e)
            print("=========================================")
            raise

        return "Connection Test OK"

    test_minio_connection()


simple_minio_conn_test()
