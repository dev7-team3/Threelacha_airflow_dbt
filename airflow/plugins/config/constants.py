"""Airflow 전역 설정 상수"""

import os

# S3 설정
BUCKET_NAME = "team3-batch"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold/team3_gold"

# Athena 설정
ATHENA_OUTPUT_SILVER = f"s3://{BUCKET_NAME}/athena-query-results/silver/"
ATHENA_OUTPUT_GOLD = f"s3://{BUCKET_NAME}/athena-query-results/gold/"

# 데이터베이스/스키마 설정
SILVER_DATABASE = "team3_silver"
GOLD_DATABASE = "team3_gold"
GOLD_METADATA_DATABASE = "team3_gold_metadata"
MART_SCHEMA = "mart"

# 테이블 설정
SILVER_TABLES = ["api1", "api10", "api13", "api17"]
MART_PREFIX = "mart_"

# AWS 리전
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")

# dbt 설정
DBT_CONTAINER_NAME = "dbt"
DBT_PROJECT_PATH = "/usr/app/Threelacha"
