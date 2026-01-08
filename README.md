# 🍊 Airflow & dbt Operational Repository (EC2)

본 레포지토리는 AWS EC2 환경에서 실제로 운영되는 Airflow 및 dbt 서비스를 관리·배포하기 위한 운영용 레포지토리입니다.<br> 
Docker Compose 기반으로 구성되어 있으며, <strong>배치 데이터 파이프라인의 실행, 스케줄링, 검증, 배포 자동화(CI/CD)</strong>를 담당합니다.

> ⚠️ 로컬 아키텍처 검증(PoC)용 레포가 아닌, 실제 데이터 파이프라인이 실행되는 운영 런타임 환경을 관리하는 레포입니다.

---

## ✨ 주요 특징

- EC2 기반 Apache Airflow Celery Executor 클러스터 운영
- Airflow (Orchestration) 와 dbt (Transformation) 명확한 책임 분리<br>
   (Airflow에서 dbt 트리거 가능)
- DAG / Plugin / dbt 코드의 체계적인 관리
- Health Checks 및 리소스 제한을 고려한 운영 설정
- GitHub Actions 기반 CI (정적 검증) / CD (자동 배포 + 롤백)
- 배포 실패 시 자동 백업 복원(Rollback) 지원

---
## 🚀 설정 및 실행방법
### 1) Git Clone
```
# repo 복사 위치 (CD 정책 고려) : /opt/Threelacha_airflow_dbt
cd /opt
git clone git@github.com:dev7-team3/Threelacha_airflow_dbt.git
cd Threelacha_airflow_dbt
```
### 2) 환경 변수 설정
```.env.example``` 파일을 복사하여 ```.env``` 파일을 생성합니다.
```
cp .env.example .env
```
- 필요에 따라 ```.env``` 파일의 아래 항목들을 환경에 맞게 수정합니다.
- ```CERT_KEY``` ,  ```CERT_ID```, ```AWS_ACCESS_KEY_ID```, ```AWS_SECRET_ACCESS_KEY```, ```AWS_DEFAULT_REGION``` 항목은 반드시 본인 발급 정보에 맞게 변경하여야 합니다. 
- 그 외 항목은 변경하지 않아도 ```docker-compse.yaml``` 설정된 기본값으로 실행가능합니다.
```
# 변경 불가 항목
AIRFLOW_ENV=aws

# 필수 변경 항목
# ---------------------------------------------------------
# KAMIS Open API 인증 정보
# ---------------------------------------------------------
CERT_KEY=<YOUR_KAMIS_API_KEY>
CERT_ID=<YOUR_KAMIS_API_ID>

# ---------------------------------------------------------
# AWS 연결 정보 등록 (REGION 정보만 필요)
# ---------------------------------------------------------
AWS_DEFAULT_REGION=<YOUR_AWS_DEFAULT_REGION>
```
### 3) Docker Compose 실행
```
docker compose up -d
```
### 4) 기동 서비스 상태 확인
```
docker compose ps
docker compose logs airflow-scheduler
```
### 5) Airflow 커넥션 정보 등록
1. Airflow Web UI 접속  
   - `http://<EC2_HOST>:8080`
2. 좌측 메뉴 **관리자(Admin) → 커넥션(Connections)** 선택
3. 파이프라인 실행에 필요한 외부 서비스 Connection 정보 등록  
   - S3 (Object Storage)  
   - Athena (Query Engine)  
   - RDS (Metadata / Application DB)
   - 상세 설정: [Airflow Connection 설정 문서](./docs/airflow_connections.md)
---

## ⚙️ Service Composition

```
EC2 Instance
 ├── Airflow API Server
 ├── Airflow Scheduler
 ├── Airflow Workers (Celery)
 ├── Airflow Triggerer 
 ├── Airflow Dag Processor
 ├── PostgreSQL (Metadata DB)
 ├── Redis (Celery Broker)
 └── dbt Container
```

---

## 🔗 데이터 파이프라인
#### 🔑 Key Point: [데이터 파이프라인 단계별 상세 정의](https://github.com/dev7-team3/Threelacha_docs/blob/main/%EB%B3%B4%EA%B3%A0%EC%84%9C/%EC%83%81%EC%84%B8%EB%82%B4%EC%9A%A9/details_of_data_pipelines.md)

- 농축수산물 가격 API 데이터를 기반으로, 수집 → 정제 → 분석 → 대시보드까지의 전 과정을 자동화하는 배치 처리 구조로 설계


<p align="center">
    <img src="./docs/images/data_pipeline.png" width=800>
</p>

#### 🔑 Key Point: DAG간 의존성 제어 `Master Pipeline`
- `TriggerDagRunOperator` 기반의 상위 오케스트레이션 DAG를 통해 전체 데이터 흐름을 통합적으로 제어하도록 설계
- 이를 통해 사용자는 개별 수집·변환 DAG을 각각 스케줄링하여 실행할 필요 없이, Master DAG 하나만 실행함으로써 전체 데이터 흐름을 일관되게 관리
- 파이프라인의 실행 순서와 데이터 정합성을 보장하면서도, 유지보수성과 확장성을 고려한 안정적인 배치 처리 구조를 구축

---

## 📁 디렉토리 구조

```
THREELACHA_AIRFLOW_DBT/
├── .github/                        # GitHub 관리 설정
│   ├── ISSUE_TEMPLATE/             # 이슈 템플릿
│   └── workflows/
│       ├── ci.yml                  # CI (lint, test 등)
│       └── cd.yml                  # CD (EC2 배포 자동화)
│
├── airflow/ 
│   ├── dags/                       # Airflow DAG 파일 저장소
│   │   └── sql/                    # DAG에서 사용하는 SQL 스크립트
│   ├── logs/                       # Airflow 실행 로그
│   ├── plugins/                    # Airflow 커스텀 플러그인
│   │   ├── config/                 # DAG에서 공통으로 사용하는 설정 정보 정의
│   │   └── metadata/               # 메타데이터 파일 저장소
│   └── config/                     # airflow.cfg 및 설정 파일
│
├── dbt/
│   ├── Threelacha/                 # 메인 dbt 프로젝트
│   │   ├── models/                 # dbt models (Gold)
│   │   ├── macros/                 # Custom dbt macros
│   │   ├── seeds/                  # Seed data
│   │   ├── tests/                  # dbt tests
│   │   ├── logs/                   # dbt 실행 로그
│   │   └── dbt_project.yml         # dbt 프로젝트 설정
│   ├── profiles.yml                # Athena / RDS 연결 설정
│   └── Dockerfile                  # [Build] dbt 커스텀 이미지 (Athena adapter 설치)
│
├── docker-compose.yaml             # EC2 Airflow + dbt 서비스 구성
├── .env.example                    # 환경 변수 템플릿
├── pyproject.toml                  # 프로젝트 메타데이터 및 의존성 정의
├── uv.lock                         # 패키지 버전 고정 파일 (uv)
├── .pre-commit-config.yaml         # 코드 스타일 및 린트 설정
└── README.md
```
---

## 🔄 CI/CD Pipeline
### ✅ CI – Pull Request / dev 브랜치
**목적**: 코드 품질 및 DAG 안정성 검증
- Ruff lint / format check
- Airflow DB 초기화 (SQLite)
- DAG reserialize
- DAG import error 검증

### 📦 CD – main 브랜치 (EC2 자동 배포)
**목적**: 배포 실패를 대비한 안정적인 무중단 배포 <br>
**배포 흐름**: 
1. 기존 서비스 디렉토리 백업 (최근 3개 유지)
2. rsync 기반 코드 동기화
3. .env 주입
4. Docker Compose 설정 검증
5. 서비스 재기동
6. Airflow Health Check
7. 실패 시 자동 롤백
> ⚠️ 본 배포 파이프라인은 GitHub-hosted runner가 아닌,
> **EC2에 설치된 self-hosted GitHub Actions runner** 환경을 전제로 구성되어 있습니다.