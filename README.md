# 🌾 Threelacha Airflow + dbt Data Pipeline

농축수산물(KAMIS) 데이터를 수집·정제·분석하기 위한
**Airflow + dbt 기반 운영형 데이터 파이프라인 프로젝트**입니다.

본 레포는 **팀 협업을 전제로 한 표준 개발 환경**을 제공하며,
로컬 개발 → CI 검증 → CD 배포까지의 흐름이 자동화되어 있습니다.

---

## 📌 Project Goals

* 공공 API(KAMIS) 기반 데이터 수집 자동화
* Airflow DAG 기반 배치 파이프라인 운영
* dbt 기반 데이터 모델링 및 품질 검증
* 실무 환경과 유사한 CI / CD / 코드 품질 관리 경험

---

## 🏗 Repository Structure

```
.
├── airflow/
│   ├── dags/                   # Airflow DAG 정의
│   ├── plugins/                # 공통 유틸, 커스텀 로직
│   └── logs/                   # (로컬/운영) 로그 디렉토리
│
├── dbt/
├── Dockerfile                  # dbt 전용 Docker 이미지 정의 (dbt-athena 어댑터 설치) 
│   ├── profiles.yml            # 운영용 dbt profile
│   └── Threelacha/             # dbt project
│       ├── models/
│       ├── macros/
│       ├── dbt_project.yml
│       └── profiles.yml        # CI용 DuckDB profile
│
├── .github/
│   ├── workflows/
│   │   ├── ci.yml              # CI (Airflow + dbt)
│   │   └── cd.yml              # CD (EC2 배포)
│   ├── ISSUE_TEMPLATE/
│   │   └── issue_template.md
│   └── pull_request_template.md
│
├── pyproject.toml              # uv 기반 Python 환경 정의
├── uv.lock                     # 의존성 lock file
├── .pre-commit-config.yaml
├── docker-compose.yaml
└── README.md
```

---

## ⚙️ Development Environment

### Python & Dependency Management

* **Python 3.11**
* **uv** 사용 (pip / venv 대체)
* 의존성은 `pyproject.toml` + `uv.lock`으로 고정

### Code Quality

* **ruff**: lint + formatter
* **pre-commit**: 커밋 시 자동 검사

---

## 🚀 Quick Start (팀원 기준)

### 1️⃣ 레포 클론

```bash
git clone https://github.com/<org>/Threelacha_airflow_dbt.git
cd Threelacha_airflow_dbt
```

### 2️⃣ uv 설치

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3️⃣ Python & 의존성 설치

```bash
uv python install 3.11
uv sync
```

> ⚠️ 반드시 `uv.lock` 기준으로 설치됩니다

---

## 🧪 Local Validation

### Airflow DB 초기화

```bash
uv run airflow db migrate
```

### Airflow DAG 검증

```bash
uv run airflow dags list
uv run airflow dags list-import-errors
```

### dbt 컴파일 (CI와 동일 조건)

```bash
uv run dbt compile \
  --project-dir dbt/Threelacha \
  --profiles-dir dbt/Threelacha
```

---

## 🔍 Pre-commit (자동 코드 검사)

### 최초 1회 설정

```bash
uv run pre-commit install
```

### 수동 실행

```bash
uv run pre-commit run --all-files
```

✔️ 커밋 시 자동으로 실행되며
✔️ lint / format 문제는 커밋이 차단됩니다

---

## 🤖 CI Pipeline (자동)

### 트리거

* `dev` 브랜치 push
* `main` / `dev` 대상 Pull Request

### 검증 항목

* ruff lint / format
* Airflow DAG import & serialization
* dbt compile / parse (DuckDB)

❌ CI 실패 시 PR merge 불가

---

## 🚢 CD Pipeline (운영 배포)

### 트리거

* `main` 브랜치 merge

### 동작

1. EC2 self-hosted runner 실행
2. 기존 서비스 백업
3. 코드 동기화
4. Docker Compose 재기동
5. Airflow 헬스체크
6. 실패 시 자동 롤백

> ⚠️ CD 실패해도 **기존 서비스는 유지됩니다**

---

## 🔐 Branch Ruleset

* `main` 브랜치 보호
* CI 필수 통과
* 최신 dev 기준 merge 강제

---

## 📝 Issue / PR Guidelines

* Issue / PR 생성 시 자동 템플릿 적용
* 작업 목적, 변경 내용, 영향 범위를 명확히 작성

---

## 👥 Team Workflow Summary

```
dev 브랜치 작업
   ↓
PR 생성 → CI 자동 검증
   ↓
CI 통과 후 merge
   ↓
main merge → CD 자동 배포
```

---

## 📎 Notes

* 운영 환경 변수는 EC2 내 `/home/ubuntu/.env`에서 관리
* dbt CI는 DuckDB profile을 사용 (AWS 연결 없음)
* 실 데이터 적재는 운영 환경에서만 수행

---

## ✨ Maintainers

* Threelacha Data Engineering Team
